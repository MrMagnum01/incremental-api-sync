"""Regression tests for the round-2 HOLD (vault: 40-sessions/2026-09-28-astra-
incremental-sync-r2-review.md), groups 1-4 only (5-6 were already closed). Each
test adapts one case from the review's reproduction probe (same directory,
*-probes.py) into an assertion of the *corrected* behaviour.
"""
import unittest

from incsync.clock import VirtualClock
from incsync.engine import apply_record, retry_fetch, parse_retry_after, run_sync
from incsync.errors import AckLost, CheckpointCorrupt, MalformedResponse, RateLimited, RetryBudgetExhausted, SimulatedCrash
from incsync.ledger import payload_digest
from .helpers import SyncTestCase, SYNC_ID


# -- Group 1: required checkpoint bindings cannot be optional -----------------

class TestRequiredCheckpointBindings(SyncTestCase):
    def test_null_checkpoint_bindings_refuse_a_source_swap_instead_of_resuming(self):
        # Probe: interrupt a 4-row source-A run after 2 rows, null out the
        # checkpoint's source_id/dest_id, then resume as source-B. The old
        # behaviour treated the null bindings as an "unbound compatibility
        # mode" and completed the run against the wrong source; a
        # schema_version=2 checkpoint must always carry both bindings.
        source = self.seeded_source(n=4, page_size=2)
        with self.assertRaises(SimulatedCrash):
            run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock,
                      crash_at="before_fetch:page2", source_id="source-A")
        self.dest.conn.execute("UPDATE checkpoint SET source_id=NULL, dest_id=NULL")

        with self.assertRaises(CheckpointCorrupt):
            run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock, source_id="source-B")

    def test_in_progress_checkpoint_with_null_fetched_count_is_corrupt_not_zero(self):
        # A missing fetched_count must not be silently treated as an implicit
        # valid 0 -- it is a corrupt, unresumable checkpoint.
        source = self.seeded_source(n=4, page_size=2)
        with self.assertRaises(SimulatedCrash):
            run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock, crash_at="before_fetch:page2")
        self.dest.conn.execute("UPDATE checkpoint SET fetched_count=NULL")

        with self.assertRaises(CheckpointCorrupt):
            run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)

    def test_in_progress_checkpoint_with_null_advertised_total_is_corrupt(self):
        source = self.seeded_source(n=4, page_size=2)
        with self.assertRaises(SimulatedCrash):
            run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock, crash_at="before_fetch:page2")
        self.dest.conn.execute("UPDATE checkpoint SET advertised_total=NULL")

        with self.assertRaises(CheckpointCorrupt):
            run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)


# -- Group 2: page validation ---------------------------------------------------

class TestPageValidationCompleteness(SyncTestCase):
    def test_nan_updated_at_is_rejected_not_silently_accepted(self):
        source = self.seeded_source(n=1, page_size=2)
        real_fetch = source.fetch_page

        def mutate(token, cursor):
            page = real_fetch(token, cursor)
            page.records[0].updated_at = float("nan")
            return page

        source.fetch_page = mutate
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "incomplete")
        self.assertFalse(result.ok)
        self.assertIn("malformed record", result.stop_reason)
        self.assertEqual(len(self.dest.dump()), 0)

    def test_boolean_total_is_rejected_not_treated_as_an_int(self):
        source = self.seeded_source(n=1, page_size=2)
        real_fetch = source.fetch_page

        def mutate(token, cursor):
            page = real_fetch(token, cursor)
            page.total = True
            return page

        source.fetch_page = mutate
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "incomplete")
        self.assertFalse(result.ok)
        self.assertIn("contract_violation", result.stop_reason)

    def test_terminal_page_carrying_a_next_cursor_is_rejected(self):
        source = self.seeded_source(n=1, page_size=2)
        real_fetch = source.fetch_page

        def mutate(token, cursor):
            page = real_fetch(token, cursor)
            page.next_cursor = (999.0, "skip")  # has_more is False on this page
            return page

        source.fetch_page = mutate
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "incomplete")
        self.assertFalse(result.ok)
        self.assertIn("next_cursor", result.stop_reason)

    def test_a_page_delivering_more_records_than_page_size_is_rejected(self):
        # 4 rows, page_size 2: page 1 is mutated to carry 3 records while
        # page_size and total are left unmodified (so the older "fetched
        # exceeds advertised" check alone would not catch it -- 3 <= 4).
        source = self.seeded_source(n=4, page_size=2)
        real_fetch = source.fetch_page

        def mutate(token, cursor):
            page = real_fetch(token, cursor)
            if cursor is None:
                page.records = source.dataset.snapshot_rows()[:3]
                page.next_cursor = (page.records[-1].updated_at, page.records[-1].id)
            return page

        source.fetch_page = mutate
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "incomplete")
        self.assertFalse(result.ok)
        self.assertIn("overfilled", result.stop_reason)
        self.assertEqual(len(self.dest.dump()), 0)

    def test_a_non_record_item_becomes_a_categorised_error_not_an_attributeerror(self):
        source = self.seeded_source(n=1, page_size=2)
        real_fetch = source.fetch_page

        def mutate(token, cursor):
            page = real_fetch(token, cursor)
            page.records = [None]
            return page

        source.fetch_page = mutate
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "incomplete")
        self.assertFalse(result.ok)
        self.assertIn("malformed record", result.stop_reason)


# -- Group 3: confirmation outcome validation -----------------------------------

class TestConfirmationOutcomeValidation(SyncTestCase):
    def test_an_outcome_outside_the_allowed_set_is_never_trusted_as_confirmed(self):
        source = self.seeded_source(n=1, page_size=10)
        r = source.dataset.snapshot_rows()[0]

        class BogusOutcomeDest:
            def apply_op(self, *a, **kw):
                raise AckLost("lost")

            def get_op(self, k):
                return {
                    "op_key": k, "namespace": r.namespace, "id": r.id, "version": r.version,
                    "op": "upsert", "digest": payload_digest(r.payload),
                    "outcome": "not_an_outcome",
                }

        outcome, detail = apply_record(BogusOutcomeDest(), r, None)
        self.assertEqual(outcome, "unknown")
        self.assertNotEqual(outcome, "confirmed_after_lost_ack")

    def test_a_direct_apply_op_result_outside_the_allowed_set_is_unknown_not_a_keyerror(self):
        source = self.seeded_source(n=1, page_size=10)
        r = source.dataset.snapshot_rows()[0]

        class BogusDirectResultDest:
            def apply_op(self, *a, **kw):
                return "not_an_outcome"

        outcome, detail = apply_record(BogusDirectResultDest(), r, None)
        self.assertEqual(outcome, "unknown")

    def test_a_delete_confirmation_reporting_an_upsert_only_outcome_is_not_trusted(self):
        source = self.seeded_source(n=1, page_size=10)
        r = source.dataset.snapshot_rows()[0]
        r.deleted = True

        class WrongOpOutcomeDest:
            def apply_op(self, *a, **kw):
                raise AckLost("lost")

            def get_op(self, k):
                from incsync.ledger import TOMBSTONE
                return {
                    "op_key": k, "namespace": r.namespace, "id": r.id, "version": r.version,
                    "op": "delete", "digest": payload_digest(TOMBSTONE),
                    "outcome": "inserted",  # a delete can never legitimately report "inserted"
                }

        outcome, detail = apply_record(WrongOpOutcomeDest(), r, None)
        self.assertEqual(outcome, "unknown")


# -- Group 4: retry elapsed budget and Retry-After parsing ----------------------

class TestRetryBudgetAndRetryAfter(SyncTestCase):
    def test_retry_after_that_would_only_start_past_the_budget_never_dispatches(self):
        # 429 with Retry-After=120 at t=0: the old `>` comparison let the sleep
        # run to exactly t=120 and then dispatched a second attempt anyway.
        # Equality is exhausted -- no second attempt may start.
        calls = []

        class Retry:
            def fetch_page(self, *args):
                calls.append(self.clock.now())
                if len(calls) == 1:
                    raise RateLimited(120)
                return "success"

        retry_source = Retry()
        retry_source.clock = self.clock
        with self.assertRaises(RetryBudgetExhausted):
            retry_fetch(retry_source, None, None, self.clock)
        self.assertEqual(calls, [0.0])  # never dispatched a second attempt

    def test_unparseable_retry_after_date_is_a_categorised_malformed_response(self):
        with self.assertRaises(MalformedResponse):
            parse_retry_after("not a date", self.clock)

    def test_elapsed_time_uses_a_monotonic_clock_not_wall_time(self):
        # incsync/clock.py:Clock.now() must read time.monotonic(), not time.time():
        # a wall-clock adjustment (NTP step, DST, manual change) must never disturb
        # the retry-elapsed accounting. Break time.time() entirely and confirm
        # Clock().now() is unaffected.
        import incsync.clock as clock_module

        def exploding_time():
            raise AssertionError("Clock.now() must not call time.time()")

        original_time = clock_module.time.time
        clock_module.time.time = exploding_time
        try:
            real_clock = clock_module.Clock()
            t1 = real_clock.now()
            t2 = real_clock.now()
            self.assertGreaterEqual(t2, t1)
        finally:
            clock_module.time.time = original_time


if __name__ == "__main__":
    unittest.main()
