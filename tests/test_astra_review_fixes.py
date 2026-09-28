"""Regression tests for the six MUST-FIX groups in the 2026-09-28 review
(vault: 40-sessions/2026-09-28-astra-incremental-api-sync-review.md). Each test
adapts one of the review's reproduction probes (same file, *-probes.py) into an
assertion of the *corrected* behaviour, plus direct coverage of the narrower
fixes each group required.
"""
import dataclasses
import json
import os
import subprocess
import sys
import unittest

from incsync.clock import VirtualClock
from incsync.engine import apply_record, retry_fetch, run_sync
from incsync.errors import (
    AckLost,
    CheckpointCorrupt,
    CheckpointIdentityMismatch,
    RetryBudgetExhausted,
    SimulatedCrash,
    TransientServerError,
)
from incsync.ledger import op_key, payload_digest
from incsync.mock_dest import Destination
from incsync.mock_source import MockSourceAPI, SourceDataset
from .helpers import SyncTestCase, SYNC_ID


# -- Group 1: resume coverage + checkpoint integrity ------------------------

class TestResumeCoverageAndCheckpointIntegrity(SyncTestCase):
    def test_resumed_run_catches_a_truncated_final_page(self):
        # 4 records, page_size 2: page 1 (records 0,1) commits and checkpoints
        # in_progress, then the run is crashed before fetching page 2.
        source = self.seeded_source(n=4, page_size=2)
        with self.assertRaises(SimulatedCrash):
            run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock, crash_at="before_fetch:page2")
        self.assertEqual(len(self.dest.dump()), 2)

        # Resume: the source's page 2 drops its last record but falsely claims done.
        source.queue_fault(2, {"type": "truncate"})
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)

        self.assertEqual(result.status, "incomplete")
        self.assertFalse(result.ok)
        self.assertIn("truncated", result.stop_reason)
        self.assertEqual(result.fetched, 3)  # 2 from before the crash + 1 new, not 4
        self.assertEqual(result.unattempted, 1)
        self.assertEqual(len(self.dest.dump()), 3)  # the 4th record was never delivered

        # The checkpoint itself must not have been left saying "complete".
        ckpt = self.dest.load_checkpoint(SYNC_ID)
        self.assertEqual(ckpt["status"], "in_progress")

    def test_resume_after_write_block_does_not_double_count_fetched_coverage(self):
        # A write block (not a fetch problem) rewinds the write cursor but must not
        # cause the re-fetched tail of that page to be double-counted as new coverage.
        source = self.seeded_source(n=3, page_size=10)

        def ack_mode_fn(namespace, id, version, op):
            return "lost_ack_unconfirmable" if id == "w001" else "ok"

        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock, ack_mode_fn=ack_mode_fn)
        self.assertEqual(result.unknown, 1)
        self.assertEqual(result.fetched, 3)

        result2 = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result2.status, "complete")
        self.assertTrue(result2.ok)
        # Re-fetching the tail to retry the blocked write must not inflate coverage:
        # fetched stays at the true total (3), it does not become 3 (persisted) + 2
        # (re-seen) = 5, and no false "extra records" contract violation fires.
        self.assertEqual(result2.fetched, 3)
        self.assertEqual(result2.unattempted, 0)
        self.assertEqual(len(self.dest.dump()), 3)

    def test_corrupt_checkpoint_status_and_schema_version_fail_closed(self):
        source = self.seeded_source(n=4, page_size=2)
        self.dest.save_checkpoint(SYNC_ID, "bogus-token", (99.0, "z"), "corrupt_status")
        self.dest.conn.execute("UPDATE checkpoint SET schema_version=999")
        with self.assertRaises(CheckpointCorrupt):
            run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)

    def test_checkpoint_bound_to_a_different_source_id_is_rejected(self):
        source = self.seeded_source(n=3, page_size=10)
        run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock, source_id="widgets-a")
        # Same destination, same sync_id, but a caller now claims a different source
        # identity -> the checkpoint's bound identity must refuse it, not trust the
        # coincidentally-matching snapshot token alone.
        with self.assertRaises(CheckpointIdentityMismatch):
            run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock, source_id="widgets-b")

    def test_checkpoint_bound_to_a_different_destination_is_rejected(self):
        source = self.seeded_source(n=3, page_size=10)
        run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock, source_id="widgets")
        other_dest = Destination(os.path.join(self.tmpdir, "other-dest.sqlite3"))
        try:
            ckpt = self.dest.load_checkpoint(SYNC_ID)
            # Same sync_id row copied onto a genuinely different destination file.
            other_dest.save_checkpoint(SYNC_ID, ckpt["snapshot_token"], ckpt["cursor"], ckpt["status"],
                                        source_id="widgets", dest_id=self.dest.identity,
                                        advertised_total=ckpt["advertised_total"],
                                        page_size=ckpt["page_size"], fetched_count=ckpt["fetched_count"],
                                        fetch_cursor=ckpt["fetch_cursor"])
            with self.assertRaises(CheckpointIdentityMismatch):
                run_sync(source, other_dest, SYNC_ID, self.lock_path, self.clock, source_id="widgets")
        finally:
            other_dest.close()


# -- Group 2: page contracts -------------------------------------------------

class TestPageContracts(SyncTestCase):
    def test_a_page_with_a_foreign_snapshot_token_is_rejected(self):
        source = self.seeded_source(n=4, page_size=2)
        real_fetch = source.fetch_page
        calls = {"n": 0}

        def swap_token(token, cursor):
            page = real_fetch(token, cursor)
            calls["n"] += 1
            return dataclasses.replace(page, snapshot_token="FOREIGN") if calls["n"] == 2 else page

        source.fetch_page = swap_token
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "incomplete")
        self.assertIn("snapshot token drifted", result.stop_reason)
        self.assertEqual(len(self.dest.dump()), 2)  # only the first, genuine page was applied

    def test_max_pages_is_enforced_before_fetching_the_next_page(self):
        source = self.seeded_source(n=10, page_size=2)
        fetch_calls = {"n": 0}
        real_fetch = source.fetch_page

        def counting_fetch(token, cursor):
            fetch_calls["n"] += 1
            return real_fetch(token, cursor)

        source.fetch_page = counting_fetch
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock, max_pages=2)
        self.assertEqual(result.status, "incomplete")
        self.assertEqual(result.stop_reason, "max_pages_exceeded")
        self.assertEqual(fetch_calls["n"], 2)  # never fetched a 3rd page beyond the budget
        self.assertEqual(len(self.dest.dump()), 4)  # exactly the 2 allowed pages were applied

    def test_a_page_that_under_fills_while_claiming_more_is_rejected(self):
        source = self.seeded_source(n=10, page_size=5)
        real_fetch = source.fetch_page
        calls = {"n": 0}

        def drop_one_but_claim_more(token, cursor):
            page = real_fetch(token, cursor)
            calls["n"] += 1
            if calls["n"] == 1:
                return dataclasses.replace(page, records=page.records[:-1])
            return page

        source.fetch_page = drop_one_but_claim_more
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "incomplete")
        self.assertIn("under-filled", result.stop_reason)
        # Contract is validated before anything on the bad page is applied.
        self.assertEqual(len(self.dest.dump()), 0)

    def test_a_page_delivering_more_than_the_advertised_total_is_rejected(self):
        source = self.seeded_source(n=4, page_size=2)
        real_fetch = source.fetch_page

        def inflate_final_page(token, cursor):
            page = real_fetch(token, cursor)
            if not page.has_more:
                last = page.records[-1]
                extra = dataclasses.replace(last, id="extra-injected", updated_at=last.updated_at + 1)
                return dataclasses.replace(page, records=page.records + [extra])
            return page

        source.fetch_page = inflate_final_page
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "incomplete")
        self.assertIn("exceeds advertised total", result.stop_reason)

    def test_a_record_with_a_malformed_field_type_blocks_the_whole_page(self):
        source = self.seeded_source(n=4, page_size=2)
        real_fetch = source.fetch_page

        def corrupt_first_page(token, cursor):
            page = real_fetch(token, cursor)
            if cursor is None:
                bad = dataclasses.replace(page.records[1], version="not-an-int")
                return dataclasses.replace(page, records=[page.records[0], bad])
            return page

        source.fetch_page = corrupt_first_page
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "incomplete")
        self.assertIn("malformed record", result.stop_reason)
        self.assertEqual(len(self.dest.dump()), 0)  # nothing from the bad page was applied


# -- Group 3: idempotency and confirmation identity --------------------------

class TestIdempotencyAndConfirmationIdentity(SyncTestCase):
    def test_op_key_does_not_collide_across_a_split_namespace_id_boundary(self):
        source = SourceDataset()
        source.seed("a:b", "c", {"same": 1})
        source.seed("a", "b:c", {"same": 1})
        api = MockSourceAPI(source, page_size=10)
        result = run_sync(api, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.inserted, 2)
        self.assertEqual(len(self.dest.dump()), 2)
        self.assertNotEqual(
            op_key("a:b", "c", 1, "upsert"),
            op_key("a", "b:c", 1, "upsert"),
        )

    def test_a_confirmation_naming_a_foreign_operation_is_never_trusted(self):
        source = self.seeded_source(n=1, page_size=10)
        r = source.dataset.snapshot_rows()[0]

        class ForeignConfirmationDest:
            def apply_op(self, *a, **k):
                raise AckLost("key")

            def get_op(self, k):
                return {
                    "op_key": "foreign", "namespace": "foreign", "id": "foreign",
                    "version": 99, "op": "delete",
                    "digest": payload_digest(r.payload), "outcome": "inserted",
                }

        outcome, detail = apply_record(ForeignConfirmationDest(), r, None)
        self.assertEqual(outcome, "unknown")
        self.assertNotEqual(outcome, "inserted")

    def test_a_confirmation_lookup_that_raises_becomes_unknown_not_a_crash(self):
        source = self.seeded_source(n=1, page_size=10)
        r = source.dataset.snapshot_rows()[0]

        class BrokenLookupDest:
            def apply_op(self, *a, **k):
                raise AckLost("key")

            def get_op(self, k):
                raise RuntimeError("destination read failed")

        outcome, detail = apply_record(BrokenLookupDest(), r, None)
        self.assertEqual(outcome, "unknown")

    def test_committed_write_with_unreadable_confirmation_is_unknown_then_replays_clean(self):
        # Distinct from "lost_ack_unconfirmable": the write here genuinely commits,
        # and only the follow-up confirmation read is unreadable.
        source = self.seeded_source(n=3, page_size=10)

        def ack_mode_fn(namespace, id, version, op):
            return "committed_then_confirmation_unreadable" if id == "w001" else "ok"

        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock, ack_mode_fn=ack_mode_fn)
        self.assertEqual(result.unknown, 1)
        self.assertFalse(result.ok)
        # The write really did commit, unlike the unconfirmable-outage case.
        self.assertIsNotNone(self.dest.get_record("widgets", "w001"))

        result2 = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result2.status, "complete")
        self.assertTrue(result2.ok)
        # w000 already fully resolved in run 1 and is not re-fetched; w001 (the
        # genuinely-committed-but-unconfirmed write) and w002 replay as unchanged --
        # no duplicate write despite the earlier UNKNOWN outcome.
        self.assertEqual(result2.unchanged, 2)
        self.assertEqual(len(self.dest.dump()), 3)


# -- Group 4: elapsed-time retry bounds ---------------------------------------

class TestRetryElapsedBudget(SyncTestCase):
    def test_slow_requests_count_against_the_120s_budget_not_just_backoff_sleeps(self):
        class SlowSource:
            def __init__(self, clock):
                self.clock = clock

            def fetch_page(self, token, cursor):
                self.clock.sleep(60)  # the "request" itself is slow
                raise TransientServerError(503)

        source = SlowSource(self.clock)
        with self.assertRaises(RetryBudgetExhausted):
            retry_fetch(source, None, None, self.clock)
        # Old behaviour let 5 x 60s requests plus backoff run past 300s; elapsed
        # time actually spent (including request duration) must stay bounded near 120s.
        self.assertLessEqual(self.clock.now(), 130.0)

    def test_http_date_retry_after_uses_the_injected_clock_not_the_real_wall_clock(self):
        from incsync.engine import parse_retry_after
        import email.utils
        import datetime

        clock = VirtualClock(start=1000.0)
        future = clock.utcnow() + datetime.timedelta(seconds=10)
        http_date = email.utils.format_datetime(future, usegmt=True)
        seconds = parse_retry_after(http_date, clock)
        self.assertAlmostEqual(seconds, 10.0, delta=1.0)

    def test_non_finite_retry_after_is_rejected_as_malformed(self):
        from incsync.engine import parse_retry_after
        from incsync.errors import MalformedResponse

        with self.assertRaises(MalformedResponse):
            parse_retry_after(float("inf"))
        with self.assertRaises(MalformedResponse):
            parse_retry_after(float("nan"))
        with self.assertRaises(MalformedResponse):
            parse_retry_after(-5)


# -- Group 5: overall failure signalling --------------------------------------

class TestOverallFailureSignalling(SyncTestCase):
    def test_unresolved_unknown_write_makes_the_run_not_ok_even_though_fetch_completed(self):
        source = self.seeded_source(n=3, page_size=10)

        def ack_mode_fn(namespace, id, version, op):
            return "lost_ack_unconfirmable" if id == "w001" else "ok"

        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock, ack_mode_fn=ack_mode_fn)
        self.assertEqual(result.status, "complete")  # fetch itself completed
        self.assertFalse(result.ok)  # but the run overall did not succeed
        self.assertEqual(result.unknown, 1)

    def test_a_failed_write_also_makes_the_run_not_ok(self):
        self.dest.apply_op("widgets:w000:1:upsert", "widgets", "w000", 1, "upsert", "digestA", {"a": 1})
        source = self.seeded_source(n=1, page_size=10)
        # w000 replayed at version 1 with a different digest than already stored -> OpKeyReused -> failed.
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.failed, 1)
        self.assertFalse(result.ok)

    def test_cli_exits_non_zero_when_the_checkpoint_is_corrupt_rather_than_silently_succeeding(self):
        # The CLI's exit code is gated on RunResult.ok (see cli.py); this integration
        # test exercises that wiring end to end via a fail-closed corrupt checkpoint,
        # since the CLI has no flag to inject an unresolved-write ack_mode directly
        # (that path is covered at the unit level above).
        import tempfile
        state_dir = tempfile.mkdtemp(prefix="incsync-cli-signalling-")
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        env = dict(os.environ)

        # Crash hard after page 1's checkpoint has genuinely been saved (25 records,
        # page_size 5 -> multiple pages), then corrupt that saved checkpoint row so
        # the resumed run must fail closed with a non-zero exit rather than silently
        # reporting success.
        first = subprocess.run(
            [sys.executable, "-m", "incsync.cli", "run", "--state-dir", state_dir,
             "--page-size", "5", "--crash-at", "before_fetch:page2"],
            cwd=repo_root, env=env, capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(first.returncode, 137)

        dest = Destination(os.path.join(state_dir, "dest.sqlite3"))
        dest.conn.execute("UPDATE checkpoint SET schema_version=999")
        dest.close()

        second = subprocess.run(
            [sys.executable, "-m", "incsync.cli", "run", "--state-dir", state_dir, "--page-size", "5"],
            cwd=repo_root, env=env, capture_output=True, text=True, timeout=30,
        )
        self.assertNotEqual(second.returncode, 0)


# -- Group 6: narrowed claims -------------------------------------------------

class TestNarrowedClaims(SyncTestCase):
    def test_local_edit_survives_an_unchanged_replay_only_a_newer_version_overrides_it(self):
        # README previously implied any next sync overrides a local edit. In fact an
        # *unchanged* replay (same version, matching digest) never touches the records
        # table at all -- only a genuinely newer source version does.
        source = self.seeded_source(n=1, page_size=10)
        run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        edited_payload = {"name": "hand-edited locally"}
        self.dest.local_edit("widgets", "w000", edited_payload, payload_digest(edited_payload))

        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.unchanged, 1)
        row = self.dest.get_record("widgets", "w000")
        self.assertEqual(row["payload"], edited_payload)  # untouched by the unchanged replay
        self.assertTrue(row["local_edit"])
        self.assertEqual(self.dest.conflicts_for("widgets", "w000"), [])

        source.dataset.update("widgets", "w000", {"name": "source v2"})
        result2 = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result2.updated, 1)
        row2 = self.dest.get_record("widgets", "w000")
        self.assertEqual(row2["payload"]["name"], "source v2")  # only the newer version overrides
        self.assertFalse(row2["local_edit"])
        self.assertEqual(len(self.dest.conflicts_for("widgets", "w000")), 1)


if __name__ == "__main__":
    unittest.main()
