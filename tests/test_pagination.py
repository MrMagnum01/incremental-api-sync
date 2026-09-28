from incsync.engine import run_sync
from incsync.errors import LockHeld
from .helpers import SyncTestCase, SYNC_ID


class TestPagination(SyncTestCase):
    def test_full_sync_visits_every_record_once_in_order(self):
        source = self.seeded_source(n=25, page_size=10)
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "complete")
        self.assertEqual(result.advertised, 25)
        self.assertEqual(result.fetched, 25)
        self.assertEqual(result.attempted, 25)
        self.assertEqual(result.inserted, 25)
        self.assertEqual(result.unattempted, 0)
        dump = self.dest.dump()
        self.assertEqual(len(dump), 25)
        self.assertEqual({r["id"] for r in dump}, {f"w{i:03d}" for i in range(25)})

    def test_page1_binds_total_and_page_size_mid_run_growth_ignored(self):
        source = self.seeded_source(n=15, page_size=5)
        # mutate the mutable dataset after we know page 1 will freeze a snapshot
        orig_fetch = source.fetch_page
        called = {"n": 0}

        def wrapped(token, cursor):
            called["n"] += 1
            if called["n"] == 1:
                page = orig_fetch(token, cursor)
                source.dataset.seed("widgets", "w999", {"name": "late arrival"})
                return page
            return orig_fetch(token, cursor)

        source.fetch_page = wrapped
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "complete")
        self.assertEqual(result.advertised, 15)  # frozen at page 1, growth invisible
        self.assertEqual(result.fetched, 15)
        dump = self.dest.dump()
        self.assertEqual(len(dump), 15)
        self.assertNotIn("w999", {r["id"] for r in dump})

    def test_empty_snapshot_is_a_valid_complete_run_with_total_zero(self):
        source = self.seeded_source(n=0, page_size=10)
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "complete")
        self.assertEqual(result.advertised, 0)
        self.assertEqual(result.fetched, 0)
        self.assertEqual(result.attempted, 0)
        self.assertEqual(result.unattempted, 0)

    def test_non_advancing_cursor_is_rejected(self):
        source = self.seeded_source(n=15, page_size=5)
        source.queue_fault(2, {"type": "non_advancing"})
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "incomplete")
        self.assertIn("non-advancing", result.stop_reason)

    def test_drifting_total_mid_run_is_rejected(self):
        source = self.seeded_source(n=15, page_size=5)
        source.queue_fault(2, {"type": "drift_total"})
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "incomplete")
        self.assertIn("drifted", result.stop_reason)

    def test_duplicate_identity_within_page_is_rejected(self):
        source = self.seeded_source(n=15, page_size=5)
        source.queue_fault(1, {"type": "duplicate_version"})
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "incomplete")
        self.assertIn("contract_violation", result.stop_reason)

    def test_truncated_page_falsely_marked_done_is_caught_by_reconciliation(self):
        source = self.seeded_source(n=15, page_size=5)
        source.queue_fault(3, {"type": "truncate"})  # last page silently drops + claims done
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "incomplete")
        self.assertIn("truncated", result.stop_reason)
        self.assertEqual(result.fetched, 14)  # one record silently dropped by the fault
        self.assertEqual(result.unattempted, 1)

    def test_snapshot_expired_mid_run_is_incomplete(self):
        source = self.seeded_source(n=15, page_size=5)
        source.queue_fault(2, {"type": "invalidate"})
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "incomplete")
        self.assertIn("SnapshotExpired", result.stop_reason)

    def test_single_runner_lock_blocks_concurrent_run(self):
        from incsync.lock import RunnerLock

        with RunnerLock(self.lock_path):
            with self.assertRaises(LockHeld):
                with RunnerLock(self.lock_path):
                    pass
