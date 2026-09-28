from incsync.engine import run_sync
from .helpers import SyncTestCase, SYNC_ID


class TestAccounting(SyncTestCase):
    def test_attempted_equals_sum_of_outcome_buckets(self):
        source = self.seeded_source(n=12, page_size=4)
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(
            result.attempted,
            result.inserted + result.updated + result.deleted + result.unchanged + result.failed + result.unknown,
        )
        self.assertEqual(result.advertised, 12)
        self.assertEqual(result.fetched, 12)
        self.assertEqual(result.unattempted, 0)

    def test_final_state_reconciles_against_independent_synthetic_ledger(self):
        source = self.seeded_source(n=10, page_size=3)
        run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        source.dataset.update("widgets", "w002", {"name": "Widget 2", "price_cents": 5000})
        source.dataset.delete("widgets", "w005")
        run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)

        # Independently-built expected ledger from the same mutation history, computed
        # without touching the engine or destination code paths at all.
        expected = {}
        for i in range(10):
            expected[f"w{i:03d}"] = {"deleted": False, "payload": {"name": f"Widget {i}", "price_cents": 100 + i}}
        expected["w002"]["payload"] = {"name": "Widget 2", "price_cents": 5000}
        expected["w005"]["deleted"] = True

        dump = {r["id"]: r for r in self.dest.dump()}
        self.assertEqual(set(dump.keys()), set(expected.keys()))
        mismatches = []
        for id_, exp in expected.items():
            got = dump[id_]
            if got["deleted"] != exp["deleted"]:
                mismatches.append((id_, "deleted", got["deleted"], exp["deleted"]))
            if not got["deleted"] and got["payload"] != exp["payload"]:
                mismatches.append((id_, "payload", got["payload"], exp["payload"]))
        self.assertEqual(mismatches, [])

    def test_incomplete_run_reports_source_deficit_separately(self):
        source = self.seeded_source(n=10, page_size=3)
        source.queue_fault(3, {"type": "server_error", "status": 503})
        for _ in range(4):
            source.queue_fault(3, {"type": "server_error", "status": 503})
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "incomplete")
        self.assertEqual(result.advertised, 10)
        self.assertEqual(result.fetched, 6)  # two pages of 3 landed before the failure
        self.assertEqual(result.unattempted, 4)
        self.assertEqual(result.attempted, 6)
