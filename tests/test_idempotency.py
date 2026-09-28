from incsync.engine import run_sync
from .helpers import SyncTestCase, SYNC_ID


class TestIdempotency(SyncTestCase):
    def test_replay_after_lost_ack_creates_no_duplicate(self):
        source = self.seeded_source(n=5, page_size=10)

        def ack_mode_fn(namespace, id, version, op):
            return "lost_ack_confirmable" if id == "w002" else "ok"

        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock, ack_mode_fn=ack_mode_fn)
        self.assertEqual(result.status, "complete")
        self.assertEqual(result.inserted, 5)  # the lost-ack write still resolves to a real outcome
        dump = self.dest.dump()
        self.assertEqual(len(dump), 5)  # no duplicate row despite the lost acknowledgement
        w2 = [r for r in dump if r["id"] == "w002"][0]
        self.assertEqual(w2["version"], 1)

        # A genuine replay of the whole run (e.g. a caller retries blindly) must not duplicate.
        source2 = self.seeded_source(n=5, page_size=10)
        result2 = run_sync(source2, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result2.unchanged, 5)
        self.assertEqual(result2.inserted, 0)
        self.assertEqual(len(self.dest.dump()), 5)

    def test_op_key_reused_with_different_payload_is_refused(self):
        key = "widgets:w001:1:upsert"
        self.dest.apply_op(key, "widgets", "w001", 1, "upsert", "digestA", {"a": 1})
        from incsync.errors import OpKeyReused
        with self.assertRaises(OpKeyReused):
            self.dest.apply_op(key, "widgets", "w001", 1, "upsert", "digestB", {"a": 2})

    def test_later_legitimate_update_is_not_swallowed_as_duplicate(self):
        source = self.seeded_source(n=3, page_size=10)
        run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        source.dataset.update("widgets", "w001", {"name": "Widget 1", "price_cents": 999})
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.updated, 1)
        self.assertEqual(result.unchanged, 2)
        row = self.dest.get_record("widgets", "w001")
        self.assertEqual(row["version"], 2)
        self.assertEqual(row["payload"]["price_cents"], 999)

    def test_unconfirmable_outage_is_recorded_unknown_and_blocks_checkpoint(self):
        source = self.seeded_source(n=3, page_size=10)

        def ack_mode_fn(namespace, id, version, op):
            return "lost_ack_unconfirmable" if id == "w001" else "ok"

        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock, ack_mode_fn=ack_mode_fn)
        self.assertEqual(result.unknown, 1)
        self.assertEqual(result.status, "complete")  # fetch itself completed; the write did not
        ckpt = self.dest.load_checkpoint(SYNC_ID)
        self.assertEqual(ckpt["status"], "in_progress")  # blocked before the unresolved record

        # Retry the run: destination is reachable again, op_key was never actually written,
        # so it applies cleanly and checkpoint advances to complete.
        result2 = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result2.status, "complete")
        self.assertEqual(len(self.dest.dump()), 3)
