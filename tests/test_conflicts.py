from incsync.engine import run_sync
from incsync.ledger import payload_digest
from .helpers import SyncTestCase, SYNC_ID


class TestConflictPolicy(SyncTestCase):
    def test_delete_propagates_as_tombstone(self):
        source = self.seeded_source(n=3, page_size=10)
        run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        source.dataset.delete("widgets", "w001")
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.deleted, 1)
        row = self.dest.get_record("widgets", "w001")
        self.assertTrue(row["deleted"])
        self.assertEqual(row["version"], 2)

    def test_update_applies_last_writer_by_version(self):
        source = self.seeded_source(n=1, page_size=10)
        run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        source.dataset.update("widgets", "w000", {"name": "v2"})
        source.dataset.update("widgets", "w000", {"name": "v3"})
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.updated, 1)
        row = self.dest.get_record("widgets", "w000")
        self.assertEqual(row["version"], 3)
        self.assertEqual(row["payload"]["name"], "v3")

    def test_older_replay_cannot_overwrite_newer_version(self):
        namespace, id_ = "widgets", "w000"
        digest_v2 = payload_digest({"name": "v2"})
        self.dest.apply_op("widgets:w000:2:upsert", namespace, id_, 2, "upsert", digest_v2, {"name": "v2"})
        from incsync.errors import StaleVersion
        digest_v1 = payload_digest({"name": "v1"})
        with self.assertRaises(StaleVersion):
            self.dest.apply_op("widgets:w000:1:upsert", namespace, id_, 1, "upsert", digest_v1, {"name": "v1"})
        row = self.dest.get_record(namespace, id_)
        self.assertEqual(row["version"], 2)  # untouched by the stale replay

    def test_older_replay_cannot_resurrect_newer_tombstone(self):
        namespace, id_ = "widgets", "w000"
        from incsync.ledger import TOMBSTONE
        self.dest.apply_op("widgets:w000:2:delete", namespace, id_, 2, "delete",
                            payload_digest(TOMBSTONE), TOMBSTONE)
        from incsync.errors import StaleVersion
        with self.assertRaises(StaleVersion):
            self.dest.apply_op("widgets:w000:1:upsert", namespace, id_, 1, "upsert",
                                payload_digest({"name": "v1"}), {"name": "v1"})
        row = self.dest.get_record(namespace, id_)
        self.assertTrue(row["deleted"])
        self.assertEqual(row["version"], 2)

    def test_local_destination_edit_is_overridden_and_logged_as_conflict(self):
        source = self.seeded_source(n=1, page_size=10)
        run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        edited_payload = {"name": "hand-edited locally"}
        self.dest.local_edit("widgets", "w000", edited_payload, payload_digest(edited_payload))
        row = self.dest.get_record("widgets", "w000")
        self.assertTrue(row["local_edit"])
        self.assertEqual(row["payload"], edited_payload)

        source.dataset.update("widgets", "w000", {"name": "source v2"})
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.updated, 1)
        row = self.dest.get_record("widgets", "w000")
        self.assertEqual(row["payload"]["name"], "source v2")  # source-authoritative: local edit lost
        self.assertFalse(row["local_edit"])
        conflicts = self.dest.conflicts_for("widgets", "w000")
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["kind"], "local_edit_overridden")
