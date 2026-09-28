import os
import tempfile
import unittest

from incsync.clock import VirtualClock
from incsync.mock_dest import Destination
from incsync.mock_source import MockSourceAPI, SourceDataset

SYNC_ID = "test-sync"


class SyncTestCase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="incsync-test-")
        self.dest = Destination(os.path.join(self.tmpdir, "dest.sqlite3"))
        self.lock_path = os.path.join(self.tmpdir, "runner.lock")
        self.clock = VirtualClock()

    def tearDown(self):
        self.dest.close()

    def seeded_source(self, n=25, page_size=10) -> MockSourceAPI:
        ds = SourceDataset()
        for i in range(n):
            ds.seed("widgets", f"w{i:03d}", {"name": f"Widget {i}", "price_cents": 100 + i})
        return MockSourceAPI(ds, page_size=page_size)
