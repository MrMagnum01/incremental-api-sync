import os
import subprocess
import sys
import unittest

from incsync.engine import run_sync
from incsync.errors import SimulatedCrash
from .helpers import SyncTestCase, SYNC_ID


class TestCrashRecovery(SyncTestCase):
    def _run_with_crash(self, source, stage):
        with self.assertRaises(SimulatedCrash):
            run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock, crash_at=stage)

    def test_crash_before_write_then_resume_writes_exactly_once(self):
        source = self.seeded_source(n=5, page_size=10)
        self._run_with_crash(source, "before_write:page1:2")
        self.assertEqual(len(self.dest.dump()), 2)  # records 0,1 committed before the crash point
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "complete")
        dump = self.dest.dump()
        self.assertEqual(len(dump), 5)
        self.assertEqual({r["id"] for r in dump}, {f"w{i:03d}" for i in range(5)})

    def test_crash_after_write_before_checkpoint_resumes_without_duplication(self):
        source = self.seeded_source(n=5, page_size=10)
        self._run_with_crash(source, "after_write:page1:4")
        # all 5 records were durably written, but the checkpoint was never even saved
        self.assertEqual(len(self.dest.dump()), 5)
        self.assertIsNone(self.dest.load_checkpoint(SYNC_ID))
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "complete")
        self.assertEqual(result.unchanged, 5)  # pure replay, no re-insert, no duplicates
        self.assertEqual(len(self.dest.dump()), 5)

    def test_crash_before_checkpoint_save_resumes_cleanly(self):
        source = self.seeded_source(n=5, page_size=10)
        self._run_with_crash(source, "before_checkpoint:page1")
        self.assertEqual(len(self.dest.dump()), 5)
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "complete")
        self.assertEqual(result.unchanged, 5)
        self.assertEqual(len(self.dest.dump()), 5)

    def test_crash_mid_multi_page_run_resumes_from_correct_page(self):
        source = self.seeded_source(n=15, page_size=5)
        self._run_with_crash(source, "before_fetch:page3")
        self.assertEqual(len(self.dest.dump()), 10)  # pages 1-2 fully applied
        result = run_sync(source, self.dest, SYNC_ID, self.lock_path, self.clock)
        self.assertEqual(result.status, "complete")
        dump = self.dest.dump()
        self.assertEqual(len(dump), 15)


class TestRealProcessCrash(unittest.TestCase):
    """The literal requirement: recovery proven across an actual process
    termination (os._exit inside a real subprocess), not just a raised exception."""

    def test_hard_kill_after_commit_before_checkpoint_recovers_via_replay(self):
        import tempfile
        state_dir = tempfile.mkdtemp(prefix="incsync-cli-")
        env = dict(os.environ)
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        first = subprocess.run(
            [sys.executable, "-m", "incsync.cli", "run", "--state-dir", state_dir,
             "--page-size", "25", "--crash-at", "after_write:page1:24"],
            cwd=repo_root, env=env, capture_output=True, text=True, timeout=30,
        )
        # os._exit(137) terminates the interpreter immediately: no clean Python exit,
        # no output flushed by the normal return path.
        self.assertEqual(first.returncode, 137)

        second = subprocess.run(
            [sys.executable, "-m", "incsync.cli", "run", "--state-dir", state_dir, "--page-size", "25"],
            cwd=repo_root, env=env, capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(second.returncode, 0, second.stdout + second.stderr)
        import json
        result = json.loads(second.stdout)
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["unchanged"] + result["inserted"], 25)

        from incsync.mock_dest import Destination
        dest = Destination(os.path.join(state_dir, "dest.sqlite3"))
        dump = dest.dump()
        self.assertEqual(len(dump), 25)  # no loss, no duplication despite the hard kill
        dest.close()
