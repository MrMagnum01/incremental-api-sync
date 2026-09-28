"""CLI entrypoint used to prove *actual process termination* recovery: the crash
test spawns this as a real subprocess and lets it os._exit() mid-run, then runs it
again and checks the destination converges without loss or duplication.

Usage:
  python -m incsync.cli run --state-dir DIR [--page-size N] [--crash-at STAGE]
"""
from __future__ import annotations

import argparse
import json
import os
import sys

from .clock import Clock
from .demo_data import build_dataset
from .engine import run_sync
from .mock_dest import Destination
from .mock_source import MockSourceAPI

SYNC_ID = "widgets->mockdest#v1"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    run_p = sub.add_parser("run")
    run_p.add_argument("--state-dir", required=True)
    run_p.add_argument("--page-size", type=int, default=10)
    run_p.add_argument("--crash-at", default=None)
    args = parser.parse_args(argv)

    if args.cmd == "run":
        os.makedirs(args.state_dir, exist_ok=True)
        dest = Destination(os.path.join(args.state_dir, "dest.sqlite3"))
        snapshot_store = os.path.join(args.state_dir, "source_snapshots.json")
        source = MockSourceAPI(build_dataset(), page_size=args.page_size,
                                snapshot_store_path=snapshot_store)
        lock_path = os.path.join(args.state_dir, "runner.lock")
        clock = Clock()
        result = run_sync(source, dest, SYNC_ID, lock_path, clock,
                           crash_at=args.crash_at, crash_hard=True, source_id="widgets")
        print(json.dumps(result.as_dict()))
        dest.close()
        # `status` is fetch completeness only; a failed or unknown write must still
        # make the process exit non-zero even when the fetch itself completed.
        return 0 if result.ok else 1
    return 2


if __name__ == "__main__":
    sys.exit(main())
