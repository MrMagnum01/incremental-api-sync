"""Single-runner enforcement: one local sync process at a time, no concurrency guarantee."""
from __future__ import annotations

import fcntl
import os

from .errors import LockHeld


class RunnerLock:
    def __init__(self, path: str):
        self.path = path
        self._fh = None

    def __enter__(self):
        self._fh = open(self.path, "a+")
        try:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._fh.close()
            self._fh = None
            raise LockHeld(f"another sync runner holds {self.path}")
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._fh:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            self._fh.close()
            self._fh = None
        return False
