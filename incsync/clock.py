"""Injectable clock/sleeper so retry/backoff logic never performs a real sleep in tests."""
from __future__ import annotations
import time


class Clock:
    """Real wall-clock. Used only by the CLI entrypoint, never by tests."""

    def now(self) -> float:
        return time.time()

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            time.sleep(seconds)


class VirtualClock:
    """Deterministic clock for tests: sleep() advances a counter instead of blocking."""

    def __init__(self, start: float = 0.0):
        self._t = start

    def now(self) -> float:
        return self._t

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            self._t += seconds
