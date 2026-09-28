"""Injectable clock/sleeper so retry/backoff logic never performs a real sleep in tests."""
from __future__ import annotations
import datetime
import time

_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


class Clock:
    """Real clock, used only by the CLI entrypoint, never by tests. `now()` uses a
    monotonic source so retry/elapsed-budget accounting is never disturbed by a
    wall-clock adjustment (NTP step, DST, manual change); `utcnow()` stays on the
    wall clock since HTTP-date Retry-After values are calendar timestamps."""

    def now(self) -> float:
        return time.monotonic()

    def utcnow(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.timezone.utc)

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            time.sleep(seconds)


class VirtualClock:
    """Deterministic clock for tests: sleep() advances a counter instead of blocking.
    utcnow() maps the counter onto a synthetic calendar time so HTTP-date Retry-After
    parsing can be exercised without ever consulting the real wall clock."""

    def __init__(self, start: float = 0.0):
        self._t = start

    def now(self) -> float:
        return self._t

    def utcnow(self) -> datetime.datetime:
        return _EPOCH + datetime.timedelta(seconds=self._t)

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            self._t += seconds
