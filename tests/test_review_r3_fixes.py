"""Astra r3 (40-sessions/2026-09-28-astra-incremental-sync-r3-review.md): deadline re-check after sleep overshoot,
and non-string outcomes treated as malformed (no TypeError)."""
import pytest
from incsync.engine import retry_fetch, apply_record, RetryBudgetExhausted
from incsync.clock import VirtualClock
from incsync.errors import RateLimited
from incsync.mock_source import SourceDataset


class Overshoot(VirtualClock):
    def sleep(self, s):
        super().sleep(s + 121)


class OneRateLimit:
    def __init__(self):
        self.calls = 0

    def fetch_page(self, *a):
        self.calls += 1
        if self.calls == 1:
            raise RateLimited("1")
        return "SUCCESS"


def test_sleep_overshoot_past_budget_does_not_dispatch():
    src = OneRateLimit()
    with pytest.raises(RetryBudgetExhausted):
        retry_fetch(src, None, None, Overshoot())
    assert src.calls == 1


@pytest.mark.parametrize("bad", [[], ["inserted"], {"inserted": 1}, 3, None])
def test_non_string_direct_outcome_is_unknown_not_typeerror(bad):
    ds = SourceDataset(); ds.seed("n", "1", {}); r = ds.snapshot_rows()[0]

    class Dest:
        def apply_op(self, *a, **kw):
            return bad
    outcome, why = apply_record(Dest(), r, None)
    assert outcome == "unknown" and why
