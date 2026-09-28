"""The sync engine: fetch, validate, apply, checkpoint, account. One run = one call
to run_sync(). See README.md "Conflict policy" and "Accounting" sections for the
rules this file implements.
"""
from __future__ import annotations

import email.utils
import os
from dataclasses import dataclass
from typing import Callable, Optional

from .errors import (
    MalformedResponse,
    RateLimited,
    RetryBudgetExhausted,
    SnapshotExpired,
    TransientServerError,
    AckLost,
    DestinationUnavailable,
    OpKeyReused,
    StaleVersion,
    SimulatedCrash,
)
from .ledger import TOMBSTONE, op_key as make_op_key, payload_digest
from .lock import RunnerLock

MAX_ATTEMPTS = 5
MAX_ELAPSED_SECONDS = 120.0


def parse_retry_after(value) -> float:
    """Retry-After per RFC 7231: either an integer number of seconds, or an HTTP-date."""
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if s.isdigit():
        return float(s)
    dt = email.utils.parsedate_to_datetime(s)
    import datetime as _dt
    now = _dt.datetime.now(_dt.timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    return max(0.0, (dt - now).total_seconds())


@dataclass
class RunResult:
    status: str  # "complete" | "incomplete"
    advertised: int
    fetched: int
    attempted: int
    unattempted: int
    inserted: int
    updated: int
    deleted: int
    unchanged: int
    failed: int
    unknown: int
    failures: list
    stop_reason: Optional[str]

    def as_dict(self):
        return dict(
            status=self.status, advertised=self.advertised, fetched=self.fetched,
            attempted=self.attempted, unattempted=self.unattempted,
            inserted=self.inserted, updated=self.updated, deleted=self.deleted,
            unchanged=self.unchanged, failed=self.failed, unknown=self.unknown,
            failures=self.failures, stop_reason=self.stop_reason,
        )


def retry_fetch(source, token, cursor, clock):
    attempts = 0
    elapsed = 0.0
    while True:
        attempts += 1
        try:
            return source.fetch_page(token, cursor)
        except RateLimited as e:
            wait = parse_retry_after(e.retry_after)
        except TransientServerError:
            wait = min(2.0 ** attempts, 30.0)
        if attempts >= MAX_ATTEMPTS or elapsed + wait > MAX_ELAPSED_SECONDS:
            raise RetryBudgetExhausted(
                f"gave up after {attempts} attempt(s), {elapsed:.1f}s elapsed")
        clock.sleep(wait)
        elapsed += wait


def apply_record(dest, r, ack_mode_fn):
    op = "delete" if r.deleted else "upsert"
    payload = TOMBSTONE if r.deleted else r.payload
    digest = payload_digest(payload)
    key = make_op_key(r.namespace, r.id, r.version, op)
    ack_mode = ack_mode_fn(r.namespace, r.id, r.version, op) if ack_mode_fn else "ok"
    try:
        outcome = dest.apply_op(key, r.namespace, r.id, r.version, op, digest, payload, ack_mode=ack_mode)
        return outcome, None
    except AckLost:
        confirmed = dest.get_op(key)
        if confirmed and confirmed["digest"] == digest:
            return confirmed["outcome"], "confirmed_after_lost_ack"
        return "unknown", "ack_lost_and_unconfirmed"
    except DestinationUnavailable as e:
        confirmed = dest.get_op(key)
        if confirmed and confirmed["digest"] == digest:
            return confirmed["outcome"], "confirmed_after_outage"
        return "unknown", str(e)
    except OpKeyReused as e:
        return "failed", str(e)
    except StaleVersion as e:
        return "failed", str(e)


def run_sync(source, dest, sync_id: str, lock_path: str, clock,
             crash_at: Optional[str] = None, crash_hard: bool = False,
             ack_mode_fn: Optional[Callable] = None, max_pages: int = 10000) -> RunResult:

    def crash_maybe(stage: str):
        if crash_at is not None and stage == crash_at:
            if crash_hard:
                os._exit(137)
            raise SimulatedCrash(stage)

    with RunnerLock(lock_path):
        ckpt = dest.load_checkpoint(sync_id)
        if ckpt and ckpt["status"] == "in_progress":
            token = ckpt["snapshot_token"]
            cursor = ckpt["cursor"]
        else:
            token = None
            cursor = None
        started_fresh = cursor is None

        stats = {"inserted": 0, "updated": 0, "deleted": 0, "unchanged": 0, "failed": 0, "unknown": 0}
        failures = []
        advertised = None
        page_size_established = None
        fetched = 0
        seen_cursors = set()
        run_status = "in_progress"
        stop_reason = None
        page_num = 0

        while True:
            page_num += 1
            crash_maybe(f"before_fetch:page{page_num}")
            try:
                page = retry_fetch(source, token, cursor, clock)
            except (RetryBudgetExhausted, SnapshotExpired, MalformedResponse) as e:
                run_status = "incomplete"
                stop_reason = f"{type(e).__name__}: {e}"
                break

            if token is None:
                token = page.snapshot_token
            if advertised is None:
                advertised = page.total
                page_size_established = page.page_size
            elif page.total != advertised or page.page_size != page_size_established:
                run_status = "incomplete"
                stop_reason = "contract_violation: total/page_size drifted mid-run"
                break

            prev_key = cursor
            violation = None
            ids_seen = set()
            for r in page.records:
                key = (r.updated_at, r.id)
                if prev_key is not None and key <= prev_key:
                    violation = "non-monotonic (updated_at,id) order"
                    break
                if (r.namespace, r.id) in ids_seen:
                    violation = "duplicate identity/version key within page"
                    break
                ids_seen.add((r.namespace, r.id))
                prev_key = key
            if violation:
                run_status = "incomplete"
                stop_reason = f"contract_violation: {violation}"
                break

            if page.next_cursor is not None:
                if page.next_cursor in seen_cursors or (cursor is not None and page.next_cursor <= cursor):
                    run_status = "incomplete"
                    stop_reason = "contract_violation: non-advancing cursor"
                    break
                seen_cursors.add(page.next_cursor)

            fetched += len(page.records)
            resolved_prefix_cursor = cursor
            page_blocked = False

            for idx, r in enumerate(page.records):
                crash_maybe(f"before_write:page{page_num}:{idx}")
                outcome, detail = apply_record(dest, r, ack_mode_fn)
                crash_maybe(f"after_write:page{page_num}:{idx}")
                stats[outcome] += 1
                if outcome in ("failed", "unknown"):
                    page_blocked = True
                    failures.append({
                        "namespace": r.namespace, "id": r.id, "version": r.version,
                        "outcome": outcome, "detail": detail,
                    })
                if not page_blocked:
                    resolved_prefix_cursor = (r.updated_at, r.id)

            crash_maybe(f"before_checkpoint:page{page_num}")
            new_status = "in_progress" if (page.has_more or page_blocked) else "complete"
            dest.save_checkpoint(sync_id, token, resolved_prefix_cursor, new_status)
            crash_maybe(f"after_checkpoint:page{page_num}")
            cursor = resolved_prefix_cursor

            if page_blocked:
                # Stop pulling further pages: progress cannot checkpoint past this
                # point anyway, and re-fetching from the rewound cursor on this same
                # call would just re-derive the same block. A later run resumes here.
                stop_reason = stop_reason or "blocked_on_unresolved_record"
                if not page.has_more and (not started_fresh or fetched == advertised):
                    run_status = "complete"  # snapshot was fully retrieved; only writes are blocked
                else:
                    run_status = "incomplete"
                break

            if not page.has_more:
                if started_fresh and fetched != advertised:
                    run_status = "incomplete"
                    stop_reason = (f"contract_violation: source reported done but delivered "
                                    f"{fetched}/{advertised} records (truncated)")
                else:
                    run_status = "complete"
                break
            if page_num > max_pages:
                run_status = "incomplete"
                stop_reason = "max_pages_exceeded"
                break

        attempted = sum(stats.values())
        unattempted = max(0, (advertised or 0) - fetched) if started_fresh else 0
        return RunResult(
            status=run_status, advertised=advertised or 0, fetched=fetched,
            attempted=attempted, unattempted=unattempted,
            inserted=stats["inserted"], updated=stats["updated"], deleted=stats["deleted"],
            unchanged=stats["unchanged"], failed=stats["failed"], unknown=stats["unknown"],
            failures=failures, stop_reason=stop_reason,
        )
