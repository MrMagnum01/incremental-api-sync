"""The sync engine: fetch, validate, apply, checkpoint, account. One run = one call
to run_sync(). See README.md "Conflict policy" and "Accounting" sections for the
rules this file implements.
"""
from __future__ import annotations

import datetime as _dt
import email.utils
import math
import os
from dataclasses import dataclass
from typing import Callable, Optional

from .errors import (
    CheckpointCorrupt,
    CheckpointIdentityMismatch,
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
from .mock_dest import CHECKPOINT_SCHEMA_VERSION, CHECKPOINT_STATUSES

MAX_ATTEMPTS = 5
MAX_ELAPSED_SECONDS = 120.0


def parse_retry_after(value, clock=None) -> float:
    """Retry-After per RFC 7231: either an integer number of seconds, or an HTTP-date.
    `clock`, when given, supplies "now" for HTTP-date deltas so a test's virtual clock
    is authoritative instead of the real wall clock. Non-finite or negative numeric
    delays and unparseable dates are categorised failures, never silently coerced."""
    if isinstance(value, (int, float)):
        if isinstance(value, bool) or not math.isfinite(value) or value < 0:
            raise MalformedResponse(f"invalid Retry-After value: {value!r}")
        return float(value)
    s = str(value).strip()
    if s.isdigit():
        return float(s)
    try:
        dt = email.utils.parsedate_to_datetime(s)
    except (TypeError, ValueError) as e:
        raise MalformedResponse(f"unparseable Retry-After value: {value!r} ({e})")
    if dt is None:
        raise MalformedResponse(f"unparseable Retry-After value: {value!r}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    now = clock.utcnow() if clock is not None else _dt.datetime.now(_dt.timezone.utc)
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

    @property
    def ok(self) -> bool:
        """Overall run success. `status` is fetch completeness only (see module
        docstring); a run can fetch the whole snapshot (`status == "complete"`) while
        leaving a write `failed` or `unknown` -- that must never look like success.
        `ok` is the single field callers (and the CLI exit code) should gate on."""
        return self.status == "complete" and self.failed == 0 and self.unknown == 0

    def as_dict(self):
        return dict(
            status=self.status, ok=self.ok, advertised=self.advertised, fetched=self.fetched,
            attempted=self.attempted, unattempted=self.unattempted,
            inserted=self.inserted, updated=self.updated, deleted=self.deleted,
            unchanged=self.unchanged, failed=self.failed, unknown=self.unknown,
            failures=self.failures, stop_reason=self.stop_reason,
        )


def retry_fetch(source, token, cursor, clock):
    """Bound retries by attempt count AND by actual elapsed time on `clock`, measured
    from before the first attempt to after each failure -- including whatever time an
    attempt itself consumed (e.g. a slow request), not just the backoff sleeps between
    attempts. A single mock in-process call cannot be interrupted mid-flight the way a
    real network request could be; this only bounds the *accounted* elapsed time."""
    attempts = 0
    start = clock.now()
    while True:
        attempts += 1
        try:
            return source.fetch_page(token, cursor)
        except RateLimited as e:
            wait = parse_retry_after(e.retry_after, clock)
        except TransientServerError:
            wait = min(2.0 ** attempts, 30.0)
        elapsed = clock.now() - start
        if attempts >= MAX_ATTEMPTS or elapsed >= MAX_ELAPSED_SECONDS or elapsed + wait >= MAX_ELAPSED_SECONDS:
            # Equality is exhausted: a retry that would only start dispatching once
            # the budget is fully spent (including sleep overshoot) never dispatches.
            raise RetryBudgetExhausted(
                f"gave up after {attempts} attempt(s), {elapsed:.1f}s elapsed")
        clock.sleep(wait)


# The only outcomes apply_op can legitimately report for each operation kind. A
# mapping-shaped response that names any other string (or a response for the wrong
# operation, e.g. "inserted" for a delete) is malformed, not a confirmed success.
_ALLOWED_OUTCOMES_BY_OP = {
    "upsert": frozenset({"inserted", "updated", "unchanged"}),
    "delete": frozenset({"deleted", "unchanged"}),
}


def _confirms(confirmed: Optional[dict], key: str, r, op: str, digest: str) -> bool:
    """A confirmation read is only trusted when it names the exact operation asked
    for: op_key, namespace, id, version, op and payload digest must all match, and
    its outcome is one of the outcomes that operation can legitimately produce. A
    destination that returns data for a different key entirely (a foreign namespace/
    id/version/op confirmation), or a mapping with an outcome that operation could
    never have produced, must never be mistaken for confirming this one."""
    if not isinstance(confirmed, dict):
        return False
    return (
        confirmed.get("op_key") == key
        and confirmed.get("namespace") == r.namespace
        and confirmed.get("id") == r.id
        and confirmed.get("version") == r.version
        and confirmed.get("op") == op
        and confirmed.get("digest") == digest
        and confirmed.get("outcome") in _ALLOWED_OUTCOMES_BY_OP.get(op, frozenset())
    )


def apply_record(dest, r, ack_mode_fn):
    op = "delete" if r.deleted else "upsert"
    payload = TOMBSTONE if r.deleted else r.payload
    digest = payload_digest(payload)
    key = make_op_key(r.namespace, r.id, r.version, op)
    ack_mode = ack_mode_fn(r.namespace, r.id, r.version, op) if ack_mode_fn else "ok"
    try:
        outcome = dest.apply_op(key, r.namespace, r.id, r.version, op, digest, payload, ack_mode=ack_mode)
        if outcome not in _ALLOWED_OUTCOMES_BY_OP.get(op, frozenset()):
            return "unknown", f"destination returned an invalid direct outcome: {outcome!r}"
        return outcome, None
    except AckLost:
        try:
            confirmed = dest.get_op(key)
        except Exception as lookup_exc:
            return "unknown", f"ack_lost_and_confirmation_lookup_failed: {lookup_exc}"
        if _confirms(confirmed, key, r, op, digest):
            return confirmed["outcome"], "confirmed_after_lost_ack"
        return "unknown", "ack_lost_and_unconfirmed"
    except DestinationUnavailable as e:
        try:
            confirmed = dest.get_op(key)
        except Exception:
            return "unknown", str(e)
        if _confirms(confirmed, key, r, op, digest):
            return confirmed["outcome"], "confirmed_after_outage"
        return "unknown", str(e)
    except OpKeyReused as e:
        return "failed", str(e)
    except StaleVersion as e:
        return "failed", str(e)


def run_sync(source, dest, sync_id: str, lock_path: str, clock,
             crash_at: Optional[str] = None, crash_hard: bool = False,
             ack_mode_fn: Optional[Callable] = None, max_pages: int = 10000,
             source_id: str = "default-source") -> RunResult:
    """One run = fetch -> validate -> apply -> checkpoint -> account, one page at a
    time. The checkpoint is bound to `source_id` and `dest.identity` (in addition to
    the snapshot token and its own schema_version): resuming against a swapped source
    or destination under the same sync_id raises CheckpointIdentityMismatch instead
    of silently trusting a coincidentally-matching token. A checkpoint with an
    unrecognised schema_version or status raises CheckpointCorrupt -- it fails closed
    rather than restarting blind. Fetch coverage (`fetched` vs the page-1 `advertised`
    total) is persisted across restarts so a resumed run that under-delivers a final
    page is still caught, not just a fresh one.

    `cursor` (the write-resolution boundary) and `fetch_cursor` (the fetch high-water
    mark) are tracked and persisted separately. A resume driven by a blocked write
    rewinds `cursor` and legitimately re-fetches records already seen, to retry their
    write -- those must not be re-counted as newly fetched. `fetch_cursor` only ever
    advances, so only records past it are added to `fetched`, however the two cursors
    happen to overlap on any given resume.
    """

    def crash_maybe(stage: str):
        if crash_at is not None and stage == crash_at:
            if crash_hard:
                os._exit(137)
            raise SimulatedCrash(stage)

    dest_id = dest.identity

    with RunnerLock(lock_path):
        ckpt = dest.load_checkpoint(sync_id)
        advertised = None
        page_size_established = None
        fetched = 0
        fetch_cursor = None
        token = None
        cursor = None
        if ckpt is not None:
            if (ckpt["schema_version"] != CHECKPOINT_SCHEMA_VERSION
                    or ckpt["status"] not in CHECKPOINT_STATUSES):
                raise CheckpointCorrupt(
                    f"{sync_id}: unreadable checkpoint (schema_version="
                    f"{ckpt['schema_version']!r}, status={ckpt['status']!r}) -- "
                    f"refusing to restart blind")
            if ckpt["source_id"] is None or ckpt["dest_id"] is None:
                raise CheckpointCorrupt(
                    f"{sync_id}: checkpoint missing required source_id/dest_id binding "
                    f"(source_id={ckpt['source_id']!r}, dest_id={ckpt['dest_id']!r}) -- "
                    f"a schema_version={CHECKPOINT_SCHEMA_VERSION} checkpoint must carry both; "
                    f"refusing to resume in an unbound compatibility mode")
            if ckpt["source_id"] != source_id:
                raise CheckpointIdentityMismatch(
                    f"{sync_id}: checkpoint bound to source_id={ckpt['source_id']!r}, "
                    f"this run is source_id={source_id!r}")
            if ckpt["dest_id"] != dest_id:
                raise CheckpointIdentityMismatch(
                    f"{sync_id}: checkpoint bound to dest_id={ckpt['dest_id']!r}, "
                    f"this run is dest_id={dest_id!r}")
            if ckpt["status"] == "in_progress":
                if not isinstance(ckpt["snapshot_token"], str) or not ckpt["snapshot_token"]:
                    raise CheckpointCorrupt(
                        f"{sync_id}: in_progress checkpoint has no usable snapshot_token "
                        f"({ckpt['snapshot_token']!r})")
                if (not isinstance(ckpt["advertised_total"], int)
                        or isinstance(ckpt["advertised_total"], bool)
                        or ckpt["advertised_total"] < 0):
                    raise CheckpointCorrupt(
                        f"{sync_id}: in_progress checkpoint has an invalid advertised_total "
                        f"({ckpt['advertised_total']!r})")
                if (not isinstance(ckpt["page_size"], int)
                        or isinstance(ckpt["page_size"], bool)
                        or ckpt["page_size"] < 1):
                    raise CheckpointCorrupt(
                        f"{sync_id}: in_progress checkpoint has an invalid page_size "
                        f"({ckpt['page_size']!r})")
                if (not isinstance(ckpt["fetched_count"], int)
                        or isinstance(ckpt["fetched_count"], bool)
                        or ckpt["fetched_count"] < 0):
                    raise CheckpointCorrupt(
                        f"{sync_id}: in_progress checkpoint has an invalid fetched_count "
                        f"({ckpt['fetched_count']!r}) -- a missing count is not an implicit 0")
                if ckpt["fetched_count"] > ckpt["advertised_total"]:
                    raise CheckpointCorrupt(
                        f"{sync_id}: in_progress checkpoint fetched_count "
                        f"{ckpt['fetched_count']} exceeds advertised_total {ckpt['advertised_total']}")
                token = ckpt["snapshot_token"]
                cursor = ckpt["cursor"]
                advertised = ckpt["advertised_total"]
                page_size_established = ckpt["page_size"]
                fetched = ckpt["fetched_count"]
                fetch_cursor = ckpt["fetch_cursor"]

        stats = {"inserted": 0, "updated": 0, "deleted": 0, "unchanged": 0, "failed": 0, "unknown": 0}
        failures = []
        seen_cursors = set()
        run_status = "in_progress"
        stop_reason = None
        page_num = 0

        while True:
            page_num += 1
            if page_num > max_pages:
                page_num -= 1
                run_status = "incomplete"
                stop_reason = "max_pages_exceeded"
                break
            crash_maybe(f"before_fetch:page{page_num}")
            try:
                page = retry_fetch(source, token, cursor, clock)
            except (RetryBudgetExhausted, SnapshotExpired, MalformedResponse) as e:
                run_status = "incomplete"
                stop_reason = f"{type(e).__name__}: {e}"
                break

            if not isinstance(page.snapshot_token, str) or not page.snapshot_token:
                run_status = "incomplete"
                stop_reason = "contract_violation: snapshot_token must be a non-empty string"
                break
            if token is None:
                token = page.snapshot_token
            elif page.snapshot_token != token:
                run_status = "incomplete"
                stop_reason = "contract_violation: snapshot token drifted mid-run"
                break

            if (not isinstance(page.total, int) or isinstance(page.total, bool) or page.total < 0
                    or not isinstance(page.page_size, int) or isinstance(page.page_size, bool)
                    or page.page_size < 1
                    or not isinstance(page.has_more, bool)
                    or not isinstance(page.records, list)):
                run_status = "incomplete"
                stop_reason = ("contract_violation: malformed page metadata "
                                "(total/page_size/has_more/records)")
                break

            if advertised is None:
                advertised = page.total
                page_size_established = page.page_size
            elif page.total != advertised or page.page_size != page_size_established:
                run_status = "incomplete"
                stop_reason = "contract_violation: total/page_size drifted mid-run"
                break

            if page.next_cursor is not None:
                nc = page.next_cursor
                if (not isinstance(nc, tuple) or len(nc) != 2
                        or not isinstance(nc[0], (int, float)) or isinstance(nc[0], bool)
                        or not math.isfinite(nc[0])
                        or not isinstance(nc[1], str) or not nc[1]):
                    run_status = "incomplete"
                    stop_reason = "contract_violation: malformed next_cursor"
                    break
            if page.has_more and page.next_cursor is None:
                run_status = "incomplete"
                stop_reason = "contract_violation: has_more is true but next_cursor is missing"
                break
            if not page.has_more and page.next_cursor is not None:
                run_status = "incomplete"
                stop_reason = "contract_violation: terminal page must not carry a next_cursor"
                break

            prev_key = cursor
            violation = None
            ids_seen = set()
            for r in page.records:
                if not all(hasattr(r, attr) for attr in
                           ("namespace", "id", "version", "updated_at", "deleted", "payload")):
                    violation = "malformed record: item is not a record"
                elif not isinstance(r.namespace, str) or not r.namespace:
                    violation = "malformed record: namespace must be a non-empty string"
                elif not isinstance(r.id, str) or not r.id:
                    violation = "malformed record: id must be a non-empty string"
                elif not isinstance(r.version, int) or isinstance(r.version, bool) or r.version < 1:
                    violation = "malformed record: version must be a positive int"
                elif (not isinstance(r.updated_at, (int, float)) or isinstance(r.updated_at, bool)
                        or not math.isfinite(r.updated_at)):
                    violation = "malformed record: updated_at must be finite numeric"
                elif not isinstance(r.deleted, bool):
                    violation = "malformed record: deleted must be boolean"
                elif not r.deleted and not isinstance(r.payload, dict):
                    violation = "malformed record: payload must be an object"
                if violation:
                    break
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

            if page.has_more and len(page.records) < page_size_established:
                run_status = "incomplete"
                stop_reason = ("contract_violation: page under-filled while claiming more "
                                "records remain (skipped or truncated page)")
                break

            # Only count records past the fetch high-water mark: a resume driven by a
            # blocked write re-fetches the tail of an already-fetched page, and that
            # must not double-count coverage against `advertised`.
            newly_fetched = [r for r in page.records if fetch_cursor is None or (r.updated_at, r.id) > fetch_cursor]
            fetched += len(newly_fetched)
            if page.records:
                last = page.records[-1]
                fetch_cursor = (last.updated_at, last.id)
            if fetched > advertised:
                run_status = "incomplete"
                stop_reason = (f"contract_violation: fetched {fetched} exceeds advertised "
                                f"total {advertised} (extra records)")
                break

            if len(page.records) > page_size_established:
                run_status = "incomplete"
                stop_reason = ("contract_violation: page delivered more records than "
                                "page_size (overfilled page)")
                break

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

            # Coverage (fetched == advertised) is checked here, before the checkpoint
            # is ever marked "complete" -- on a fresh run and on a resumed one alike --
            # so a truncated final page can never be persisted as done.
            coverage_complete = (not page.has_more) and not page_blocked and fetched == advertised
            truncated = (not page.has_more) and not page_blocked and fetched != advertised

            crash_maybe(f"before_checkpoint:page{page_num}")
            new_status = "complete" if coverage_complete else "in_progress"
            dest.save_checkpoint(sync_id, token, resolved_prefix_cursor, new_status,
                                  source_id=source_id, dest_id=dest_id,
                                  advertised_total=advertised, page_size=page_size_established,
                                  fetched_count=fetched, fetch_cursor=fetch_cursor)
            crash_maybe(f"after_checkpoint:page{page_num}")
            cursor = resolved_prefix_cursor

            if page_blocked:
                # Stop pulling further pages: progress cannot checkpoint past this
                # point anyway, and re-fetching from the rewound cursor on this same
                # call would just re-derive the same block. A later run resumes here.
                stop_reason = stop_reason or "blocked_on_unresolved_record"
                run_status = "complete" if (not page.has_more and fetched == advertised) else "incomplete"
                break

            if not page.has_more:
                if truncated:
                    run_status = "incomplete"
                    stop_reason = (f"contract_violation: source reported done but delivered "
                                    f"{fetched}/{advertised} records (truncated)")
                else:
                    run_status = "complete"
                break

        attempted = sum(stats.values())
        unattempted = max(0, (advertised or 0) - fetched)
        return RunResult(
            status=run_status, advertised=advertised or 0, fetched=fetched,
            attempted=attempted, unattempted=unattempted,
            inserted=stats["inserted"], updated=stats["updated"], deleted=stats["deleted"],
            unchanged=stats["unchanged"], failed=stats["failed"], unknown=stats["unknown"],
            failures=failures, stop_reason=stop_reason,
        )
