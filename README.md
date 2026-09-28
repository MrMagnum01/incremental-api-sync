# Incremental API sync (demo)

Independent review: cleared by the company's reviewer at commit aa1d3a1 (scope: bounded synthetic portfolio demo only; full-source snapshots with incremental destination writes, no live vendor connector). Later commits are not covered by that review.

One-way **full-snapshot reconciliation with incremental destination writes**: every completed run rescans the source's entire current record set (not just what changed since the last checkpoint) and applies it against a mock destination, so only the records that are actually new or changed produce a write. This is not incremental *source* fetching and it does not retain source-side version history across runs — see "Pagination contract" and "Assumptions" below. Both source and destination are local, in-process contracts — clearly synthetic, not any real SaaS, not bound to a network port.

Upwork job types this demonstrates patterns for: "API integration / data sync", "sync CRM or e-commerce data between systems", "ETL from REST API".

**Out of scope, not built:** real vendor APIs, OAuth, two-way sync, schema migration. Nothing here claims to be a tested live CRM/vendor connector — only the patterns (pagination, idempotency, checkpointing, conflict handling, retries, accounting) are demonstrated, against a mock.

## Quickstart

```
python3 -m venv .venv
. .venv/bin/activate
python3 -m unittest discover -s tests -t . -v
```

No third-party runtime dependencies — standard library only. No `pip install` needed even in the venv.

## Architecture

- `incsync/mock_source.py` — the mock source API. A fetch with `cursor=None` freezes a **snapshot**: a token over the current record set, sorted deterministically by `(updated_at, id)`. Every later page for that token reuses the same token, the same `total` and `page_size`, and walks the frozen order without gaps, duplicates or going backwards. Mutations to the underlying dataset made after the freeze are invisible to that snapshot — this is what "bind page size and total to page 1" means in practice.
- `incsync/mock_dest.py` — the mock destination: a SQLite file with a `records` table, an `applied_ops` idempotency ledger, a `conflicts` log, and a `checkpoint` table. Every write to `records` and its corresponding `applied_ops` row commit in the same SQLite transaction, so a crash can never separate "the write happened" from "we can prove it happened".
- `incsync/engine.py` — fetch → validate → apply → checkpoint → account, one page at a time.
- `incsync/cli.py` — a real subprocess entrypoint used only to prove recovery across an actual OS-level process kill (see Checkpointing below).

## Assumptions this demo makes explicit

- **Mock only.** The source and destination are synthetic, in-process objects. No claim is made about safety against an arbitrary real, mutable vendor API.
- **Single runner.** One local sync process at a time, enforced with an `flock`-based lock (`incsync/lock.py`). There is no concurrent-sync guarantee; a second runner is refused outright, not queued.
- **Full-snapshot reconciliation, not incremental fetching.** Each run's page-1 freeze covers the source's *entire current record set*, walked in full every time — not a delta since the last checkpoint. Correctness relies on the destination-side idempotency ledger and version checks to turn that full rescan into incremental writes, not on the source only returning what changed. This demo assumes the source retains tombstones and change history until a consumer's checkpoint has moved past them; it does not model source-side garbage collection of history, and it does not implement or test retrieval of only-the-delta-since-last-run.
- **Snapshot durability is source-side, and publication of it is not crash-atomic.** The mock source persists frozen snapshots to a small JSON side-file (`source_snapshots.json` in the CLI's state dir) so a fresh process can resume a run that references a token it did not itself create — mirroring a real API's server-side pagination-cursor durability. That file write (`json.dump` to an existing path) is a plain overwrite, not a temp-file-plus-rename; a real crash mid-write could corrupt it. The demo's proven crash recovery (see Checkpointing) is scoped to the destination/checkpoint SQLite boundary, which *is* transactional — it does not harden or claim atomicity for this side-file publication step.
- **Checkpoint identity binding.** The checkpoint is bound to an explicit `source_id` (passed to `run_sync`), the destination's own identity (`Destination.identity`, its file path), the snapshot token, and the checkpoint's own `schema_version` — a mismatch on any of these raises `CheckpointIdentityMismatch` or `CheckpointCorrupt` rather than silently trusting a coincidentally-matching token. This does not extend to validating the shape of the *records themselves* against an external schema registry — only structural type checks on each record's fields (see Pagination contract).

## Pagination contract (acceptance #1)

Cursor = `(updated_at, id)` tuple, strictly increasing. The engine rejects, as a run-ending contract violation:
- non-advancing or repeated cursors (loop guard),
- `total`/`page_size` drift after page 1, or a page whose `snapshot_token` doesn't match the one frozen at page 1,
- duplicate identity/version keys within a page, non-monotonic record order, or a record with a malformed field (wrong type, empty id/namespace, non-positive version),
- a page that under-fills while still claiming more records remain (`has_more=true` with fewer than the established `page_size`) — a skipped or truncated page,
- a source that reports "done" (`has_more=false`) having delivered fewer records than the `total` it advertised at page 1 (a truncated page silently claiming completion),
- cumulative fetched records exceeding the `total` advertised at page 1 (extra/over-delivered records),
- fetching past `max_pages` — enforced *before* the next page is fetched, not after it has already been applied.

Coverage (`fetched == advertised`) is validated before the checkpoint can ever be saved as `"complete"` — on a fresh run and on a resumed one alike. Fetch coverage and the write-resolution cursor are tracked and persisted as two independent positions specifically so a resumed run that under-delivers its final page is still caught (not just a fresh, uninterrupted one), while a resume that only re-fetches an already-seen page tail to retry a blocked write is not double-counted as new coverage. See `tests/test_pagination.py` and `tests/test_astra_review_fixes.py`.

## Idempotency (acceptance #2)

Idempotency key = `namespace:id:version:operation` with each component individually escaped (`incsync/ledger.py:op_key`), paired with a canonical SHA-256 digest of the payload (tombstone payload for deletes). Escaping prevents a separator embedded in a namespace or id from colliding two distinct records into the same key (e.g. `namespace="a:b", id="c"` vs `namespace="a", id="b:c"`). The key is version-bound, so:
- a legitimate later update (new version) is never swallowed as a duplicate of an earlier one,
- the same key replayed with the **same** digest is a true no-op (`unchanged`, no duplicate write),
- the same key replayed with a **different** digest is refused (`OpKeyReused`), never silently accepted.

**Lost-acknowledgement handling:** the mock destination can simulate a write that commits but whose acknowledgement never reaches the caller (`ack_mode="lost_ack_confirmable"`). The engine responds by reading the destination back by operation key and verifies the confirmation actually names *this* operation — op_key, namespace, id, version, op and digest must all match, not just the digest — before trusting it (a confirmation for a foreign identity is never mistaken for confirming this one). If it finds a genuine match, it reports the real, confirmed outcome (no duplicate, no re-write).

Two distinct destination-outage shapes are modelled and tested separately: `"lost_ack_unconfirmable"` fails *before* the write ever commits — nothing to find on confirmation, so the engine records `UNKNOWN` from a genuinely absent lookup. `"committed_then_confirmation_unreadable"` is the other case: the write *does* commit durably, but the confirmation read immediately afterward also fails (a transient outage, not a permanent one) — the engine still records `UNKNOWN`, but a later replay under the same key finds the real, already-applied outcome and resolves cleanly with no duplicate write. A lookup that raises outright (rather than returning nothing) is likewise turned into `UNKNOWN`, never left to escape the engine's accounting as an unhandled exception. See `tests/test_idempotency.py` and `tests/test_astra_review_fixes.py`.

## Checkpointing (acceptance #3)

The checkpoint (`records`/`applied_ops`/`checkpoint` all live in the same SQLite file) advances only past the **contiguous fully-resolved prefix** of a page: the first `failed` or `unknown` outcome in a page blocks the checkpoint from moving past it, even though later records in that page may already have been durably written (idempotent replay makes re-processing them on resume safe).

Recovery is proven by:
- **in-process crash injection** (`incsync/errors.py:SimulatedCrash`) at `before_write`, `after_write`, and `before_checkpoint`, per-record granularity — `tests/test_checkpoint_crash.py::TestCrashRecovery`.
- **an actual OS process kill**: `tests/test_checkpoint_crash.py::TestRealProcessCrash` spawns `python -m incsync.cli run` as a real subprocess, has it call `os._exit(137)` right after committing the last record's write and before saving the checkpoint, confirms the process died non-gracefully (`returncode == 137`), then re-runs the same CLI command and confirms the destination converges with no loss and no duplication.

A run's own `status` field is about **fetch** completeness (did we retrieve the whole snapshot); the checkpoint's own status is about **write** completeness. A run can legitimately report `status: "complete"` (fetch finished) while its checkpoint stays `in_progress` (a write is still blocked) — the two are deliberately different axes. `RunResult.ok` is the single overall-success field: `False` whenever `status != "complete"` **or** any write is `failed`/`unknown`, so a fetch-complete run with an unresolved write is never mistaken for success (see Accounting).

The checkpoint is bound to `source_id`, `Destination.identity`, the snapshot token and the checkpoint's own `schema_version` (see Assumptions above) — a checkpoint with an unrecognised `schema_version` or status value raises `CheckpointCorrupt` (fails closed instead of restarting blind), and one bound to a different source or destination identity raises `CheckpointIdentityMismatch`.

## Conflict policy (acceptance #4)

**One-way, source-authoritative.** Updates apply as last-writer-by-version (`incsync/mock_dest.py:apply_op`): an incoming version must be strictly greater than what the destination holds, or it is rejected as `StaleVersion` — this also means an older replay can never resurrect a newer tombstone. Deletes propagate as tombstones (`deleted=1`, payload cleared), not row removal, so their version still participates in the same ordering.

**Local destination edits** (`Destination.local_edit`, simulating an out-of-band edit made directly against the destination) are overridden **only when a genuinely newer source version next arrives for that record**, not on every subsequent sync — an unchanged replay (same version, matching digest) is a true idempotent no-op that never touches the `records` row at all, so a local edit survives it untouched. When a newer version does arrive, the override is never silent: it is logged to the `conflicts` table (`kind="local_edit_overridden"`) so the event is auditable even though the documented policy is "source always wins". Tested in `tests/test_conflicts.py` and `tests/test_astra_review_fixes.py`.

## Rate limits and errors (acceptance #5)

`incsync/engine.py:retry_fetch` retries `429` (honouring `Retry-After`, parsed as either seconds or an HTTP-date via `parse_retry_after`) and transient `5xx`, bounded to 5 attempts and **120 seconds of actual elapsed time on the injected clock** — measured from before the first attempt, so a slow attempt's own duration counts against the budget, not just the backoff sleeps between attempts. HTTP-date `Retry-After` values are resolved against the same injected clock (`Clock.utcnow` / `VirtualClock.utcnow`), never the real wall clock, so this is deterministically testable. A non-finite or negative numeric delay, or an unparseable date, is rejected as a categorised `MalformedResponse` rather than silently coerced. If the required delay would exceed the remaining budget, the engine gives up rather than sleeping past it ("defer rather than retry too early"). Malformed response bodies become a categorised, run-ending `MalformedResponse` failure — never silent success. Tests inject an in-process `VirtualClock` (`incsync/clock.py`) so no test ever performs a real `sleep`.

Authentication, permanent-validation and conflict errors (`StaleVersion`, `OpKeyReused`) are never retried by design — they are not raised from the fetch path at all.

**Honesty about the mock's own limits:** a mock in-process call cannot be interrupted mid-flight the way a real network request timing out could be — `retry_fetch`'s elapsed-time accounting bounds how much time is *recorded* against the budget, it does not demonstrate cancelling a request already in progress.

## Accounting (acceptance #6)

Every run returns a `RunResult` (`incsync/engine.py`) where, by construction:

```
attempted == inserted + updated + deleted + unchanged + failed + unknown
```

`advertised` (page-1 total), `fetched`, `attempted` and `unattempted` are reported as separate fields, not folded together — `unattempted` is the source deficit (`advertised - fetched`), tracked as true cumulative coverage across restarts (see Checkpointing), so it is meaningful whether the run started fresh or resumed. `unknown` is used exactly when a write's outcome could not be established even after attempting to confirm it by reading the destination back. An empty snapshot is a valid, `complete` run with an explicit `total: 0`, distinguished from a failed fetch. `tests/test_accounting.py::test_final_state_reconciles_against_independent_synthetic_ledger` builds its expected end-state independently of the engine and diffs it against `Destination.dump()` per-record, not just by totals.

**Overall success is `RunResult.ok`, not `status`.** `status` is fetch completeness only; a run can fetch the entire snapshot (`status == "complete"`) while a write stays `failed` or `unknown`. `ok` is `False` whenever `status != "complete"` or `failed + unknown > 0`, and the CLI's process exit code is gated on it (`0` only when `ok`), so an unresolved or unknown write is never reported as success. See `tests/test_astra_review_fixes.py::TestOverallFailureSignalling`.

## Self-check performed before delivery

- Fresh clone, fresh venv, `python3 -m unittest discover -s tests -t . -v` — all tests pass, no network access, no third-party packages installed.
- `gitleaks detect` over the working tree and full git history — no findings.
- `LICENSES.md` lists every dependency actually used (none beyond the Python standard library).
