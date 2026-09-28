# Incremental API sync (demo)

Synthetic portfolio demonstration, implemented with AI coding agents; independent review pending. No client data or client work.

One-way incremental sync of records from a mock source REST API into a mock destination system. Both are local, in-process contracts — clearly synthetic, not any real SaaS, not bound to a network port.

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
- **Retention.** This demo assumes the source retains tombstones and change history until a consumer's checkpoint has moved past them. It does not model source-side garbage collection of history.
- **Snapshot durability is source-side.** The mock source persists frozen snapshots to a small JSON side-file (`source_snapshots.json` in the CLI's state dir) so a fresh process can resume a run that references a token it did not itself create — mirroring a real API's server-side pagination-cursor durability. The demo does not persist or re-validate identity/schema binding across a source/destination swap; only the token match is checked.

## Pagination contract (acceptance #1)

Cursor = `(updated_at, id)` tuple, strictly increasing. The engine rejects, as a run-ending contract violation:
- non-advancing or repeated cursors (loop guard),
- `total`/`page_size` drift after page 1,
- duplicate identity/version keys within a page,
- non-monotonic record order,
- a source that reports "done" (`has_more=false`) having delivered fewer records than the `total` it advertised at page 1 (a truncated page silently claiming completion).

A fresh, uninterrupted run is checked end-to-end: `fetched == advertised` or the run is `incomplete`. See `tests/test_pagination.py`.

## Idempotency (acceptance #2)

Idempotency key = `namespace:id:version:operation` (`incsync/ledger.py:op_key`), paired with a canonical SHA-256 digest of the payload (tombstone payload for deletes). The key is version-bound, so:
- a legitimate later update (new version) is never swallowed as a duplicate of an earlier one,
- the same key replayed with the **same** digest is a true no-op (`unchanged`, no duplicate write),
- the same key replayed with a **different** digest is refused (`OpKeyReused`), never silently accepted.

**Lost-acknowledgement handling:** the mock destination can simulate a write that commits but whose acknowledgement never reaches the caller (`ack_mode="lost_ack_confirmable"`). The engine responds by reading the destination back by operation key: if it finds a matching digest, it reports the real, confirmed outcome (no duplicate, no re-write). If the destination cannot even be read back (`"lost_ack_unconfirmable"`), the engine refuses to guess and records `UNKNOWN` — never inferring success or absence from an inconclusive signal. See `tests/test_idempotency.py`.

## Checkpointing (acceptance #3)

The checkpoint (`records`/`applied_ops`/`checkpoint` all live in the same SQLite file) advances only past the **contiguous fully-resolved prefix** of a page: the first `failed` or `unknown` outcome in a page blocks the checkpoint from moving past it, even though later records in that page may already have been durably written (idempotent replay makes re-processing them on resume safe).

Recovery is proven by:
- **in-process crash injection** (`incsync/errors.py:SimulatedCrash`) at `before_write`, `after_write`, and `before_checkpoint`, per-record granularity — `tests/test_checkpoint_crash.py::TestCrashRecovery`.
- **an actual OS process kill**: `tests/test_checkpoint_crash.py::TestRealProcessCrash` spawns `python -m incsync.cli run` as a real subprocess, has it call `os._exit(137)` right after committing the last record's write and before saving the checkpoint, confirms the process died non-gracefully (`returncode == 137`), then re-runs the same CLI command and confirms the destination converges with no loss and no duplication.

A run's own `status` field is about **fetch** completeness (did we retrieve the whole snapshot); the checkpoint's own status is about **write** completeness. A run can legitimately report `status: "complete"` (fetch finished) while its checkpoint stays `in_progress` (a write is still blocked) — the two are deliberately different axes. The demo does not attempt full source/destination/schema identity binding on the checkpoint beyond the snapshot token match — see Assumptions above.

## Conflict policy (acceptance #4)

**One-way, source-authoritative.** Updates apply as last-writer-by-version (`incsync/mock_dest.py:apply_op`): an incoming version must be strictly greater than what the destination holds, or it is rejected as `StaleVersion` — this also means an older replay can never resurrect a newer tombstone. Deletes propagate as tombstones (`deleted=1`, payload cleared), not row removal, so their version still participates in the same ordering.

**Local destination edits** (`Destination.local_edit`, simulating an out-of-band edit made directly against the destination) are deliberately overridden by the next source-authoritative sync — but never silently: the override is logged to the `conflicts` table (`kind="local_edit_overridden"`) so the event is auditable even though the documented policy is "source always wins". Tested in `tests/test_conflicts.py`.

## Rate limits and errors (acceptance #5)

`incsync/engine.py:retry_fetch` retries `429` (honouring `Retry-After`, parsed as either seconds or an HTTP-date via `parse_retry_after`) and transient `5xx`, bounded to 5 attempts and 120 elapsed seconds. If the required delay would exceed the remaining budget, the engine gives up rather than sleeping past it ("defer rather than retry too early"). Malformed response bodies become a categorised, run-ending `MalformedResponse` failure — never silent success. Tests inject an in-process `VirtualClock` (`incsync/clock.py`) so no test ever performs a real `sleep`.

Authentication, permanent-validation and conflict errors (`StaleVersion`, `OpKeyReused`) are never retried by design — they are not raised from the fetch path at all.

## Accounting (acceptance #6)

Every run returns a `RunResult` (`incsync/engine.py`) where, by construction:

```
attempted == inserted + updated + deleted + unchanged + failed + unknown
```

`advertised` (page-1 total), `fetched`, `attempted` and `unattempted` are reported as separate fields, not folded together — `unattempted` is the source deficit (`advertised - fetched`) on a run that started from a fresh snapshot; it is not computed across a resumed run, since a resume necessarily re-fetches some already-seen records from the rewound checkpoint position (see Checkpointing). `unknown` is used exactly when a write's outcome could not be established even after attempting to confirm it by reading the destination back. An empty snapshot is a valid, `complete` run with an explicit `total: 0`, distinguished from a failed fetch. `tests/test_accounting.py::test_final_state_reconciles_against_independent_synthetic_ledger` builds its expected end-state independently of the engine and diffs it against `Destination.dump()` per-record, not just by totals.

## Self-check performed before delivery

- Fresh clone, fresh venv, `python3 -m unittest discover -s tests -t . -v` — all tests pass, no network access, no third-party packages installed.
- `gitleaks detect` over the working tree and full git history — no findings.
- `LICENSES.md` lists every dependency actually used (none beyond the Python standard library).
