"""Mock source REST API. In-process only, clearly synthetic — not a real vendor.

Contract (binds page 1): a fetch with cursor=None freezes a snapshot token over the
current record set, sorted deterministically by (updated_at, id). Every subsequent
page for that token must reuse the same token, the same total and page_size, and
walk the frozen order without gaps, duplicates or going backwards. Mutations made
to the dataset after the freeze are invisible to that snapshot.
"""
from __future__ import annotations

import copy
import json
import os
import uuid
from dataclasses import dataclass
from typing import Optional

from .errors import (
    ContractViolation,
    MalformedResponse,
    RateLimited,
    SnapshotExpired,
    TransientServerError,
)


@dataclass
class SourceRecord:
    namespace: str
    id: str
    version: int
    updated_at: float
    deleted: bool
    payload: dict


@dataclass
class Page:
    snapshot_token: str
    total: int
    page_size: int
    records: list
    next_cursor: Optional[tuple]
    has_more: bool


class SourceDataset:
    """The mutable "true" state of the source. Tests mutate it to simulate changed
    and deleted records, including changes made mid-run (after a snapshot freeze)."""

    def __init__(self):
        self._records: dict[tuple, SourceRecord] = {}
        self._clock = 0.0

    def _tick(self) -> float:
        self._clock += 1.0
        return self._clock

    def seed(self, namespace: str, id: str, payload: dict) -> SourceRecord:
        rec = SourceRecord(namespace, id, version=1, updated_at=self._tick(),
                            deleted=False, payload=dict(payload))
        self._records[(namespace, id)] = rec
        return rec

    def update(self, namespace: str, id: str, payload: dict) -> SourceRecord:
        key = (namespace, id)
        prev = self._records[key]
        rec = SourceRecord(namespace, id, version=prev.version + 1, updated_at=self._tick(),
                            deleted=False, payload=dict(payload))
        self._records[key] = rec
        return rec

    def delete(self, namespace: str, id: str) -> SourceRecord:
        key = (namespace, id)
        prev = self._records[key]
        rec = SourceRecord(namespace, id, version=prev.version + 1, updated_at=self._tick(),
                            deleted=True, payload={})
        self._records[key] = rec
        return rec

    def snapshot_rows(self) -> list:
        rows = list(self._records.values())
        rows.sort(key=lambda r: (r.updated_at, r.id))
        return copy.deepcopy(rows)


class MockSourceAPI:
    """`snapshot_store_path`, if given, persists frozen snapshots to a JSON file so a
    fresh process (e.g. after a real crash-and-restart test) can resume a run that
    references a token it never itself created. This mirrors a real API's server-side
    pagination cursor durability; the mutable dataset itself is not persisted here."""

    def __init__(self, dataset: SourceDataset, page_size: int = 10, snapshot_store_path: Optional[str] = None):
        self.dataset = dataset
        self.page_size = page_size
        self.snapshot_store_path = snapshot_store_path
        self._snapshots: dict[str, list] = {}
        self._fault_queue: dict[int, list] = {}  # page_number(1-based) -> [fault, ...]
        self._pages_served: dict[str, int] = {}
        if snapshot_store_path and os.path.exists(snapshot_store_path):
            with open(snapshot_store_path) as f:
                raw = json.load(f)
            for token, rows in raw.items():
                self._snapshots[token] = [
                    SourceRecord(r["namespace"], r["id"], r["version"], r["updated_at"], r["deleted"], r["payload"])
                    for r in rows
                ]

    def _persist_snapshots(self) -> None:
        if not self.snapshot_store_path:
            return
        raw = {
            token: [
                {"namespace": r.namespace, "id": r.id, "version": r.version,
                 "updated_at": r.updated_at, "deleted": r.deleted, "payload": r.payload}
                for r in rows
            ]
            for token, rows in self._snapshots.items()
        }
        with open(self.snapshot_store_path, "w") as f:
            json.dump(raw, f)

    def queue_fault(self, page_number: int, fault: dict) -> None:
        """fault: {"type": "rate_limit"|"server_error"|"malformed"|"invalidate"|
        "non_advancing"|"truncate"|"duplicate_version"|"drift_total", ...extra}"""
        self._fault_queue.setdefault(page_number, []).append(fault)

    def _pop_fault(self, page_number: int) -> Optional[dict]:
        q = self._fault_queue.get(page_number)
        if q:
            return q.pop(0)
        return None

    def fetch_page(self, snapshot_token: Optional[str], cursor: Optional[tuple]) -> Page:
        page_number = self._pages_served.get(snapshot_token or "__new__", 0) + 1

        fault = self._pop_fault(page_number)
        if fault:
            ftype = fault["type"]
            if ftype == "rate_limit":
                raise RateLimited(fault.get("retry_after", 1))
            if ftype == "server_error":
                raise TransientServerError(fault.get("status", 503))
            if ftype == "invalidate":
                if snapshot_token in self._snapshots:
                    del self._snapshots[snapshot_token]
                raise SnapshotExpired()

        if snapshot_token is None:
            token = str(uuid.uuid4())
            rows = self.dataset.snapshot_rows()
            self._snapshots[token] = rows
            self._pages_served[token] = 0
            self._persist_snapshots()
        else:
            token = snapshot_token
            if token not in self._snapshots:
                raise SnapshotExpired()
            rows = self._snapshots[token]

        total = len(rows)
        page_size = self.page_size

        if cursor is None:
            start_idx = 0
        else:
            cur_updated, cur_id = cursor
            start_idx = 0
            for i, r in enumerate(rows):
                if (r.updated_at, r.id) > (cur_updated, cur_id):
                    start_idx = i
                    break
            else:
                start_idx = len(rows)

        slice_ = rows[start_idx:start_idx + page_size]
        force_no_more = False

        if fault and fault["type"] == "truncate":
            # Simulate a server bug: drop the last record of the page AND falsely
            # report the snapshot as fully delivered, so the record is permanently
            # lost unless the client reconciles fetched-count against total.
            slice_ = slice_[: max(0, len(slice_) - 1)]
            force_no_more = True
        if fault and fault["type"] == "duplicate_version" and slice_:
            slice_ = [slice_[0]] + slice_  # inject a duplicate identity/version key
        if fault and fault["type"] == "non_advancing":
            next_cursor_override = cursor  # pretend nothing advanced
        else:
            next_cursor_override = None

        has_more = (not force_no_more) and (start_idx + len(slice_)) < total
        next_cursor = (slice_[-1].updated_at, slice_[-1].id) if (has_more and slice_) else None
        if next_cursor_override is not None:
            next_cursor = next_cursor_override
            has_more = True  # non-advancing cursor still claims more pages -> loop guard trips

        reported_total = total
        reported_page_size = page_size
        if fault and fault["type"] == "drift_total":
            reported_total = total + 1

        self._pages_served[token] = self._pages_served.get(token, 0) + 1

        if fault and fault["type"] == "malformed":
            raise MalformedResponse(fault.get("reason", "unparseable body"))

        return Page(
            snapshot_token=token,
            total=reported_total,
            page_size=reported_page_size,
            records=slice_,
            next_cursor=next_cursor,
            has_more=has_more,
        )
