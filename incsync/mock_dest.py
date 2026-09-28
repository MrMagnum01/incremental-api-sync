"""Mock destination system: a small durable store (SQLite file) with an idempotency
ledger, a conflict log and a checkpoint table bound to source/destination identity.

Every write that reaches `records` goes through `applied_ops` in the same commit,
so a crash can never separate "the write happened" from "we can prove it happened".
"""
from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Optional

from .errors import AckLost, DestinationUnavailable, OpKeyReused, StaleVersion

SCHEMA = """
CREATE TABLE IF NOT EXISTS records (
    namespace TEXT NOT NULL,
    id TEXT NOT NULL,
    version INTEGER NOT NULL,
    digest TEXT NOT NULL,
    deleted INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    local_edit INTEGER NOT NULL DEFAULT 0,
    updated_at REAL NOT NULL,
    PRIMARY KEY (namespace, id)
);
CREATE TABLE IF NOT EXISTS applied_ops (
    op_key TEXT PRIMARY KEY,
    namespace TEXT NOT NULL,
    id TEXT NOT NULL,
    version INTEGER NOT NULL,
    op TEXT NOT NULL,
    digest TEXT NOT NULL,
    outcome TEXT NOT NULL,
    applied_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS conflicts (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    namespace TEXT NOT NULL,
    record_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    detail TEXT NOT NULL,
    at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS checkpoint (
    sync_id TEXT PRIMARY KEY,
    snapshot_token TEXT,
    cursor_updated_at REAL,
    cursor_id TEXT,
    schema_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    source_id TEXT,
    dest_id TEXT,
    advertised_total INTEGER,
    page_size INTEGER,
    fetched_count INTEGER,
    fetch_cursor_updated_at REAL,
    fetch_cursor_id TEXT,
    updated_at REAL NOT NULL
);
"""

CHECKPOINT_STATUSES = ("in_progress", "complete")
CHECKPOINT_SCHEMA_VERSION = 2


class Destination:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.executescript(SCHEMA)
        self._unreadable_confirmations: set = set()

    @property
    def identity(self) -> str:
        """Stable identity of this destination instance, bound into the checkpoint
        so a swap to a different destination file under the same sync_id is caught
        instead of silently trusted."""
        return self.db_path

    def close(self):
        self.conn.close()

    # -- records ---------------------------------------------------------
    def get_record(self, namespace: str, id: str):
        cur = self.conn.execute(
            "SELECT namespace,id,version,digest,deleted,payload_json,local_edit "
            "FROM records WHERE namespace=? AND id=?", (namespace, id))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "namespace": row[0], "id": row[1], "version": row[2], "digest": row[3],
            "deleted": bool(row[4]), "payload": json.loads(row[5]), "local_edit": bool(row[6]),
        }

    def local_edit(self, namespace: str, id: str, payload: dict, digest: str) -> None:
        """Simulate an out-of-band edit made directly against the destination,
        bypassing the sync engine entirely (used to test the conflict policy)."""
        now = time.time()
        self.conn.execute(
            "INSERT INTO records(namespace,id,version,digest,deleted,payload_json,local_edit,updated_at) "
            "VALUES(?,?,COALESCE((SELECT version FROM records WHERE namespace=? AND id=?),0),?,0,?,1,?) "
            "ON CONFLICT(namespace,id) DO UPDATE SET digest=excluded.digest, payload_json=excluded.payload_json, "
            "local_edit=1, updated_at=excluded.updated_at",
            (namespace, id, namespace, id, digest, json.dumps(payload), now),
        )

    def get_op(self, op_key: str):
        if op_key in self._unreadable_confirmations:
            # One-shot: models a transient outage at confirmation time, not a
            # permanently unreadable key -- a later retry must be able to read it.
            self._unreadable_confirmations.discard(op_key)
            raise DestinationUnavailable(f"confirmation read failed for {op_key}")
        cur = self.conn.execute(
            "SELECT op_key,namespace,id,version,op,digest,outcome FROM applied_ops WHERE op_key=?",
            (op_key,))
        row = cur.fetchone()
        if not row:
            return None
        return {"op_key": row[0], "namespace": row[1], "id": row[2], "version": row[3],
                "op": row[4], "digest": row[5], "outcome": row[6]}

    def apply_op(self, op_key: str, namespace: str, id: str, version: int, op: str,
                 digest: str, payload: dict, ack_mode: str = "ok") -> str:
        """Apply one write inside a single commit. Returns the outcome string.
        ack_mode:
          "ok"                    - normal: commit, return outcome.
          "lost_ack_confirmable"  - commit succeeds, but raises AckLost as if the
                                     response never reached the caller. A follow-up
                                     get_op() will find it.
          "lost_ack_unconfirmable"- raises DestinationUnavailable *before* committing
                                     anything and before any read can confirm it either
                                     -> the caller must record UNKNOWN.
          "committed_then_confirmation_unreadable" - commit succeeds (the write is
                                     durable, unlike "lost_ack_unconfirmable" above),
                                     but the acknowledgement is lost AND the follow-up
                                     get_op() read for this exact key also fails ->
                                     the caller must record UNKNOWN even though the
                                     write already happened; a later replay under the
                                     same key must still see it as already-applied.
        Raises OpKeyReused / StaleVersion for conflicts; caller records those as failed.
        """
        if ack_mode == "lost_ack_unconfirmable":
            raise DestinationUnavailable("destination unreachable for write or confirmation")

        existing_op = self.get_op(op_key)
        if existing_op is not None:
            if existing_op["digest"] != digest:
                raise OpKeyReused(f"{op_key} reused with different payload digest")
            return "unchanged"  # true idempotent replay: already applied, no duplicate write

        current = self.get_record(namespace, id)
        if current is not None and version <= current["version"]:
            raise StaleVersion(
                f"{namespace}/{id} incoming version {version} <= stored {current['version']}")

        was_local_edit = bool(current and current["local_edit"])

        if op == "delete":
            outcome = "deleted"
        elif current is None:
            outcome = "inserted"
        else:
            outcome = "updated"

        now = time.time()
        self.conn.execute("BEGIN")
        try:
            self.conn.execute(
                "INSERT INTO records(namespace,id,version,digest,deleted,payload_json,local_edit,updated_at) "
                "VALUES(?,?,?,?,?,?,0,?) "
                "ON CONFLICT(namespace,id) DO UPDATE SET version=excluded.version, digest=excluded.digest, "
                "deleted=excluded.deleted, payload_json=excluded.payload_json, local_edit=0, updated_at=excluded.updated_at",
                (namespace, id, version, digest, 1 if op == "delete" else 0, json.dumps(payload), now),
            )
            self.conn.execute(
                "INSERT INTO applied_ops(op_key,namespace,id,version,op,digest,outcome,applied_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (op_key, namespace, id, version, op, digest, outcome, now),
            )
            if was_local_edit:
                self.conn.execute(
                    "INSERT INTO conflicts(namespace,record_id,kind,detail,at) VALUES(?,?,?,?,?)",
                    (namespace, id, "local_edit_overridden",
                     "source-authoritative sync replaced a local destination edit", now),
                )
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

        if ack_mode == "lost_ack_confirmable":
            raise AckLost(op_key)

        if ack_mode == "committed_then_confirmation_unreadable":
            self._unreadable_confirmations.add(op_key)
            raise DestinationUnavailable("write committed but confirmation read failed")

        return outcome

    def conflicts_for(self, namespace: str, id: str):
        cur = self.conn.execute(
            "SELECT kind,detail,at FROM conflicts WHERE namespace=? AND record_id=? ORDER BY seq",
            (namespace, id))
        return [{"kind": r[0], "detail": r[1], "at": r[2]} for r in cur.fetchall()]

    def dump(self):
        cur = self.conn.execute("SELECT namespace,id,version,digest,deleted,payload_json FROM records ORDER BY namespace,id")
        return [
            {"namespace": r[0], "id": r[1], "version": r[2], "digest": r[3],
             "deleted": bool(r[4]), "payload": json.loads(r[5])}
            for r in cur.fetchall()
        ]

    # -- checkpoint --------------------------------------------------------
    def load_checkpoint(self, sync_id: str):
        cur = self.conn.execute(
            "SELECT sync_id,snapshot_token,cursor_updated_at,cursor_id,schema_version,status,"
            "source_id,dest_id,advertised_total,page_size,fetched_count,"
            "fetch_cursor_updated_at,fetch_cursor_id "
            "FROM checkpoint WHERE sync_id=?", (sync_id,))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "sync_id": row[0], "snapshot_token": row[1],
            "cursor": (row[2], row[3]) if row[2] is not None else None,
            "schema_version": row[4], "status": row[5],
            "source_id": row[6], "dest_id": row[7],
            "advertised_total": row[8], "page_size": row[9], "fetched_count": row[10],
            "fetch_cursor": (row[11], row[12]) if row[11] is not None else None,
        }

    def save_checkpoint(self, sync_id: str, snapshot_token: str, cursor, status: str,
                         source_id: Optional[str] = None, dest_id: Optional[str] = None,
                         advertised_total: Optional[int] = None, page_size: Optional[int] = None,
                         fetched_count: Optional[int] = None, fetch_cursor=None) -> None:
        now = time.time()
        cur_updated, cur_id = cursor if cursor else (None, None)
        fetch_cur_updated, fetch_cur_id = fetch_cursor if fetch_cursor else (None, None)
        self.conn.execute(
            "INSERT INTO checkpoint(sync_id,snapshot_token,cursor_updated_at,cursor_id,schema_version,"
            "status,source_id,dest_id,advertised_total,page_size,fetched_count,"
            "fetch_cursor_updated_at,fetch_cursor_id,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(sync_id) DO UPDATE SET snapshot_token=excluded.snapshot_token, "
            "cursor_updated_at=excluded.cursor_updated_at, cursor_id=excluded.cursor_id, "
            "schema_version=excluded.schema_version, status=excluded.status, "
            "source_id=excluded.source_id, dest_id=excluded.dest_id, "
            "advertised_total=excluded.advertised_total, page_size=excluded.page_size, "
            "fetched_count=excluded.fetched_count, "
            "fetch_cursor_updated_at=excluded.fetch_cursor_updated_at, "
            "fetch_cursor_id=excluded.fetch_cursor_id, updated_at=excluded.updated_at",
            (sync_id, snapshot_token, cur_updated, cur_id, CHECKPOINT_SCHEMA_VERSION, status,
             source_id, dest_id, advertised_total, page_size, fetched_count,
             fetch_cur_updated, fetch_cur_id, now),
        )
