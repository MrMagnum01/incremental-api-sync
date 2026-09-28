"""Canonical digest and idempotency-key helpers, shared by engine and tests so the
synthetic reconciliation ledger can be computed independently of the engine's own math."""
from __future__ import annotations

import hashlib
import json

TOMBSTONE = {"__deleted__": True}


def canonical_json(payload: dict) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def payload_digest(payload: dict) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def op_key(namespace: str, id: str, version: int, op: str) -> str:
    """Idempotency key: namespace + record id + source version + operation.
    Version-bound so a later legitimate update is never swallowed as a duplicate
    of an earlier one, and a replay of the same version+op is always recognised."""
    return f"{namespace}:{id}:{version}:{op}"
