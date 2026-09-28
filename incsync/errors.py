"""Categorised error taxonomy. Every failure mode the engine can hit is named here
so accounting can bucket outcomes instead of collapsing everything to "failed"."""
from __future__ import annotations


class SourceAPIError(Exception):
    """Base for errors raised by the mock source API."""


class RateLimited(SourceAPIError):
    def __init__(self, retry_after):
        self.retry_after = retry_after  # seconds (float) or a raw HTTP-date string
        super().__init__(f"429 rate limited, retry_after={retry_after!r}")


class TransientServerError(SourceAPIError):
    def __init__(self, status: int):
        self.status = status
        super().__init__(f"transient {status}")


class SnapshotExpired(SourceAPIError):
    """The frozen page-1 snapshot token is no longer valid at the source."""


class ContractViolation(SourceAPIError):
    """Page response breaks the pagination contract: non-advancing cursor, drifted
    total/page_size, duplicate/missing version keys, truncated/extra pages, bad order."""


class MalformedResponse(SourceAPIError):
    """Response body cannot be parsed into the record schema."""


class RetryBudgetExhausted(SourceAPIError):
    """Retries exhausted (5 attempts or 120s elapsed) without a usable response."""


class PermanentAPIError(SourceAPIError):
    """Auth / permanent validation / 4xx-not-429 — never retried."""


class DestinationWriteError(Exception):
    """Base for mock-destination write failures."""


class AckLost(DestinationWriteError):
    """Write was durably applied but the caller never received the acknowledgement.
    The destination can still be read back by operation key to confirm the outcome."""


class DestinationUnavailable(DestinationWriteError):
    """Neither the write nor a confirming read could be completed: true UNKNOWN."""


class OpKeyReused(DestinationWriteError):
    """Same idempotency key presented with a different payload digest: refused."""


class StaleVersion(DestinationWriteError):
    """Incoming version is not newer than what the destination already holds."""


class LockHeld(Exception):
    """Another sync runner already holds the single-runner lock."""


class CheckpointCorrupt(Exception):
    """Checkpoint state failed integrity validation; fail closed, do not restart blind."""


class CheckpointIdentityMismatch(Exception):
    """Checkpoint is bound to a different source/destination/snapshot/schema identity."""


class SimulatedCrash(Exception):
    """Raised by the in-process crash hook to emulate a hard process kill at a
    specific stage, without actually terminating the interpreter (fast unit tests)."""

    def __init__(self, stage: str):
        self.stage = stage
        super().__init__(f"simulated crash at {stage}")
