"""Incremental processing identities and interfaces; no IO/publication engine."""

import json
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from typing import Protocol

from .contracts import utc


@dataclass(frozen=True)
class ProcessingManifest:
    partition_id: str
    source_checksums: tuple[str, ...]
    configuration_sha256: str
    method_version: str
    schema_version: str
    core_start: datetime
    core_end: datetime
    halo_start: datetime
    halo_end: datetime
    carried_state_sha256: str | None
    producer_git_sha: str

    def __post_init__(self) -> None:
        bounds = tuple(
            utc(t) for t in (self.halo_start, self.core_start, self.core_end, self.halo_end)
        )
        if not bounds[0] <= bounds[1] < bounds[2] <= bounds[3]:
            raise ValueError("halo must contain positive core window")
        for name in ("core_start", "core_end", "halo_start", "halo_end"):
            object.__setattr__(self, name, utc(getattr(self, name)))
        hashes = (*self.source_checksums, self.configuration_sha256)
        if not self.source_checksums or any(
            len(h) != 64 or any(c not in "0123456789abcdef" for c in h) for h in hashes
        ):
            raise ValueError("source/configuration SHA-256 required")
        if self.carried_state_sha256 is not None and (
            len(self.carried_state_sha256) != 64
            or any(c not in "0123456789abcdef" for c in self.carried_state_sha256)
        ):
            raise ValueError("invalid carried-state SHA-256")
        if not self.partition_id or not self.method_version or not self.schema_version:
            raise ValueError("manifest identity and versions required")
        if len(self.producer_git_sha) != 40 or any(
            c not in "0123456789abcdef" for c in self.producer_git_sha
        ):
            raise ValueError("producer Git SHA required")

    @property
    def idempotency_key(self) -> str:
        payload = {
            "partition": self.partition_id,
            "sources": sorted(self.source_checksums),
            "config": self.configuration_sha256,
            "method": self.method_version,
            "schema": self.schema_version,
            "producer_git_sha": self.producer_git_sha,
            "state": self.carried_state_sha256,
            "bounds": [
                utc(t).isoformat()
                for t in (self.core_start, self.core_end, self.halo_start, self.halo_end)
            ],
        }
        return sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


class CompletionLedger(Protocol):
    """Future atomic commit: failed partitions never count as complete.

    Halo samples support boundaries; emit only core-owned contributions, retaining
    per-vessel invalid/conflict barriers and state fingerprints. Reuse requires matching
    idempotency key and verified output checksums. No dense vessel-cell-time grid.
    """

    def verified_outputs(self, idempotency_key: str) -> tuple[str, ...] | None: ...
    def commit(self, manifest: ProcessingManifest, output_checksums: tuple[str, ...]) -> None: ...
