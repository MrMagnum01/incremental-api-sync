"""Deterministic synthetic dataset builder, shared by the CLI and the crash-recovery
test so a freshly-started process can reconstruct the same source-of-truth."""
from __future__ import annotations

from .mock_source import SourceDataset

NAMESPACE = "widgets"


def build_dataset(n: int = 25) -> SourceDataset:
    ds = SourceDataset()
    for i in range(n):
        ds.seed(NAMESPACE, f"w{i:03d}", {"name": f"Widget {i}", "price_cents": 100 + i})
    return ds
