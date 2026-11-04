"""Bounded raw-lineage collection and the worker's Shapley result envelope."""

import json
from math import isfinite
from typing import Any

from app.lineage import explain

MAX_ROWS = 100_000
MAX_PLAYERS = 64
MAX_SAMPLING_WORK = 50_000_000
LINEAGE_FIELDS = ("individual_id", "island_id", "fitness", "parent_ids", "operator", "origin_island", "origin_individual_id")


def compact_row(raw: dict[str, Any]) -> dict[str, Any]:
    """Keep ancestry only; never retain genomes or arbitrary runner fields."""
    row = {key: raw[key] for key in LINEAGE_FIELDS if key in raw}
    identity = row["individual_id"]
    if not isinstance(identity, str) or not identity.strip():
        raise ValueError("Each evaluation needs a nonempty individual ID.")
    row["individual_id"] = identity.strip()
    island = row["island_id"]
    if isinstance(island, bool) or not isinstance(island, (int, str)):
        raise TypeError("Each evaluation needs a nonnegative island ID.")
    row["island_id"] = int(island)
    row["fitness"] = float(row["fitness"])
    if row["island_id"] < 0 or not isfinite(row["fitness"]):
        raise ValueError("Each evaluation needs a nonnegative island and finite fitness.")
    parents = row.get("parent_ids")
    if isinstance(parents, str):
        parents = [parent.strip() for parent in parents.split(";") if parent.strip()]
    elif parents is None:
        parents = []
    if not isinstance(parents, list) or any(not isinstance(parent, str) or not parent.strip() for parent in parents):
        raise ValueError("Parent IDs must be a list of nonempty individual IDs.")
    row["parent_ids"] = [parent.strip() for parent in parents]
    origin = row.get("origin_individual_id")
    if origin is not None and not isinstance(origin, str):
        raise ValueError("Origin individual ID must be a string.")
    if row.get("operator") is not None and not isinstance(row["operator"], str):
        raise TypeError("Operator must be a string.")
    origin_island = row.get("origin_island")
    if origin_island not in (None, ""):
        if isinstance(origin_island, bool) or not isinstance(origin_island, (int, str)):
            raise TypeError("Origin island ID must be an integer.")
        row["origin_island"] = int(origin_island)
    return row


class ShapleyAccumulator:
    """Accumulate equally during streaming, recovery and batch processing."""

    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        self.rows: list[dict[str, Any]] = []
        self.islands: dict[int, None] = {}
        self.detail: str | None = None

    def unavailable(self, detail: str) -> None:
        self.detail = detail
        self.rows.clear()
        self.islands.clear()

    def add(self, rows: list[dict[str, Any]]) -> None:
        if self.detail is not None:
            return
        if len(self.rows) + len(rows) > MAX_ROWS:
            self.unavailable(f"Shapley analysis supports at most {MAX_ROWS:,} evaluation records.")
            return
        try:
            for raw in rows:
                row = compact_row(raw)
                self.islands[row["island_id"]] = None
                if len(self.islands) > MAX_PLAYERS:
                    self.unavailable(f"Shapley analysis supports at most {MAX_PLAYERS} islands.")
                    return
                self.rows.append(row)
        except (KeyError, TypeError, ValueError, OverflowError) as error:
            self.unavailable(f"Unusable evaluation lineage: {error}")
        except MemoryError:
            self.unavailable("Insufficient memory for Shapley analysis.")

    def finish(self, metadata: Any) -> dict[str, Any]:
        envelope: dict[str, Any] = {"schema_version": 1, "run_id": self.run_id, "status": "unavailable"}
        if self.detail is not None:
            return dict(envelope, detail=self.detail)
        try:
            explanation = explain(self.rows, maximising=getattr(metadata, "maximising", None), run_id=self.run_id, work_limit=MAX_SAMPLING_WORK)
            # Finite input fitness can still overflow derived percentages or sampling statistics.
            json.dumps(explanation, allow_nan=False)
        except (ValueError, OverflowError) as error:
            return dict(envelope, detail=f"Shapley analysis unavailable: {error}")
        except MemoryError:
            return dict(envelope, detail="Insufficient memory for Shapley analysis.")
        explanation["source"] = {
            "kind": "backend-lineage",
            "algorithm": getattr(metadata, "algorithm", None),
            "benchmark": getattr(metadata, "benchmark", None),
            "topology": getattr(metadata, "migration_topology", None),
        }
        return dict(envelope, status="ready", explanation=explanation)
