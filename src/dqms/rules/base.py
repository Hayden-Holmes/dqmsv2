"""Shared plumbing for rule generators."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from dqms.models import Column, Metadata, Profile, Rule, RuleType, Tier

NUMERIC_TYPES = {
    "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "INT64", "NUMERIC", "DECIMAL", "BIGNUMERIC",
    "FLOAT", "FLOAT64", "DOUBLE", "REAL", "NUMBER", "MONEY", "LONG", "SHORT", "BYTE",
}
STRING_TYPES = {"CHAR", "VARCHAR", "NVARCHAR", "NCHAR", "CHARACTER", "STRING", "TEXT"}
DATE_TYPES = {"DATE"}
TIMESTAMP_TYPES = {"TIMESTAMP", "DATETIME", "TIMESTAMPTZ", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"}


@dataclass
class Context:
    metadata: Metadata
    profile: Profile | None = None
    min_profile_rows: int = 100  # below this, row-level profile claims are too weak to trust


class RuleGenerator(Protocol):
    name: str

    def generate(self, ctx: Context) -> list[Rule]: ...


def type_family(column: Column) -> str:
    """One of: numeric, string, date, timestamp, boolean, unknown."""
    base = column.base_type
    if base in NUMERIC_TYPES:
        return "numeric"
    if base in STRING_TYPES:
        return "string"
    if base in DATE_TYPES:
        return "date"
    if base in TIMESTAMP_TYPES:
        return "timestamp"
    if base in {"BOOL", "BOOLEAN"}:
        return "boolean"
    return "unknown"


def is_temporal(column: Column) -> bool:
    return type_family(column) in {"date", "timestamp"}


def rule(
    type_: RuleType,
    columns: list[str] | str,
    tier: Tier,
    confidence: float,
    rationale: str,
    evidence: list[str] | None = None,
    **params: Any,
) -> Rule:
    return Rule(
        type=type_,
        columns=[columns] if isinstance(columns, str) else columns,
        params=params,
        tier=tier,
        confidence=confidence,
        rationale=rationale,
        evidence=evidence or [],
    )
