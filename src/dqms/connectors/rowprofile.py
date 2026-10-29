"""Streaming type inference + aggregate profiling for row-oriented files (CSV, JSONL).

Rows are consumed one at a time and only aggregates are kept. Types are inferred
conservatively: a column is numeric only if *every* non-null value is, so dirty
data (an 'N/A' among integers) surfaces as a STRING column rather than being
silently coerced. Observed lengths are reported but never turned into declared ones.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from dqms.models import Column, ColumnProfile, Profile

NULL_TOKENS = {"", "null", "NULL", "\\N"}
MAX_TRACKED_DISTINCT = 200_000
MAX_ENUM = 20

_INT = re.compile(r"^[+-]?\d+$")
_FLOAT = re.compile(r"^[+-]?(\d+\.\d*|\.\d+|\d+)([eE][+-]?\d+)?$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?$")


@dataclass
class _ColumnState:
    name: str
    n: int = 0
    nulls: int = 0
    kinds: set[str] = field(default_factory=set)
    num_min: int | float | None = None
    num_max: int | float | None = None
    date_min: str | None = None
    date_max: str | None = None
    len_min: int | None = None
    len_max: int | None = None
    distinct: set[str] | None = field(default_factory=set)  # None once it overflowed


class RowProfiler:
    def __init__(self, names: list[str], parse_text: bool = True):
        """`parse_text`: values are strings that may encode numbers/bools (CSV). For JSON,
        typed values are trusted and only date-like strings are interpreted."""
        self._cols = [_ColumnState(n) for n in names]
        self._parse_text = parse_text
        self.rows = 0

    def add(self, values: list[Any]) -> None:
        self.rows += 1
        for state, raw in zip(self._cols, values):
            state.n += 1
            if raw is None or (isinstance(raw, str) and raw in NULL_TOKENS):
                state.nulls += 1
                continue
            kind, normalized = _classify(raw, self._parse_text)
            state.kinds.add(kind)
            text = raw if isinstance(raw, str) else str(raw)
            if state.distinct is not None:
                state.distinct.add(text)
                if len(state.distinct) > MAX_TRACKED_DISTINCT:
                    state.distinct = None
            if kind in {"int", "float"}:
                state.num_min = normalized if state.num_min is None else min(state.num_min, normalized)
                state.num_max = normalized if state.num_max is None else max(state.num_max, normalized)
            elif kind in {"date", "timestamp"}:
                state.date_min = text if state.date_min is None else min(state.date_min, text)
                state.date_max = text if state.date_max is None else max(state.date_max, text)
            if kind == "str" or kind in {"date", "timestamp", "bool", "int", "float"}:
                length = len(text)
                state.len_min = length if state.len_min is None else min(state.len_min, length)
                state.len_max = length if state.len_max is None else max(state.len_max, length)

    def finish(self, sampled: bool, include_values: bool = False, total_rows: int | None = None) -> tuple[list[Column], Profile]:
        columns: list[Column] = []
        profile = Profile(row_count=self.rows, total_rows=total_rows, sampled=sampled)
        for s in self._cols:
            datatype = _resolve_type(s.kinds)
            columns.append(Column(name=s.name, datatype=datatype))  # nullability is unknowable from a file
            non_null = s.n - s.nulls
            stats = ColumnProfile(
                column=s.name,
                null_rate=(s.nulls / s.n) if s.n else None,
                distinct_count=len(s.distinct) if s.distinct is not None else None,
                distinct_rate=(len(s.distinct) / non_null) if s.distinct is not None and non_null else None,
            )
            if datatype in {"INTEGER", "FLOAT"}:
                stats.min_value, stats.max_value = _tidy(s.num_min), _tidy(s.num_max)
            elif datatype in {"DATE", "TIMESTAMP"}:
                stats.min_value, stats.max_value = s.date_min, s.date_max
            elif datatype == "STRING":
                stats.min_length, stats.max_length = s.len_min, s.len_max
                if include_values and not sampled and s.distinct is not None and 0 < len(s.distinct) <= MAX_ENUM:
                    stats.top_values = sorted(s.distinct)
            profile.columns.append(stats)
        return columns, profile


def _classify(raw: Any, parse_text: bool) -> tuple[str, int | float | None]:
    if isinstance(raw, bool):
        return "bool", None
    if isinstance(raw, int):
        return "int", raw
    if isinstance(raw, float):
        return "float", raw
    if not isinstance(raw, str):
        return "str", None  # nested JSON values etc.
    text = raw.strip()
    if parse_text:
        if text.lower() in {"true", "false"}:
            return "bool", None
        if _INT.match(text) and not (len(text.lstrip("+-")) > 1 and text.lstrip("+-").startswith("0")):
            return "int", int(text)
        if _FLOAT.match(text) and not (len(text) > 1 and text.startswith("0") and text[1].isdigit()):
            return "float", float(text)
    if _DATE.match(text):
        try:
            date.fromisoformat(text)
            return "date", None
        except ValueError:
            return "str", None
    if _TIMESTAMP.match(text):
        try:
            datetime.fromisoformat(text.replace(" ", "T"))
            return "timestamp", None
        except ValueError:
            return "str", None
    return "str", None


def _resolve_type(kinds: set[str]) -> str:
    if not kinds:
        return "STRING"
    if kinds == {"bool"}:
        return "BOOLEAN"
    if kinds == {"int"}:
        return "INTEGER"
    if kinds <= {"int", "float"}:
        return "FLOAT"
    if kinds == {"date"}:
        return "DATE"
    if kinds <= {"date", "timestamp"}:
        return "TIMESTAMP"
    return "STRING"


def _tidy(value: int | float | None) -> int | float | None:
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value
