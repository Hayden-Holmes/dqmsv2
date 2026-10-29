"""Normalize arbitrary profiling output into the canonical `Profile`.

Equivalent metric names are mapped through an alias table. Metrics with no
canonical equivalent are preserved in `ColumnProfile.extra`; nothing is dropped
and nothing is invented.
"""

from __future__ import annotations

from typing import Any

from dqms.models import ColumnProfile, Profile

_NAME_KEYS = ("column", "name", "column_name", "field")

# alias -> (canonical field, transform)
_FRACTION = "fraction"  # already 0..1
_PERCENT = "percent"  # 0..100 -> /100
_AUTO = "auto"  # <=1 is a fraction, otherwise a percent
_RAW = "raw"

_ALIASES: dict[str, tuple[str, str]] = {
    "null_rate": ("null_rate", _FRACTION),
    "null_ratio": ("null_rate", _FRACTION),
    "null_fraction": ("null_rate", _FRACTION),
    "null_percentage": ("null_rate", _PERCENT),
    "null_percent": ("null_rate", _PERCENT),
    "null_pct": ("null_rate", _PERCENT),
    "completeness": ("_completeness", _AUTO),
    "completeness_percent": ("_completeness", _PERCENT),
    "distinct_rate": ("distinct_rate", _FRACTION),
    "uniqueness": ("distinct_rate", _AUTO),
    "unique_percentage": ("distinct_rate", _PERCENT),
    "unique_percent": ("distinct_rate", _PERCENT),
    "distinct_count": ("distinct_count", _RAW),
    "unique_count": ("distinct_count", _RAW),
    "approx_distinct": ("distinct_count", _RAW),
    "min": ("min_value", _RAW),
    "min_value": ("min_value", _RAW),
    "minimum": ("min_value", _RAW),
    "max": ("max_value", _RAW),
    "max_value": ("max_value", _RAW),
    "maximum": ("max_value", _RAW),
    "min_length": ("min_length", _RAW),
    "max_length": ("max_length", _RAW),
    "top_values": ("top_values", _RAW),
    "distinct_values": ("top_values", _RAW),
    "allowed_values": ("top_values", _RAW),
    "pattern": ("pattern", _RAW),
    "regex": ("pattern", _RAW),
}
_ROW_COUNT_KEYS = ("row_count", "rows", "total_rows", "count")
_PROFILE_HINTS = set(_ALIASES) | {"null_count"}


def looks_like_profile(doc: Any) -> bool:
    if not isinstance(doc, dict):
        return False
    entries = doc.get("column_profiles") or doc.get("columns")
    if not isinstance(entries, list) or not entries:
        return False
    return any(isinstance(e, dict) and _PROFILE_HINTS & {k.lower() for k in e} for e in entries) or (
        "column_profiles" in doc
    )


def normalize_profile(doc: dict[str, Any]) -> Profile:
    row_count = next((int(doc[k]) for k in _ROW_COUNT_KEYS if isinstance(doc.get(k), (int, float))), None)
    entries = doc.get("column_profiles") or doc.get("columns") or []
    columns = [_normalize_column(e, row_count) for e in entries if isinstance(e, dict)]
    table = next((doc[k] for k in ("table", "table_name", "dataset") if isinstance(doc.get(k), str) and doc[k]), None)
    return Profile(table=table, row_count=row_count, columns=[c for c in columns if c])


def _normalize_column(entry: dict[str, Any], row_count: int | None) -> ColumnProfile | None:
    name = next((entry[k] for k in _NAME_KEYS if isinstance(entry.get(k), str)), None)
    if not name:
        return None
    # Accept both flat metrics and a nested {"metrics": {...}} block.
    metrics = {**{k: v for k, v in entry.items() if k not in _NAME_KEYS}, **(entry.get("metrics") or {})}
    metrics.pop("metrics", None)

    profile = ColumnProfile(column=name)
    completeness: float | None = None
    for raw_key, value in metrics.items():
        key = raw_key.lower()
        if key == "null_count":
            if row_count and isinstance(value, (int, float)):
                profile.null_rate = value / row_count
            else:
                profile.extra[raw_key] = value
            continue
        alias = _ALIASES.get(key)
        if alias is None or value is None:
            profile.extra[raw_key] = value
            continue
        field, mode = alias
        if mode != _RAW:
            value = _to_fraction(value, mode)
            if value is None:
                profile.extra[raw_key] = metrics[raw_key]
                continue
        if field == "_completeness":
            completeness = value
        elif field == "top_values":
            profile.top_values = [str(v) for v in value] if isinstance(value, list) else None
        else:
            setattr(profile, field, value)
    if profile.null_rate is None and completeness is not None:
        profile.null_rate = round(1.0 - completeness, 10)
    return profile


def _to_fraction(value: Any, mode: str) -> float | None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    if mode == _PERCENT:
        return value / 100.0
    if mode == _AUTO:
        return value if value <= 1 else value / 100.0
    return float(value)
