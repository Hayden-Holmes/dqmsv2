"""Aggregate-only table profiling shared by every SQL warehouse connector.

One implementation, with the dialect differences (quoting, sampling) isolated
in `SqlDialect`. Cost controls live here: tables above `max_rows` are sampled,
and the result is marked `sampled` so the rule engine will not treat it as proof
of uniqueness or true extremes. String MIN/MAX is never collected (it would
expose raw values); value lists are opt-in.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from dqms.connectors.base import ProfileOptions
from dqms.connectors.errors import ConnectorError
from dqms.models import Column, ColumnProfile, Profile
from dqms.rules.base import type_family

MAX_ENUM = 20
CHUNK = 40  # columns per aggregate query
_SAFE_PART = re.compile(r"^[A-Za-z0-9_$\-]+$")
_COMPLEX = {
    "RECORD", "STRUCT", "ARRAY", "JSON", "GEOGRAPHY", "BYTES", "VARIANT", "OBJECT", "MAP",
    "BINARY", "VARBINARY", "GEOMETRY", "INTERVAL",
}


class Warehouse(Protocol):
    def query(self, sql: str) -> list[dict[str, Any]]: ...


@dataclass(frozen=True)
class SqlDialect:
    name: str
    quote_path: Callable[[tuple[str, ...]], str]
    sample: Callable[[str, float], str]  # (table expression, percent) -> sampled expression

    def quote_column(self, name: str) -> str:
        name = _safe_column(name)
        return f'"{name}"' if self.name == "snowflake" else f"`{name}`"


def _quote_each(q: str) -> Callable[[tuple[str, ...]], str]:
    return lambda parts: ".".join(f"{q}{p}{q}" for p in parts)


BIGQUERY = SqlDialect(
    "bigquery",
    lambda parts: "`" + ".".join(parts) + "`",
    lambda table, pct: f"{table} TABLESAMPLE SYSTEM ({pct:.6f} PERCENT)",
)
SNOWFLAKE = SqlDialect(
    "snowflake", _quote_each('"'), lambda table, pct: f"{table} TABLESAMPLE BERNOULLI ({pct:.6f})"
)
DATABRICKS = SqlDialect(
    "databricks", _quote_each("`"), lambda table, pct: f"{table} TABLESAMPLE ({pct:.6f} PERCENT)"
)


def safe_part(value: str, what: str) -> str:
    """Identifiers are inlined into SQL, so they are validated rather than escaped."""
    if not isinstance(value, str) or not _SAFE_PART.match(value):
        raise ConnectorError(f"{what} {value!r} may only contain letters, digits, '_', '$' and '-'")
    return value


def _safe_column(name: str) -> str:
    if any(ch in name for ch in ('`', '"', "\n", "\r", "\x00", "\\")):
        raise ConnectorError(f"column name {name!r} cannot be profiled safely")
    return name


def profile_table(
    warehouse: Warehouse,
    dialect: SqlDialect,
    parts: tuple[str, ...],
    columns: list[Column],
    options: ProfileOptions,
) -> Profile:
    table = dialect.quote_path(tuple(safe_part(p, "identifier") for p in parts))
    total = int(_row(warehouse.query(f"SELECT COUNT(*) AS n FROM {table}"))["n"] or 0)
    if total == 0:
        return Profile(row_count=0, total_rows=0)

    sampled = total > options.max_rows
    source = dialect.sample(table, max(0.001, min(100.0, options.max_rows / total * 100))) if sampled else table

    plan = [c for c in columns if (c.base_type or "") not in _COMPLEX]
    profile = Profile(total_rows=total, sampled=sampled)

    for start in range(0, len(plan), CHUNK):
        chunk = plan[start : start + CHUNK]
        selects, meta = ["COUNT(*) AS _rows"], []
        for i, col in enumerate(chunk):
            q, family = dialect.quote_column(col.name), type_family(col)
            selects += [f"COUNTIF({q} IS NULL) AS n{i}" if dialect.name == "bigquery"
                        else f"SUM(CASE WHEN {q} IS NULL THEN 1 ELSE 0 END) AS n{i}",
                        f"COUNT(DISTINCT {q}) AS d{i}"]
            if family in {"numeric", "date", "timestamp"}:
                selects += [f"MIN({q}) AS lo{i}", f"MAX({q}) AS hi{i}"]
            elif family == "string":
                selects += [f"MIN(LENGTH({q})) AS minl{i}", f"MAX(LENGTH({q})) AS maxl{i}"]
            meta.append((i, col, family))

        row = _row(warehouse.query(f"SELECT {', '.join(selects)} FROM {source}"))
        n = int(row["_rows"] or 0)
        if profile.row_count is None:
            profile.row_count = n
        for i, col, family in meta:
            nulls, distinct = int(row[f"n{i}"] or 0), int(row[f"d{i}"] or 0)
            stats = ColumnProfile(
                column=col.name,
                null_rate=(nulls / n) if n else None,
                distinct_count=distinct,
                distinct_rate=(distinct / (n - nulls)) if n - nulls > 0 else None,
            )
            if family in {"numeric", "date", "timestamp"}:
                stats.min_value, stats.max_value = _plain(row[f"lo{i}"]), _plain(row[f"hi{i}"])
            elif family == "string":
                stats.min_length, stats.max_length = _int(row[f"minl{i}"]), _int(row[f"maxl{i}"])
                if options.include_values and not sampled and 0 < distinct <= MAX_ENUM:
                    q = dialect.quote_column(col.name)
                    values = warehouse.query(
                        f"SELECT {q} AS v FROM {source} WHERE {q} IS NOT NULL GROUP BY {q} ORDER BY {q} LIMIT {MAX_ENUM}"
                    )
                    stats.top_values = [str(_lower(r)["v"]) for r in values]
            profile.columns.append(stats)
    return profile


def _lower(row: dict[str, Any]) -> dict[str, Any]:
    return {str(k).lower(): v for k, v in row.items()}  # Snowflake upper-cases unquoted aliases


def _row(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        raise ConnectorError("profiling query returned no rows")
    return _lower(rows[0])


def _int(value: Any) -> int | None:
    return None if value is None else int(value)


def _plain(value: Any) -> Any:
    """JSON-friendly scalar (dates -> ISO strings, Decimal -> float)."""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if value is not None and not isinstance(value, (int, float, str, bool)):
        try:
            return float(value)
        except (TypeError, ValueError):
            return str(value)
    return value


class DbApiWarehouse:
    """Adapts any DB-API connection (Snowflake, Databricks, ...) to the `Warehouse` protocol."""

    def __init__(self, connection: Any):
        self._conn = connection

    def query(self, sql: str) -> list[dict[str, Any]]:
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql)
            if cursor.description is None:
                return []
            names = [d[0] for d in cursor.description]
            return [dict(zip(names, row)) for row in cursor.fetchall()]
        except ConnectorError:
            raise
        except Exception as exc:  # surface permission/timeout errors without echoing SQL
            raise ConnectorError(f"query failed: {type(exc).__name__}: {str(exc)[:300]}") from exc
        finally:
            close = getattr(cursor, "close", None)
            if close:
                close()

    def close(self) -> None:
        close = getattr(self._conn, "close", None)
        if close:
            close()


def yes_no(value: Any) -> bool | None:
    """INFORMATION_SCHEMA is_nullable ('YES'/'NO') -> nullable bool; None when unknown."""
    if isinstance(value, bool):
        return value
    return {"yes": True, "no": False}.get(str(value).strip().lower())
