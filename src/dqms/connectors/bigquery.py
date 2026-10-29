"""BigQuery connector. Requires the `bigquery` extra and application-default credentials.

Metadata comes from the table resource (schema, modes, descriptions, labels,
declared PK/FK constraints). Profiling is a single aggregate query: counts,
null counts, distinct counts, numeric/date min/max and string lengths. String
MIN/MAX is deliberately not collected (it would expose raw values), and value
lists are opt-in (`include_values`).
"""

from __future__ import annotations

import re
from typing import Any

from dqms.connectors.base import ConnectorError, Snapshot
from dqms.models import Column, ColumnProfile, ForeignKey, Metadata, Profile

_IDENT = re.compile(r"^[A-Za-z0-9_\-]+$")
_NUMERIC = {"INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC", "DECIMAL", "BIGDECIMAL"}
_TEMPORAL = {"DATE", "DATETIME", "TIMESTAMP", "TIME"}
_STRING = {"STRING"}
MAX_ENUM = 20


class BigQueryConnector:
    kind = "bigquery"

    def __init__(self, client_factory=None):
        self._client_factory = client_factory

    def fetch(self, ref: dict[str, Any], include_profile: bool = False) -> Snapshot:
        project, dataset, table = (_ident(ref, k) for k in ("project_id", "dataset_id", "table_id"))
        client = self._client(project)
        bq_table = client.get_table(f"{project}.{dataset}.{table}")

        metadata = Metadata(
            name=bq_table.table_id,
            description=bq_table.description,
            tags=[f"{k}:{v}" for k, v in (bq_table.labels or {}).items()],
        )
        for field in bq_table.schema:
            metadata.columns.append(
                Column(
                    name=field.name,
                    datatype=field.field_type,
                    nullable=False if field.mode == "REQUIRED" else True if field.mode == "NULLABLE" else None,
                    max_length=int(field.max_length) if getattr(field, "max_length", None) else None,
                    description=field.description,
                )
            )
        self._apply_constraints(metadata, getattr(bq_table, "table_constraints", None))

        profile = self._profile(client, project, dataset, table, metadata, ref) if include_profile else None
        return Snapshot(metadata, profile)

    # ------------------------------------------------------------------ #

    def _client(self, project: str):
        if self._client_factory:
            return self._client_factory(project)
        try:
            from google.cloud import bigquery
        except ImportError as exc:
            raise ConnectorError("install the 'bigquery' extra: pip install 'dqms[bigquery]'") from exc
        return bigquery.Client(project=project)

    @staticmethod
    def _apply_constraints(metadata: Metadata, constraints: Any) -> None:
        if constraints is None:
            return
        pk = getattr(constraints, "primary_key", None)
        if pk is not None:
            metadata.primary_keys = list(pk.columns)
        for fk in getattr(constraints, "foreign_keys", None) or []:
            ref_table = fk.referenced_table.table_id
            for pair in fk.column_references:
                metadata.foreign_keys.append(
                    ForeignKey(column=pair.referencing_column, references_table=ref_table,
                               references_column=pair.referenced_column)
                )

    def _profile(self, client, project, dataset, table, metadata: Metadata, ref: dict[str, Any]) -> Profile:
        fqn = f"`{project}.{dataset}.{table}`"
        selects, plan = ["COUNT(*) AS _rows"], []
        for i, col in enumerate(metadata.columns):
            kind = (col.datatype or "").upper()
            if kind in {"RECORD", "STRUCT", "ARRAY", "JSON", "GEOGRAPHY", "BYTES"}:
                continue
            q = f"`{col.name}`"
            selects += [f"COUNTIF({q} IS NULL) AS n{i}", f"COUNT(DISTINCT {q}) AS d{i}"]
            if kind in _NUMERIC | _TEMPORAL:
                selects += [f"MIN({q}) AS lo{i}", f"MAX({q}) AS hi{i}"]
            if kind in _STRING:
                selects += [f"MIN(LENGTH({q})) AS minl{i}", f"MAX(LENGTH({q})) AS maxl{i}"]
            plan.append((i, col, kind))

        row = dict(next(iter(client.query(f"SELECT {', '.join(selects)} FROM {fqn}").result())).items())
        total = int(row["_rows"])
        profile = Profile(row_count=total)
        for i, col, kind in plan:
            nulls, distinct = int(row[f"n{i}"]), int(row[f"d{i}"])
            stats = ColumnProfile(
                column=col.name,
                null_rate=(nulls / total) if total else None,
                distinct_count=distinct,
                distinct_rate=(distinct / (total - nulls)) if total - nulls > 0 else None,
            )
            if kind in _NUMERIC | _TEMPORAL:
                stats.min_value, stats.max_value = _plain(row[f"lo{i}"]), _plain(row[f"hi{i}"])
            if kind in _STRING:
                stats.min_length, stats.max_length = row[f"minl{i}"], row[f"maxl{i}"]
                if ref.get("include_values") and 0 < distinct <= MAX_ENUM:
                    q = f"`{col.name}`"
                    values = client.query(
                        f"SELECT {q} AS v FROM {fqn} WHERE {q} IS NOT NULL GROUP BY {q} ORDER BY {q} LIMIT {MAX_ENUM}"
                    ).result()
                    stats.top_values = [str(r["v"]) for r in values]
            profile.columns.append(stats)
        return profile


def _ident(ref: dict[str, Any], key: str) -> str:
    value = ref.get(key)
    if not isinstance(value, str) or not _IDENT.match(value):
        raise ConnectorError(f"'{key}' is required and may only contain letters, digits, '_' and '-'")
    return value


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
