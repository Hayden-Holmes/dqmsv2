"""BigQuery connector. Needs the `bigquery` extra and the service's application-default credentials.

Metadata comes from the table resource: schema, modes, descriptions, labels,
declared PK/FK constraints, and policy tags (treated as "restricted" so those
columns' values are never sent to an LLM). Knowledge Catalog aspects (stewardship,
column semantics, stated business rules) are read too; a catalog that cannot be
read costs a warning, never the table. DQMS_CATALOG=off turns that off. Profiling uses the shared aggregate
profiler, with a cap on bytes billed so a runaway table fails instead of costing.
"""

from __future__ import annotations

import os
from typing import Any

from dqms.catalog import CatalogError, CatalogReader, apply_aspects, fetch_entry_aspects
from dqms.connectors.base import BrowseResult, ConnectorError, DatasetRef, ExplorerNode, ProfileOptions, Snapshot
from dqms.connectors.sqlprofile import BIGQUERY, profile_table, safe_part
from dqms.connectors.uri import SourceURI
from dqms.models import Column, ForeignKey, Metadata

DEFAULT_MAX_BYTES_BILLED = 100 * 2**30  # 100 GiB
_AUTO = object()


class BigQueryConnector:
    schemes = ("bigquery",)

    def __init__(self, client_factory=None, max_bytes_billed: int | None = None, catalog: CatalogReader | None | object = _AUTO):
        self._client_factory = client_factory
        # An injected client means a test double: only the real client gets the real catalog reader.
        if catalog is _AUTO:
            off = os.getenv("DQMS_CATALOG", "").lower() in {"off", "0", "false", "no"}
            catalog = None if off or client_factory is not None else fetch_entry_aspects
        self._catalog: CatalogReader | None = catalog  # type: ignore[assignment]
        self._max_bytes = (
            int(os.getenv("DQMS_BQ_MAX_BYTES_BILLED", DEFAULT_MAX_BYTES_BILLED))
            if max_bytes_billed is None
            else max_bytes_billed  # 0 disables the cap
        )

    def is_container(self, uri: SourceURI) -> bool:
        return len(uri.parts) < 2

    def discover(self, uri: SourceURI, limit: int = 500) -> list[DatasetRef]:
        project = safe_part(uri.authority, "project")
        if len(uri.parts) > 1:
            raise ConnectorError("not a container: " + uri.raw)
        client = self._client(project)
        datasets = [safe_part(uri.parts[0], "dataset")] if uri.parts else [d.dataset_id for d in client.list_datasets()]
        found: list[DatasetRef] = []
        for dataset in datasets:
            for table in client.list_tables(dataset):
                if getattr(table, "table_type", "TABLE") == "TABLE":
                    found.append(DatasetRef(uri=f"bigquery://{project}/{dataset}/{table.table_id}", name=table.table_id))
                    if len(found) >= limit:
                        return found
        return found

    def browse(self, uri: SourceURI, limit: int = 500) -> BrowseResult:
        """project -> datasets -> tables. Only listing calls; no table is read."""
        project = safe_part(uri.authority, "project")
        if len(uri.parts) > 1:
            raise ConnectorError("a table has no children")
        client = self._client(project)
        nodes: list[ExplorerNode] = []
        if not uri.parts:
            for ds in client.list_datasets():
                nodes.append(ExplorerNode(uri=f"bigquery://{project}/{ds.dataset_id}", name=ds.dataset_id, kind="container"))
                if len(nodes) > limit:
                    break
        else:
            dataset = safe_part(uri.parts[0], "dataset")
            for t in client.list_tables(dataset):
                if getattr(t, "table_type", "TABLE") == "TABLE":
                    nodes.append(ExplorerNode(uri=f"bigquery://{project}/{dataset}/{t.table_id}", name=t.table_id, kind="dataset"))
                    if len(nodes) > limit:
                        break
        return BrowseResult(nodes=nodes[:limit], truncated=len(nodes) > limit)

    def fetch(self, uri: SourceURI, profile: ProfileOptions) -> Snapshot:
        if len(uri.parts) != 2:
            raise ConnectorError("expected bigquery://project/dataset/table")
        project = safe_part(uri.authority, "project")
        dataset, table = (safe_part(p, "identifier") for p in uri.parts)
        client = self._client(project)
        bq_table = client.get_table(f"{project}.{dataset}.{table}")

        metadata = Metadata(
            name=bq_table.table_id,
            description=bq_table.description,
            tags=[f"{k}:{v}" for k, v in (bq_table.labels or {}).items()],
        )
        for field in bq_table.schema:
            policy_tags = getattr(getattr(field, "policy_tags", None), "names", None) or ()
            metadata.columns.append(
                Column(
                    name=field.name,
                    # A REPEATED column is an array whatever its element type; treating it as its element
                    # type makes COUNT(DISTINCT)/LENGTH invalid and fails the whole profiling query.
                    datatype="ARRAY" if field.mode == "REPEATED" else field.field_type,
                    nullable=False if field.mode == "REQUIRED" else True if field.mode == "NULLABLE" else None,
                    max_length=int(field.max_length) if getattr(field, "max_length", None) else None,
                    description=field.description,
                    classifications=["restricted (policy tag)"] if policy_tags else [],
                )
            )
        self._apply_constraints(metadata, getattr(bq_table, "table_constraints", None))

        stats, warnings = None, []
        if self._catalog is not None:
            try:
                aspects = self._catalog(project, getattr(bq_table, "location", None) or "us", dataset, table)
                warnings += ["Knowledge Catalog: " + note for note in apply_aspects(metadata, aspects)]
            except CatalogError as exc:
                warnings.append(f"Knowledge Catalog aspects were not read, so catalog-based rules are missing: {exc}")
        if profile.enabled:
            try:
                stats = profile_table(
                    _BigQueryWarehouse(client, self._max_bytes, profile.timeout_s),
                    BIGQUERY, (project, dataset, table), metadata.columns, profile,
                )
            except ConnectorError as exc:  # keep the schema-derived rules rather than losing the table
                warnings.append(f"profiling failed, so only schema-based rules were generated: {exc}")
        return Snapshot(metadata, stats, warnings)

    def _client(self, project: str):
        if self._client_factory:
            return self._client_factory(project)
        try:
            from google.cloud import bigquery
        except ImportError as exc:
            raise ConnectorError("BigQuery support needs: pip install 'dqms[bigquery]'") from exc
        return bigquery.Client(project=project)

    @staticmethod
    def _apply_constraints(metadata: Metadata, constraints: Any) -> None:
        if constraints is None:
            return
        pk = getattr(constraints, "primary_key", None)
        if pk is not None:
            metadata.primary_keys = list(pk.columns)
        for fk in getattr(constraints, "foreign_keys", None) or []:
            for pair in fk.column_references:
                metadata.foreign_keys.append(
                    ForeignKey(column=pair.referencing_column, references_table=fk.referenced_table.table_id,
                               references_column=pair.referenced_column)
                )


class _BigQueryWarehouse:
    def __init__(self, client, max_bytes_billed: int, timeout_s: int):
        self._client, self._max_bytes, self._timeout = client, max_bytes_billed, timeout_s

    def query(self, sql: str) -> list[dict[str, Any]]:
        config = None
        if self._max_bytes:
            from google.cloud import bigquery

            config = bigquery.QueryJobConfig(maximum_bytes_billed=self._max_bytes)
        try:
            rows = self._client.query(sql, job_config=config).result(timeout=self._timeout)
        except Exception as exc:  # surface cost-cap and permission errors without leaking SQL
            raise ConnectorError(f"BigQuery query failed: {type(exc).__name__}: {str(exc)[:300]}") from exc
        return [dict(r.items()) for r in rows]
