"""Databricks / Unity Catalog connector. Needs the `databricks` extra.

Credentials come from the service's environment:
    DQMS_DATABRICKS_HTTP_PATH   SQL warehouse HTTP path
    DQMS_DATABRICKS_TOKEN       personal access token or service-principal token

URI: databricks://<workspace-host>/<catalog>/<schema>/<table>
Besides schema and comments it reads declared constraints and Unity Catalog column
tags (tags that look sensitive mark the column so its values are withheld from any LLM).
"""

from __future__ import annotations

import os
from typing import Any

from dqms.connectors.base import BrowseResult, ConnectorError, DatasetRef, ExplorerNode, ProfileOptions, Snapshot
from dqms.connectors.sqlprofile import DATABRICKS, DbApiWarehouse, profile_table, safe_part, yes_no
from dqms.connectors.uri import SourceURI
from dqms.models import Column, ForeignKey, Metadata


class DatabricksConnector:
    schemes = ("databricks",)

    def __init__(self, connection_factory=None):
        self._connection_factory = connection_factory

    def is_container(self, uri: SourceURI) -> bool:
        return len(uri.parts) < 3

    def discover(self, uri: SourceURI, limit: int = 500) -> list[DatasetRef]:
        if not 1 <= len(uri.parts) <= 2:
            raise ConnectorError("expected databricks://host/catalog or databricks://host/catalog/schema")
        catalog = safe_part(uri.parts[0], "catalog")
        where = "table_type IN ('MANAGED', 'EXTERNAL', 'BASE TABLE')"
        if len(uri.parts) == 2:
            where += f" AND table_schema = '{safe_part(uri.parts[1], 'schema')}'"
        wh = self._warehouse(uri.authority)
        try:
            rows = wh.query(
                f"SELECT table_schema, table_name FROM `{catalog}`.information_schema.tables WHERE {where} "
                f"ORDER BY table_schema, table_name LIMIT {int(limit)}"
            )
        finally:
            wh.close()
        return [DatasetRef(uri=f"databricks://{uri.authority}/{catalog}/{r['table_schema']}/{r['table_name']}",
                           name=r["table_name"]) for r in map(_lower, rows)]

    def browse(self, uri: SourceURI, limit: int = 500) -> BrowseResult:
        """workspace -> catalogs -> schemas -> tables, from SHOW / information_schema only."""
        depth = len(uri.parts)
        if depth > 2:
            raise ConnectorError("a table has no children")
        base = f"databricks://{uri.authority}"
        if depth == 2:
            refs = self.discover(uri, limit + 1)
            nodes = [ExplorerNode(uri=r.uri, name=r.name, kind="dataset") for r in refs]
            return BrowseResult(nodes=nodes[:limit], truncated=len(nodes) > limit)
        wh = self._warehouse(uri.authority)
        try:
            if depth == 0:
                rows = [_lower(r) for r in wh.query("SHOW CATALOGS")]
                names = [next(iter(r.values())) for r in rows]
                nodes = [ExplorerNode(uri=f"{base}/{n}", name=n, kind="container") for n in names if _safe(n)]
            else:
                catalog = safe_part(uri.parts[0], "catalog")
                rows = [_lower(r) for r in wh.query(
                    f"SELECT schema_name FROM `{catalog}`.information_schema.schemata ORDER BY schema_name LIMIT {int(limit) + 1}")]
                nodes = [ExplorerNode(uri=f"{base}/{catalog}/{r['schema_name']}", name=r["schema_name"], kind="container")
                         for r in rows if r["schema_name"] != "information_schema" and _safe(r["schema_name"])]
        finally:
            wh.close()
        return BrowseResult(nodes=nodes[:limit], truncated=len(nodes) > limit)

    def fetch(self, uri: SourceURI, profile: ProfileOptions) -> Snapshot:
        if len(uri.parts) != 3:
            raise ConnectorError("expected databricks://host/catalog/schema/table")
        catalog, schema, table = (safe_part(p, n) for p, n in zip(uri.parts, ("catalog", "schema", "table")))
        wh = self._warehouse(uri.authority)
        try:
            metadata = self._metadata(wh, catalog, schema, table)
            stats, warnings = None, []
            if profile.enabled:
                try:
                    stats = profile_table(wh, DATABRICKS, (catalog, schema, table), metadata.columns, profile)
                except ConnectorError as exc:
                    warnings.append(f"profiling failed, so only schema-based rules were generated: {exc}")
        finally:
            wh.close()
        return Snapshot(metadata, stats, warnings)

    def _metadata(self, wh: DbApiWarehouse, catalog: str, schema: str, table: str) -> Metadata:
        info = f"`{catalog}`.information_schema"
        scope = f"table_schema = '{schema}' AND table_name = '{table}'"
        columns = [_lower(r) for r in wh.query(
            "SELECT column_name, data_type, is_nullable, character_maximum_length, comment "
            f"FROM {info}.columns WHERE {scope} ORDER BY ordinal_position"
        )]
        if not columns:
            raise ConnectorError(f"table {catalog}.{schema}.{table} not found or not visible to the service")
        comment = next(iter(wh.query(f"SELECT comment FROM {info}.tables WHERE {scope}")), {})
        meta = Metadata(name=table, description=_lower(comment).get("comment") or None)
        for c in columns:
            length = c.get("character_maximum_length")
            meta.columns.append(Column(
                name=c["column_name"], datatype=str(c["data_type"]).upper(), nullable=yes_no(c["is_nullable"]),
                max_length=int(length) if length else None, description=c.get("comment") or None,
            ))

        join = (f"FROM {info}.table_constraints tc JOIN {info}.key_column_usage kcu "
                "ON tc.constraint_catalog = kcu.constraint_catalog AND tc.constraint_schema = kcu.constraint_schema "
                "AND tc.constraint_name = kcu.constraint_name")
        tc_scope = f"tc.table_schema = '{schema}' AND tc.table_name = '{table}'"
        for step in (self._keys, self._tags):  # each is best effort: missing privileges must not block
            try:
                step(wh, meta, info, join, tc_scope, scope)
            except ConnectorError:
                pass
        return meta

    @staticmethod
    def _keys(wh, meta: Metadata, info: str, join: str, tc_scope: str, _scope: str) -> None:
        pk = wh.query(f"SELECT kcu.column_name {join} WHERE tc.constraint_type = 'PRIMARY KEY' AND {tc_scope} "
                      "ORDER BY kcu.ordinal_position")
        meta.primary_keys = [_lower(r)["column_name"] for r in pk]
        fk = wh.query(
            "SELECT kcu.column_name AS fk_column, ccu.table_name AS ref_table, ccu.column_name AS ref_column "
            f"{join} JOIN {info}.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name "
            f"AND tc.constraint_schema = ccu.constraint_schema WHERE tc.constraint_type = 'FOREIGN KEY' AND {tc_scope}"
        )
        for r in map(_lower, fk):
            meta.foreign_keys.append(ForeignKey(column=r["fk_column"], references_table=r["ref_table"],
                                                references_column=r["ref_column"]))

    @staticmethod
    def _tags(wh, meta: Metadata, info: str, _join: str, _tc: str, scope: str) -> None:
        for r in map(_lower, wh.query(f"SELECT column_name, tag_name, tag_value FROM {info}.column_tags WHERE {scope.replace('table_schema', 'schema_name')}")):
            col = meta.column(r["column_name"])
            if col is not None:
                value = r.get("tag_value")
                col.classifications.append(f"{r['tag_name']}={value}" if value else str(r["tag_name"]))

    def _warehouse(self, host: str) -> DbApiWarehouse:
        safe_part(host.replace(".", "-"), "workspace host")
        return DbApiWarehouse(self._connection_factory(host) if self._connection_factory else _connect(host))


def _safe(name: str) -> bool:
    try:
        safe_part(name, "name")
        return True
    except ConnectorError:
        return False


def _lower(row: dict[str, Any]) -> dict[str, Any]:
    return {str(k).lower(): v for k, v in row.items()}


def _connect(host: str):
    try:
        from databricks import sql
    except ImportError as exc:
        raise ConnectorError("Databricks support needs: pip install 'dqms[databricks]'") from exc
    http_path, token = os.getenv("DQMS_DATABRICKS_HTTP_PATH"), os.getenv("DQMS_DATABRICKS_TOKEN")
    if not http_path or not token:
        raise ConnectorError("Databricks credentials are not configured on the service")
    return sql.connect(server_hostname=host, http_path=http_path, access_token=token)
