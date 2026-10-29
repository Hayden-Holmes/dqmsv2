"""Snowflake connector. Needs the `snowflake` extra; credentials come from the service's environment.

    DQMS_SNOWFLAKE_USER, DQMS_SNOWFLAKE_ROLE, DQMS_SNOWFLAKE_WAREHOUSE
    DQMS_SNOWFLAKE_PASSWORD  or  DQMS_SNOWFLAKE_PRIVATE_KEY_PATH (key-pair auth, preferred)

URI: snowflake://<account>/<database>/<schema>/<table>
"""

from __future__ import annotations

import os
from typing import Any

from dqms.connectors.base import BrowseResult, ConnectorError, DatasetRef, ExplorerNode, ProfileOptions, Snapshot
from dqms.connectors.sqlprofile import SNOWFLAKE, DbApiWarehouse, profile_table, safe_part, yes_no
from dqms.connectors.uri import SourceURI
from dqms.models import Column, ForeignKey, Metadata

UNBOUNDED_TEXT = 16_777_216  # Snowflake's default VARCHAR length, i.e. "no declared limit"


class SnowflakeConnector:
    schemes = ("snowflake",)

    def __init__(self, connection_factory=None):
        self._connection_factory = connection_factory

    def is_container(self, uri: SourceURI) -> bool:
        return len(uri.parts) < 3

    def discover(self, uri: SourceURI, limit: int = 500) -> list[DatasetRef]:
        if not 1 <= len(uri.parts) <= 2:
            raise ConnectorError("expected snowflake://account/database or snowflake://account/database/schema")
        db = safe_part(uri.parts[0], "database")
        where = "table_type = 'BASE TABLE'"
        if len(uri.parts) == 2:
            where += f" AND table_schema = '{safe_part(uri.parts[1], 'schema')}'"
        wh = self._warehouse(uri.authority)
        try:
            rows = wh.query(
                f'SELECT table_schema, table_name, row_count, bytes FROM "{db}".INFORMATION_SCHEMA.TABLES WHERE {where} '
                f"ORDER BY table_schema, table_name LIMIT {int(limit)}"
            )
        finally:
            wh.close()
        return [
            DatasetRef(uri=f"snowflake://{uri.authority}/{db}/{r['table_schema']}/{r['table_name']}", name=r["table_name"],
                       rows=_opt_int(r.get("row_count")), bytes=_opt_int(r.get("bytes")))
            for r in map(_lower, rows)
        ]

    def browse(self, uri: SourceURI, limit: int = 500) -> BrowseResult:
        """account -> databases -> schemas -> tables, from SHOW / information_schema only."""
        depth = len(uri.parts)
        if depth > 2:
            raise ConnectorError("a table has no children")
        base = f"snowflake://{uri.authority}"
        if depth == 2:
            refs = self.discover(uri, limit + 1)
            nodes = [ExplorerNode(uri=r.uri, name=r.name, kind="dataset", bytes=r.bytes, rows=r.rows) for r in refs]
            return BrowseResult(nodes=nodes[:limit], truncated=len(nodes) > limit)
        wh = self._warehouse(uri.authority)
        try:
            if depth == 0:
                rows = [_lower(r) for r in wh.query("SHOW DATABASES")]
                names = [r["name"] for r in rows]
                nodes = [ExplorerNode(uri=f"{base}/{n}", name=n, kind="container") for n in names if _safe(n)]
            else:
                db = safe_part(uri.parts[0], "database")
                rows = [_lower(r) for r in wh.query(
                    f'SELECT schema_name FROM "{db}".INFORMATION_SCHEMA.SCHEMATA ORDER BY schema_name LIMIT {int(limit) + 1}')]
                nodes = [ExplorerNode(uri=f"{base}/{db}/{r['schema_name']}", name=r["schema_name"], kind="container")
                         for r in rows if r["schema_name"].upper() != "INFORMATION_SCHEMA" and _safe(r["schema_name"])]
        finally:
            wh.close()
        return BrowseResult(nodes=nodes[:limit], truncated=len(nodes) > limit)

    def fetch(self, uri: SourceURI, profile: ProfileOptions) -> Snapshot:
        if len(uri.parts) != 3:
            raise ConnectorError("expected snowflake://account/database/schema/table")
        db, schema, table = (safe_part(p, n) for p, n in zip(uri.parts, ("database", "schema", "table")))
        wh = self._warehouse(uri.authority, profile.timeout_s)
        try:
            metadata = self._metadata(wh, db, schema, table)
            stats = profile_table(wh, SNOWFLAKE, (db, schema, table), metadata.columns, profile) if profile.enabled else None
        finally:
            wh.close()
        return Snapshot(metadata, stats)

    def _metadata(self, wh: DbApiWarehouse, db: str, schema: str, table: str) -> Metadata:
        info = f'"{db}".INFORMATION_SCHEMA'
        scope = f"table_schema = '{schema}' AND table_name = '{table}'"
        columns = [_lower(r) for r in wh.query(
            "SELECT column_name, data_type, is_nullable, character_maximum_length, comment "
            f"FROM {info}.COLUMNS WHERE {scope} ORDER BY ordinal_position"
        )]
        if not columns:
            raise ConnectorError(f"table {db}.{schema}.{table} not found or not visible to the service role")
        comment = next(iter(wh.query(f"SELECT comment FROM {info}.TABLES WHERE {scope}")), {})
        meta = Metadata(name=table, description=_lower(comment).get("comment") or None)
        for c in columns:
            length = c["character_maximum_length"]
            meta.columns.append(Column(
                name=c["column_name"], datatype=str(c["data_type"]).upper(), nullable=yes_no(c["is_nullable"]),
                max_length=int(length) if length and int(length) != UNBOUNDED_TEXT else None,
                description=c["comment"] or None,
            ))
        qualified = f'"{db}"."{schema}"."{table}"'
        # Constraints are informational in Snowflake, but they are *declared* intent, so they count.
        try:
            pk = sorted((_lower(r) for r in wh.query(f"SHOW PRIMARY KEYS IN TABLE {qualified}")),
                        key=lambda r: int(r.get("key_sequence") or 0))
            meta.primary_keys = [r["column_name"] for r in pk]
            for r in map(_lower, wh.query(f"SHOW IMPORTED KEYS IN TABLE {qualified}")):
                meta.foreign_keys.append(ForeignKey(column=r["fk_column_name"], references_table=r["pk_table_name"],
                                                    references_column=r["pk_column_name"]))
        except ConnectorError:
            pass  # missing privilege on constraints should not block profiling
        return meta

    def _warehouse(self, account: str, timeout_s: int = 300) -> DbApiWarehouse:
        safe_part(account.replace(".", "-"), "account")
        conn = self._connection_factory(account) if self._connection_factory else _connect(account)
        wh = DbApiWarehouse(conn)
        wh.query(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {int(timeout_s)}")
        return wh


def _safe(name: str) -> bool:
    try:
        safe_part(name, "name")
        return True
    except ConnectorError:
        return False  # names we could not safely address later are not offered


def _opt_int(value: Any) -> int | None:
    return int(value) if value is not None else None


def _lower(row: dict[str, Any]) -> dict[str, Any]:
    return {str(k).lower(): v for k, v in row.items()}


def _connect(account: str):
    try:
        import snowflake.connector as sf
    except ImportError as exc:
        raise ConnectorError("Snowflake support needs: pip install 'dqms[snowflake]'") from exc
    params: dict[str, Any] = {
        "account": account, "user": os.getenv("DQMS_SNOWFLAKE_USER"),
        "role": os.getenv("DQMS_SNOWFLAKE_ROLE"), "warehouse": os.getenv("DQMS_SNOWFLAKE_WAREHOUSE"),
    }
    key_path = os.getenv("DQMS_SNOWFLAKE_PRIVATE_KEY_PATH")
    if key_path:
        from cryptography.hazmat.primitives import serialization

        with open(key_path, "rb") as fh:
            key = serialization.load_pem_private_key(fh.read(), password=None)
        params["private_key"] = key.private_bytes(
            serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        )
    else:
        params["password"] = os.getenv("DQMS_SNOWFLAKE_PASSWORD")
    if not params["user"] or not (key_path or params["password"]):
        raise ConnectorError("Snowflake credentials are not configured on the service")
    return sf.connect(**{k: v for k, v in params.items() if v})
