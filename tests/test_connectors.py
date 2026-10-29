import io
import json
from datetime import date
from types import SimpleNamespace

import pytest
from fakes import FakeBigQueryClient, FakeConnection, FakeWarehouse, MemStore, bq_field

from dqms.connectors.base import ConnectorError, ConnectorRegistry, ProfileOptions
from dqms.connectors.bigquery import BigQueryConnector
from dqms.connectors.databricks import DatabricksConnector
from dqms.connectors.objectstore import ObjectStoreConnector, RangeFile
from dqms.connectors.snowflake import SnowflakeConnector
from dqms.connectors.sqlprofile import BIGQUERY, SNOWFLAKE, profile_table
from dqms.connectors.uri import parse_source
from dqms.models import Column, RuleType, Tier
from dqms.service import RuleService

PROFILE = ProfileOptions(enabled=True)


# ------------------------------- URIs --------------------------------- #


def test_source_uri_parsing():
    uri = parse_source("bigquery://my-proj/ds/tbl")
    assert (uri.scheme, uri.authority, uri.parts) == ("bigquery", "my-proj", ("ds", "tbl"))
    assert parse_source("gs://bucket/a/b/").trailing_slash
    for bad in ("not-a-uri", "gs://", "gs://b/x?y=1", "gs://b/../secret", "s3://b/a/./b"):
        with pytest.raises(ConnectorError):
            parse_source(bad)


def test_registry_reports_unknown_scheme():
    with pytest.raises(ConnectorError, match="no connector for 'ftp://'"):
        ConnectorRegistry().resolve("ftp://host/file")


# --------------------------- SQL profiler ----------------------------- #


def orders_columns():
    return [Column(name="id", datatype="INTEGER"), Column(name="status", datatype="STRING"),
            Column(name="placed", datatype="DATE"), Column(name="payload", datatype="JSON")]


def agg_row(**kw):
    base = {"_rows": 1000, "n0": 0, "d0": 1000, "lo0": 1, "hi0": 1000, "n1": 10, "d1": 3, "minl1": 3, "maxl1": 9,
            "n2": 0, "d2": 300, "lo2": date(2024, 1, 1), "hi2": date(2024, 10, 1)}
    return [{**base, **kw}]


def test_profile_math_and_no_string_extremes():
    wh = FakeWarehouse(("COUNT(*) AS n FROM", [{"n": 1000}]), ("AS _rows", agg_row()))
    profile = profile_table(wh, BIGQUERY, ("p", "d", "orders"), orders_columns(), PROFILE)
    ids, status, placed = profile.columns  # JSON column is skipped entirely
    assert not profile.sampled and profile.row_count == 1000
    assert ids.null_rate == 0 and ids.distinct_rate == 1.0 and (ids.min_value, ids.max_value) == (1, 1000)
    assert status.null_rate == 0.01 and status.distinct_rate == pytest.approx(3 / 990)
    assert (status.min_length, status.max_length) == (3, 9) and status.min_value is None
    assert (placed.min_value, placed.max_value) == ("2024-01-01", "2024-10-01")
    assert not any("MIN(`status`)" in s or "MAX(`status`)" in s for s in wh.sql)
    assert not any("`payload`" in s for s in wh.sql)


def test_large_tables_are_sampled_and_marked():
    wh = FakeWarehouse(("COUNT(*) AS n FROM", [{"n": 50_000_000}]), ("AS _rows", agg_row(_rows=5000)))
    opts = ProfileOptions(enabled=True, max_rows=5000, include_values=True)
    profile = profile_table(wh, BIGQUERY, ("p", "d", "orders"), orders_columns(), opts)
    assert profile.sampled and profile.total_rows == 50_000_000 and profile.row_count == 5000
    assert any("TABLESAMPLE SYSTEM" in s for s in wh.sql)
    assert profile.column("status").top_values is None  # a sample cannot prove a closed value set


def test_value_lists_are_opt_in():
    handlers = [("COUNT(*) AS n FROM", [{"n": 1000}]), ("AS _rows", agg_row()),
                ("GROUP BY", [{"v": "NEW"}, {"v": "SHIPPED"}, {"v": "DONE"}])]
    off = profile_table(FakeWarehouse(*handlers), SNOWFLAKE, ("DB", "S", "T"), orders_columns(), PROFILE)
    on = profile_table(FakeWarehouse(*handlers), SNOWFLAKE, ("DB", "S", "T"), orders_columns(),
                       ProfileOptions(enabled=True, include_values=True))
    assert off.column("status").top_values is None
    assert on.column("status").top_values == ["NEW", "SHIPPED", "DONE"]


def test_unsafe_identifiers_never_reach_sql():
    wh = FakeWarehouse()
    with pytest.raises(ConnectorError):
        profile_table(wh, BIGQUERY, ("p", "d", "t`; DROP TABLE x; --"), orders_columns(), PROFILE)
    wh = FakeWarehouse(("COUNT(*) AS n FROM", [{"n": 10}]))
    with pytest.raises(ConnectorError):
        profile_table(wh, SNOWFLAKE, ("DB", "S", "T"), [Column(name='a"; DROP TABLE x; --', datatype="INTEGER")], PROFILE)
    assert all("DROP" not in s for s in wh.sql)


def test_snowflake_uppercase_aliases_are_handled():
    upper = [{k.upper(): v for k, v in agg_row()[0].items()}]
    wh = FakeWarehouse(("COUNT(*) AS n FROM", [{"N": 1000}]), ("AS _rows", upper))
    profile = profile_table(wh, SNOWFLAKE, ("DB", "S", "T"), orders_columns(), PROFILE)
    assert profile.row_count == 1000 and profile.column("id").distinct_rate == 1.0


# ------------------------------ BigQuery ------------------------------- #


def fake_bq(profile_rows=None):
    constraints = SimpleNamespace(
        primary_key=SimpleNamespace(columns=["id"]),
        foreign_keys=[SimpleNamespace(referenced_table=SimpleNamespace(table_id="customers"),
                                      column_references=[SimpleNamespace(referencing_column="customer_id",
                                                                         referenced_column="id")])],
    )
    table = SimpleNamespace(
        table_id="orders", description="All orders", labels={"team": "growth"}, table_constraints=constraints,
        schema=[bq_field("id", "INTEGER", "REQUIRED"), bq_field("customer_id", "INTEGER"),
                bq_field("email", "STRING", description="Contact", policy_tags=["pt/1"])],
    )
    answers = [("COUNT(*) AS n FROM", [{"n": 1000}]),
               ("AS _rows", profile_rows or [{"_rows": 1000, "n0": 0, "d0": 1000, "lo0": 1, "hi0": 1000,
                                              "n1": 5, "d1": 100, "lo1": 1, "hi1": 99,
                                              "n2": 0, "d2": 900, "minl2": 5, "maxl2": 40}])]
    return FakeBigQueryClient(table, answers, {"sales": [("orders", "TABLE"), ("v_orders", "VIEW")], "hr": [("staff", "TABLE")]})


def test_bigquery_metadata_mapping():
    connector = BigQueryConnector(client_factory=lambda p: fake_bq(), max_bytes_billed=0)
    snap = connector.fetch(parse_source("bigquery://proj/sales/orders"), ProfileOptions())
    meta = snap.metadata
    assert snap.profile is None
    assert meta.name == "orders" and meta.tags == ["team:growth"] and meta.primary_keys == ["id"]
    assert meta.column("id").nullable is False and meta.column("customer_id").nullable is True
    assert "restricted (policy tag)" in meta.column("email").classifications
    assert meta.foreign_keys[0].references_table == "customers"


def test_bigquery_profile_and_rules_end_to_end():
    registry = ConnectorRegistry()
    registry.register(BigQueryConnector(client_factory=lambda p: fake_bq(), max_bytes_billed=0))
    analysis = RuleService(connectors=registry).analyze_source("bigquery://proj/sales/orders", PROFILE)
    assert analysis.source == "bigquery://proj/sales/orders" and analysis.profile.row_count == 1000
    assert any(r.type is RuleType.UNIQUE and r.columns == ["id"] and r.tier is Tier.DECLARED for r in analysis.ruleset.rules)
    assert any(r.type is RuleType.FOREIGN_KEY for r in analysis.ruleset.rules)


def test_bigquery_discovery_lists_tables_only():
    connector = BigQueryConnector(client_factory=lambda p: fake_bq(), max_bytes_billed=0)
    assert connector.is_container(parse_source("bigquery://proj")) and connector.is_container(parse_source("bigquery://proj/sales"))
    assert not connector.is_container(parse_source("bigquery://proj/sales/orders"))
    uris = {r.uri for r in connector.discover(parse_source("bigquery://proj"))}
    assert uris == {"bigquery://proj/sales/orders", "bigquery://proj/hr/staff"}  # views skipped
    assert len(connector.discover(parse_source("bigquery://proj"), limit=1)) == 1


def test_bigquery_rejects_bad_refs():
    connector = BigQueryConnector(client_factory=lambda p: fake_bq(), max_bytes_billed=0)
    with pytest.raises(ConnectorError):
        connector.fetch(parse_source("bigquery://proj/ds"), PROFILE)
    with pytest.raises(ConnectorError):
        connector.fetch(parse_source("bigquery://proj/ds/t;drop"), PROFILE)


# ------------------------------ Snowflake ------------------------------ #


def snowflake_conn():
    return FakeConnection(
        ("ALTER SESSION", []),
        ("INFORMATION_SCHEMA.COLUMNS", [
            {"COLUMN_NAME": "ID", "DATA_TYPE": "NUMBER", "IS_NULLABLE": "NO", "CHARACTER_MAXIMUM_LENGTH": None, "COMMENT": "pk"},
            {"COLUMN_NAME": "NOTE", "DATA_TYPE": "TEXT", "IS_NULLABLE": "YES", "CHARACTER_MAXIMUM_LENGTH": 16777216, "COMMENT": None},
            {"COLUMN_NAME": "CODE", "DATA_TYPE": "TEXT", "IS_NULLABLE": "NO", "CHARACTER_MAXIMUM_LENGTH": 3, "COMMENT": None}]),
        ("INFORMATION_SCHEMA.TABLES WHERE table_schema", [{"COMMENT": "Order facts"}]),
        ("SHOW PRIMARY KEYS", [{"column_name": "B", "key_sequence": 2}, {"column_name": "A", "key_sequence": 1}]),
        ("SHOW IMPORTED KEYS", [{"fk_column_name": "CODE", "pk_table_name": "CODES", "pk_column_name": "CODE"}]),
    )


def test_snowflake_metadata():
    connector = SnowflakeConnector(connection_factory=lambda account: snowflake_conn())
    snap = connector.fetch(parse_source("snowflake://acct/DB/PUB/ORDERS"), ProfileOptions())
    m = snap.metadata
    assert m.description == "Order facts" and m.primary_keys == ["A", "B"]  # ordered by key_sequence
    assert m.column("ID").nullable is False and m.column("NOTE").max_length is None  # unbounded TEXT
    assert m.column("CODE").max_length == 3
    assert (m.foreign_keys[0].column, m.foreign_keys[0].references_table) == ("CODE", "CODES")


def test_snowflake_missing_constraint_privilege_does_not_block():
    conn = snowflake_conn()
    conn.handlers.insert(0, ("SHOW PRIMARY KEYS", ConnectorError("insufficient privileges")))
    snap = SnowflakeConnector(connection_factory=lambda a: conn).fetch(parse_source("snowflake://a/DB/S/T"), ProfileOptions())
    assert snap.metadata.columns and snap.metadata.primary_keys == []


def test_snowflake_container_and_unknown_table():
    conn = FakeConnection(("ALTER SESSION", []), ("INFORMATION_SCHEMA.TABLES", [
        {"TABLE_SCHEMA": "PUB", "TABLE_NAME": "ORDERS"}, {"TABLE_SCHEMA": "PUB", "TABLE_NAME": "ITEMS"}]),
        ("INFORMATION_SCHEMA.COLUMNS", []))
    connector = SnowflakeConnector(connection_factory=lambda a: conn)
    assert connector.is_container(parse_source("snowflake://a/DB")) and not connector.is_container(parse_source("snowflake://a/DB/S/T"))
    assert [r.uri for r in connector.discover(parse_source("snowflake://a/DB/PUB"))] == [
        "snowflake://a/DB/PUB/ORDERS", "snowflake://a/DB/PUB/ITEMS"]
    with pytest.raises(ConnectorError, match="not found or not visible"):
        connector.fetch(parse_source("snowflake://a/DB/PUB/NOPE"), ProfileOptions())


# ------------------------------ Databricks ----------------------------- #


def test_databricks_metadata_tags_and_best_effort_constraints():
    conn = FakeConnection(
        ("information_schema.columns", [
            {"column_name": "id", "data_type": "BIGINT", "is_nullable": "NO", "character_maximum_length": None, "comment": "key"},
            {"column_name": "ssn", "data_type": "STRING", "is_nullable": "YES", "character_maximum_length": None, "comment": None}]),
        ("information_schema.tables WHERE", [{"comment": "People"}]),
        ("constraint_type = 'PRIMARY KEY'", [{"column_name": "id"}]),
        ("constraint_type = 'FOREIGN KEY'", ConnectorError("no privilege")),
        ("information_schema.column_tags", [{"column_name": "ssn", "tag_name": "pii", "tag_value": "true"}]),
    )
    snap = DatabricksConnector(connection_factory=lambda host: conn).fetch(
        parse_source("databricks://host.example.com/main/hr/people"), ProfileOptions())
    m = snap.metadata
    assert m.description == "People" and m.primary_keys == ["id"] and m.foreign_keys == []
    assert m.column("ssn").classifications == ["pii=true"]


# ----------------------------- object stores --------------------------- #

ORDERS_CSV = (
    "order_id,zip,amount,placed,status,legacy_flag\n"
    + "".join(f"{i},0{1000 + i},{i}.5,2024-0{1 + i % 9}-1{i % 9},{['NEW', 'SHIPPED'][i % 2]},{'N/A' if i % 7 == 0 else i}\n"
              for i in range(1, 201))
).encode()


def csv_connector(objects=None, **extra):
    store = MemStore(objects or {("bkt", "exports/orders.csv"): ORDERS_CSV, **extra})
    return ObjectStoreConnector(store_factory=lambda scheme, authority: store), store


def test_csv_type_inference_is_conservative():
    connector, _ = csv_connector()
    snap = connector.fetch(parse_source("gs://bkt/exports/orders.csv"), PROFILE)
    types = {c.name: c.datatype for c in snap.metadata.columns}
    assert types == {"order_id": "INTEGER", "zip": "STRING", "amount": "FLOAT", "placed": "DATE",
                     "status": "STRING", "legacy_flag": "STRING"}  # zip keeps leading zeros; N/A makes flag a string
    assert all(c.nullable is None for c in snap.metadata.columns)  # nullability is unknowable from a file
    ids = snap.profile.column("order_id")
    assert (ids.min_value, ids.max_value, ids.distinct_rate, ids.null_rate) == (1, 200, 1.0, 0.0)
    assert snap.profile.row_count == 200 and not snap.profile.sampled
    assert snap.profile.column("status").top_values is None  # value lists are opt-in


def test_csv_truncation_marks_profile_sampled_and_drops_partial_row():
    connector, store = csv_connector()
    snap = connector.fetch(parse_source("gs://bkt/exports/orders.csv"), ProfileOptions(enabled=True, max_bytes=2000))
    assert snap.profile.sampled and 0 < snap.profile.row_count < 200
    assert store.reads[-1][2] == 2000  # only the requested bytes were read, not the whole object


def test_csv_max_rows_caps_and_marks_sampled():
    connector, _ = csv_connector()
    snap = connector.fetch(parse_source("gs://bkt/exports/orders.csv"), ProfileOptions(enabled=True, max_rows=50))
    assert snap.profile.row_count == 50 and snap.profile.sampled


def test_csv_ragged_rows_blank_and_duplicate_headers():
    data = b"a,a,\n1,2,3\n4\n\n5,6,7,8\n"
    connector, _ = csv_connector({("b", "x.csv"): data})
    snap = connector.fetch(parse_source("s3://b/x.csv"), PROFILE)
    assert [c.name for c in snap.metadata.columns] == ["a", "a_2", "column_3"]
    assert snap.profile.row_count == 3 and snap.profile.column("a_2").null_rate == pytest.approx(1 / 3)


def test_jsonl_profile_skips_bad_lines():
    lines = [json.dumps({"id": i, "ok": i % 2 == 0, "ts": "2024-05-01T10:00:00Z", "n": None if i == 3 else 1.5})
             for i in range(10)]
    data = ("\n".join(lines[:5] + ["{not json"] + lines[5:])).encode()
    connector, _ = csv_connector({("b", "e.jsonl"): data})
    snap = connector.fetch(parse_source("s3://b/e.jsonl"), PROFILE)
    types = {c.name: c.datatype for c in snap.metadata.columns}
    assert types == {"id": "INTEGER", "ok": "BOOLEAN", "ts": "TIMESTAMP", "n": "FLOAT"}
    assert snap.profile.row_count == 10 and snap.profile.column("n").null_rate == 0.1


def test_parquet_uses_footer_statistics_without_scanning():
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    table = pa.table({"id": pa.array(range(1, 1001), pa.int64()),
                      "name": pa.array([None if i % 10 == 0 else f"n{i}" for i in range(1000)]),
                      "day": pa.array([date(2024, 1, 1 + i % 28) for i in range(1000)])})
    buf = io.BytesIO()
    pq.write_table(table, buf, row_group_size=300)
    connector, store = csv_connector({("b", "t.parquet"): buf.getvalue()})
    snap = connector.fetch(parse_source("gs://b/t.parquet"), PROFILE)
    assert {c.name: c.datatype for c in snap.metadata.columns} == {"id": "INTEGER", "name": "STRING", "day": "DATE"}
    assert snap.profile.row_count == 1000 and not snap.profile.sampled
    assert (snap.profile.column("id").min_value, snap.profile.column("id").max_value) == (1, 1000)
    assert snap.profile.column("name").null_rate == 0.1
    assert snap.profile.column("name").min_value is None  # string extremes would expose raw values
    assert snap.profile.column("day").max_value == "2024-01-28"


def test_parquet_stats_come_from_ranged_reads_not_a_full_download():
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    import random

    rnd = random.Random(7)
    n = 60_000
    table = pa.table({"id": pa.array(range(n), pa.int64()),
                      "blob": pa.array([f"{rnd.getrandbits(128):032x}" for _ in range(n)])})
    buf = io.BytesIO()
    pq.write_table(table, buf, row_group_size=10_000, compression="none")
    size = len(buf.getvalue())
    assert size > 2_000_000  # far larger than the 256 KB read-ahead window
    connector, store = csv_connector({("b", "big.parquet"): buf.getvalue()})
    snap = connector.fetch(parse_source("gs://b/big.parquet"), PROFILE)
    assert snap.profile.row_count == n and snap.profile.column("id").max_value == n - 1
    assert sum(length for _, _, length in store.reads) < size * 0.15


def test_range_file_is_seekable_and_cached():
    store = MemStore({("b", "k"): bytes(range(256)) * 4})
    f = RangeFile(store, "b", "k", 1024)
    f.seek(-4, io.SEEK_END)
    assert f.read(4) == (bytes(range(256)) * 4)[-4:]
    f.seek(10)
    assert f.read(5) == bytes(range(10, 15)) and f.read(5) == bytes(range(15, 20))
    assert len(store.reads) == 2  # the second read was served from the read-ahead cache


def test_container_discovery_and_layouts():
    connector, _ = csv_connector({
        ("bkt", "exports/a.csv"): b"x\n1\n", ("bkt", "exports/b.parquet"): b"PAR1....",
        ("bkt", "exports/_SUCCESS"): b"", ("bkt", "exports/readme.md"): b"hi", ("bkt", "exports/empty.csv"): b"",
        ("bkt", "other/c.csv"): b"x\n1\n",
    })
    assert connector.is_container(parse_source("gs://bkt/exports/")) and connector.is_container(parse_source("gs://bkt"))
    assert not connector.is_container(parse_source("gs://bkt/exports/a.csv"))
    assert [r.uri for r in connector.discover(parse_source("gs://bkt/exports/"))] == [
        "gs://bkt/exports/a.csv", "gs://bkt/exports/b.parquet"]  # no markers, docs or empty files
    az, _ = csv_connector({("data", "in/x.csv"): b"a\n1\n"})
    assert [r.uri for r in az.discover(parse_source("az://acct/data/in/"))] == ["az://acct/data/in/x.csv"]
    assert az.fetch(parse_source("az://acct/data/in/x.csv"), PROFILE).metadata.name == "x"


def test_object_store_errors():
    connector, _ = csv_connector()
    with pytest.raises(ConnectorError, match="not found"):
        connector.fetch(parse_source("gs://bkt/exports/missing.csv"), PROFILE)
    with pytest.raises(ConnectorError, match="unsupported file type"):
        connector.fetch(parse_source("gs://bkt/exports/orders.xlsx"), PROFILE)
    empty, _ = csv_connector({("b", "h.csv"): b"a,b\n"})
    with pytest.raises(ConnectorError, match="no data rows"):
        empty.fetch(parse_source("gs://b/h.csv"), PROFILE)


def test_sampled_profile_never_claims_uniqueness_ranges_or_closed_value_sets():
    connector, _ = csv_connector()
    registry = ConnectorRegistry()
    registry.register(connector)
    service = RuleService(connectors=registry)
    full = service.analyze_source("gs://bkt/exports/orders.csv", ProfileOptions(enabled=True, include_values=True)).ruleset.rules
    part = service.analyze_source("gs://bkt/exports/orders.csv", ProfileOptions(enabled=True, include_values=True, max_rows=150)).ruleset.rules
    kinds = lambda rules: {(r.type, tuple(r.columns)) for r in rules if r.tier is Tier.OBSERVED}  # noqa: E731
    assert (RuleType.UNIQUE, ("order_id",)) in kinds(full) and (RuleType.RANGE, ("order_id",)) in kinds(full)
    assert (RuleType.ACCEPTED_VALUES, ("status",)) in kinds(full)
    sampled = kinds(part)
    assert not {t for t, _ in sampled} & {RuleType.UNIQUE, RuleType.RANGE, RuleType.ACCEPTED_VALUES}
    assert all(r.confidence <= 0.4 for r in part if r.tier is Tier.OBSERVED and r.type is RuleType.NOT_NULL)
