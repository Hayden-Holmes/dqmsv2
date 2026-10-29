"""The explorer: browse one level at a time, reading nothing, showing only what a key may read."""

from types import SimpleNamespace

import pytest
import yaml
from fakes import FakeBigQueryClient, FakeConnection, MemStore
from fastapi.testclient import TestClient

from dqms.api.app import create_app
from dqms.auth import Authenticator, Principal, explorer_roots, generate_key
from dqms.connectors.base import ConnectorError, ConnectorRegistry
from dqms.connectors.bigquery import BigQueryConnector
from dqms.connectors.databricks import DatabricksConnector
from dqms.connectors.objectstore import ObjectStoreConnector
from dqms.connectors.snowflake import SnowflakeConnector
from dqms.connectors.uri import parse_source
from dqms.llm.gateway import LLMMode

CSV = b"id,name\n1,a\n2,b\n"


def nodes(result):
    return [(n.name, n.kind) for n in result.nodes]


# ------------------------------ BigQuery ------------------------------ #


def bq_client():
    return FakeBigQueryClient(
        SimpleNamespace(), datasets={"sales": [("orders", "TABLE"), ("v_orders", "VIEW"), ("items", "TABLE")], "hr": [("staff", "TABLE")]}
    )


def test_bigquery_drill_down_reads_nothing():
    client = bq_client()
    connector = BigQueryConnector(client_factory=lambda p: client, max_bytes_billed=0)
    top = connector.browse(parse_source("bigquery://proj"))
    assert nodes(top) == [("sales", "container"), ("hr", "container")]
    tables = connector.browse(parse_source("bigquery://proj/sales"))
    assert nodes(tables) == [("orders", "dataset"), ("items", "dataset")]  # the view is not offered
    assert tables.nodes[0].uri == "bigquery://proj/sales/orders"
    assert client.sql == []  # listing calls only: not one query ran
    with pytest.raises(ConnectorError, match="no children"):
        connector.browse(parse_source("bigquery://proj/sales/orders"))


def test_bigquery_browse_truncates_and_says_so():
    connector = BigQueryConnector(client_factory=lambda p: bq_client(), max_bytes_billed=0)
    result = connector.browse(parse_source("bigquery://proj/sales"), limit=1)
    assert len(result.nodes) == 1 and result.truncated


# ------------------------------ Snowflake ------------------------------ #


def test_snowflake_drill_down():
    conn = FakeConnection(
        ("ALTER SESSION", []),
        ("SHOW DATABASES", [{"created_on": "x", "name": "ANALYTICS"}, {"created_on": "y", "name": "bad name;drop"}]),
        ("INFORMATION_SCHEMA.SCHEMATA", [{"SCHEMA_NAME": "INFORMATION_SCHEMA"}, {"SCHEMA_NAME": "PUBLIC"}, {"SCHEMA_NAME": "MART"}]),
        ("INFORMATION_SCHEMA.TABLES", [{"TABLE_SCHEMA": "MART", "TABLE_NAME": "ORDERS", "ROW_COUNT": 10, "BYTES": 99}]),
    )
    connector = SnowflakeConnector(connection_factory=lambda account: conn)
    assert nodes(connector.browse(parse_source("snowflake://acct"))) == [("ANALYTICS", "container")]  # unsafe name not offered
    assert nodes(connector.browse(parse_source("snowflake://acct/ANALYTICS"))) == [("PUBLIC", "container"), ("MART", "container")]
    tables = connector.browse(parse_source("snowflake://acct/ANALYTICS/MART"))
    assert [(n.name, n.rows, n.bytes) for n in tables.nodes] == [("ORDERS", 10, 99)]
    assert not any("SELECT *" in s or " FROM \"ANALYTICS\".\"MART\"" in s for s in conn.sql)  # no table was queried
    with pytest.raises(ConnectorError, match="no children"):
        connector.browse(parse_source("snowflake://acct/ANALYTICS/MART/ORDERS"))


# ------------------------------ Databricks ----------------------------- #


def test_databricks_drill_down():
    conn = FakeConnection(
        ("SHOW CATALOGS", [{"catalog": "main"}, {"catalog": "samples"}]),
        ("information_schema.schemata", [{"schema_name": "information_schema"}, {"schema_name": "hr"}]),
        ("information_schema.tables", [{"table_schema": "hr", "table_name": "people"}]),
    )
    connector = DatabricksConnector(connection_factory=lambda host: conn)
    assert nodes(connector.browse(parse_source("databricks://h.example.com"))) == [("main", "container"), ("samples", "container")]
    assert nodes(connector.browse(parse_source("databricks://h.example.com/main"))) == [("hr", "container")]
    assert nodes(connector.browse(parse_source("databricks://h.example.com/main/hr"))) == [("people", "dataset")]


# ----------------------------- object stores --------------------------- #


def store_connector():
    objects = {
        ("bkt", "a/x.csv"): CSV, ("bkt", "a/readme.md"): b"docs", ("bkt", "a/sub/y.parquet"): b"PAR1....",
        ("bkt", "a/sub/deeper/z.jsonl"): b'{"a":1}\n', ("bkt", "a/docs/notes.md"): b"notes", ("bkt", "a/empty/z.csv"): b"",
        ("bkt", "top.csv"): CSV, ("data", "in/q.csv"): CSV,
    }
    store = MemStore(objects)
    return ObjectStoreConnector(store_factory=lambda s, a: store), store


def test_object_store_shows_one_folder_level_with_only_data_folders():
    connector, store = store_connector()
    root = connector.browse(parse_source("gs://bkt"))
    assert nodes(root) == [("a/", "container"), ("top.csv", "dataset")]
    level = connector.browse(parse_source("gs://bkt/a/"))
    assert nodes(level) == [("sub/", "container"), ("x.csv", "dataset")]  # docs/ and empty/ hold no data files
    assert level.nodes[0].uri == "gs://bkt/a/sub/" and level.nodes[1].uri == "gs://bkt/a/x.csv" and level.nodes[1].bytes == len(CSV)
    assert nodes(connector.browse(parse_source("gs://bkt/a/sub"))) == [("deeper/", "container"), ("y.parquet", "dataset")]
    assert store.reads == []  # listing only


def test_object_store_azure_layout_and_errors():
    connector, _ = store_connector()
    assert nodes(connector.browse(parse_source("az://acct/data/in/"))) == [("q.csv", "dataset")]
    with pytest.raises(ConnectorError, match="include the container"):
        connector.browse(parse_source("az://acct"))
    with pytest.raises(ConnectorError, match="no children"):
        connector.browse(parse_source("gs://bkt/a/x.csv"))


# ------------------------------ allowlist ------------------------------ #


def test_roots_come_from_the_deepest_literal_prefix_of_each_pattern():
    assert explorer_roots([
        "bigquery://my-proj/*", "gs://b/sales/orders_*", "bigquery://p/d/t", "gs://bucket/exports/",
        "snowflake://acct/DB/PUB/*", "bigquery://*", "gs://*/x",
    ]) == ["bigquery://my-proj", "bigquery://p/d", "gs://b/sales", "gs://bucket/exports", "snowflake://acct/DB/PUB"]
    assert explorer_roots([]) == []


def test_can_browse_follows_the_allowlist_without_allowing_siblings():
    who = Principal("k", ("bigquery://proj/sales/*",))
    assert who.can_browse("bigquery://proj") and who.can_browse("bigquery://proj/sales")
    assert not who.can_browse("bigquery://proj/hr") and not who.can_browse("bigquery://other")
    assert who.can_read("bigquery://proj/sales/orders") and not who.can_read("bigquery://proj/hr/staff")
    assert not Principal("anon").can_browse("bigquery://proj")


# --------------------------------- API ---------------------------------- #


@pytest.fixture
def api(tmp_path):
    key, digest = generate_key()
    path = tmp_path / "auth.yaml"
    path.write_text(yaml.safe_dump({"keys": [{"name": "ci", "sha256": digest, "sources": ["bigquery://proj/sales/*"]}]}))
    client = bq_client()
    registry = ConnectorRegistry()
    registry.register(BigQueryConnector(client_factory=lambda p: client, max_bytes_billed=0))
    app = create_app(llm_mode=LLMMode.OFF, auth=Authenticator(Authenticator.load_keys(path)), connectors=registry)
    return SimpleNamespace(http=TestClient(app), headers={"Authorization": f"Bearer {key}"}, bq=client)


def browse(api, uri, headers=None, **extra):
    return api.http.post("/v1/explorer/browse", json={"uri": uri, **extra}, headers=api.headers if headers is None else headers)


def test_roots_endpoint(api):
    body = api.http.get("/v1/explorer/roots", headers=api.headers).json()
    assert body["roots"] == ["bigquery://proj/sales"] and body["max_datasets"] >= 1
    assert api.http.get("/v1/explorer/roots").status_code == 401


def test_browse_never_reveals_what_the_key_cannot_read(api):
    top = browse(api, "bigquery://proj").json()
    assert [n["name"] for n in top["nodes"]] == ["sales"]  # 'hr' exists but is invisible to this key
    tables = browse(api, "bigquery://proj/sales").json()
    assert [n["name"] for n in tables["nodes"]] == ["orders", "items"]
    assert browse(api, "bigquery://proj/hr").status_code == 403
    assert browse(api, "bigquery://other").status_code == 403
    assert api.bq.sql == []  # exploring ran no queries


def test_browse_requires_auth_and_valid_uris(api):
    assert browse(api, "bigquery://proj", headers={}).status_code == 401
    assert browse(api, "not-a-uri").status_code == 400
    assert browse(api, "bigquery://proj/sales/orders").status_code == 400  # a table has no children
    anon = TestClient(create_app(llm_mode=LLMMode.OFF, auth=Authenticator.anonymous(), connectors=ConnectorRegistry()))
    assert anon.get("/v1/explorer/roots").json()["roots"] == []
    assert anon.post("/v1/explorer/browse", json={"uri": "bigquery://proj"}).status_code == 403


def test_healthz_advertises_the_dataset_cap(api):
    assert api.http.get("/healthz").json()["max_datasets"] >= 1
