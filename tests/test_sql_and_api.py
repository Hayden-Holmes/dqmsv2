import json

import pytest
from conftest import FIXTURES, FakeLLM
from fastapi.testclient import TestClient

from dqms.api.app import create_app
from dqms.emitters.sql import emit_sql_checks
from dqms.models import Column, Metadata
from dqms.rules.engine import RuleEngine
from dqms.service import RuleService


def upload(client, *names, **form):
    files = []
    for name in names:
        path = next(FIXTURES.glob(f"{name}.txt"))
        real = "ddl.sql" if name == "ddl" else f"{name}.json"
        files.append(("files", (real, path.read_bytes())))
    return client.post("/v1/analyze", files=files, data=form)


# ------------------------------- SQL ------------------------------------ #


def test_sql_emission_for_both_dialects(all_docs):
    analysis = RuleService().analyze_documents(all_docs)
    for dialect in ("bigquery", "postgres"):
        checks = emit_sql_checks(analysis.ruleset, analysis.metadata, dialect, "proj.ds.customer")
        by_id = {c.rule_id: c for c in checks}
        assert "NOT" not in by_id["customer.customer_id.unique"].sql.split("WHERE")[0]
        assert by_id["customer.first_name.not_null"].sql.endswith("WHERE " + ('`first_name`' if dialect == "bigquery" else '"first_name"') + " IS NULL")
        assert by_id["customer.first_name.datatype"].sql is None  # schema-enforced, no row check
        assert by_id["customer.country_code.accepted_values"].sql is None  # needs a reference list
    bq = {c.rule_id: c.sql for c in emit_sql_checks(analysis.ruleset, analysis.metadata, "bigquery")}
    pg = {c.rule_id: c.sql for c in emit_sql_checks(analysis.ruleset, analysis.metadata, "postgres")}
    assert "REGEXP_CONTAINS" in bq["customer.email.regex"] and "!~" in pg["customer.email.regex"]


def test_sql_refuses_unsafe_identifiers_and_escapes_literals():
    meta = Metadata(
        name="t",
        columns=[
            Column(name="a`; DROP TABLE x; --", datatype="INT", nullable=False),
            Column(name="status", datatype="STRING", description="Allowed values: it's, \\evil"),
        ],
    )
    ruleset = RuleEngine().generate(meta)
    checks = {c.rule_id: c for c in emit_sql_checks(ruleset, meta, "bigquery")}
    bad = next(c for c in checks.values() if c.rule_id.startswith("t.a_") and c.rule_id.endswith(".not_null"))
    assert bad.sql is None and "cannot be safely quoted" in bad.note
    enum = checks["t.status.accepted_values"].sql
    assert "'it\\'s'" in enum and "'\\\\evil'" in enum
    postgres = {c.rule_id: c.sql for c in emit_sql_checks(ruleset, meta, "postgres")}
    assert "'it''s'" in postgres["t.status.accepted_values"]


# ------------------------------- API ------------------------------------ #


@pytest.fixture
def client():
    return TestClient(create_app(llm_from_env=False))


def test_health(client):
    body = client.get("/healthz").json()
    assert body["status"] == "ok" and body["llm"] is False and "bigquery" in body["connectors"]


def test_ui_is_served(client):
    res = client.get("/")
    assert res.status_code == 200 and "Drag" in res.text


def test_upload_multiple_files_returns_analysis(client):
    res = upload(client, "ddl", "profiling", "validation_metadata")
    assert res.status_code == 200
    body = res.json()
    assert body["ruleset"]["dataset"] == "customer"
    assert {d["route"] for d in body["documents"]} == {"deterministic"}
    assert body["profile"]["row_count"] == 150000


def test_upload_options_and_dataset_override(client):
    res = upload(client, "ddl", dataset_name="renamed", min_confidence="0.9")
    body = res.json()
    assert body["ruleset"]["dataset"] == "renamed"
    assert all(r["confidence"] >= 0.9 for r in body["ruleset"]["rules"])


def test_unreadable_file_is_reported_but_does_not_fail_the_batch(client):
    ddl = (FIXTURES / "ddl.txt").read_bytes()
    res = client.post("/v1/analyze", files=[("files", ("ddl.sql", ddl)), ("files", ("sheet.xlsx", b"PK\x03\x04\x00\x00"))])
    assert res.status_code == 200
    routes = {d["name"]: d["route"] for d in res.json()["documents"]}
    assert routes == {"ddl.sql": "deterministic", "sheet.xlsx": "rejected"}


def test_nothing_extractable_is_a_422_with_guidance(client):
    res = client.post("/v1/analyze", files=[("files", ("notes.md", b"meeting notes, no schema"))])
    assert res.status_code == 422 and "LLM" in res.json()["detail"]


def test_only_unreadable_files_is_a_422(client):
    res = client.post("/v1/analyze", files=[("files", ("empty.json", b""))])
    assert res.status_code == 422


def test_unknown_format_works_through_the_api_when_an_llm_is_present():
    reply = {"metadata": {"name": "orders", "columns": [{"name": "order_id", "nullable": False}]}, "profile": None}
    client = TestClient(create_app(FakeLLM(reply, {"rules": []})))
    res = client.post("/v1/analyze", files=[("files", ("contract.md", b"orders has order_id which is required"))])
    assert res.status_code == 200
    assert res.json()["documents"][0]["route"] == "llm"


def test_json_endpoint_and_sql_export_roundtrip(client):
    ddl = (FIXTURES / "ddl.txt").read_text()
    analysis = client.post("/v1/analyze/json", json={"documents": [ddl]}).json()
    res = client.post("/v1/export/sql", json={"metadata": analysis["metadata"], "ruleset": analysis["ruleset"],
                                              "dialect": "postgres", "table": "public.customer"})
    assert res.status_code == 200
    assert any(c["sql"] and '"public"."customer"' in c["sql"] for c in res.json())


def test_unknown_connector_is_a_400(client):
    res = client.post("/v1/sources/nope/analyze", json={"ref": {}})
    assert res.status_code == 400 and "available" in res.json()["detail"]


def test_bigquery_connector_validates_identifiers(client):
    res = client.post("/v1/sources/bigquery/analyze", json={"ref": {"project_id": "p", "dataset_id": "d; DROP", "table_id": "t"}})
    assert res.status_code == 400
