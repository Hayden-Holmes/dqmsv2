"""Scope: a request touches only what was asked for, and nothing is read until that is settled."""

import pytest
import yaml
from fakes import FakeBigQueryClient, FakeConnection, MemStore, bq_field
from fastapi.testclient import TestClient
from types import SimpleNamespace

from dqms import cli, scope
from dqms.api.app import create_app
from dqms.auth import Authenticator, generate_key
from dqms.client import DqmsClient
from dqms.connectors.base import ConnectorRegistry, ProfileOptions
from dqms.connectors.bigquery import BigQueryConnector
from dqms.connectors.objectstore import ObjectStoreConnector
from dqms.connectors.snowflake import SnowflakeConnector
from dqms.connectors.uri import parse_source
from dqms.llm.gateway import LLMMode
from dqms.scope import ScopeRequest, plan_scope
from dqms.service import RuleService

CSV = b"id,name\n" + b"".join(f"{i},n{i}\n".encode() for i in range(1, 30))


def many(count: int, prefix="exports/t", **extra) -> dict:
    objects = {("bkt", f"{prefix}{i:03d}.csv"): CSV for i in range(count)}
    objects.update(extra)
    return objects


def service_over(objects: dict) -> tuple[RuleService, MemStore]:
    store = MemStore(objects)
    registry = ConnectorRegistry()
    registry.register(ObjectStoreConnector(store_factory=lambda s, a: store))
    return RuleService(connectors=registry), store


# ------------------------------ planning ------------------------------ #


def test_plan_lists_datasets_and_sizes_without_reading_any_data():
    service, store = service_over(many(5))
    plan = plan_scope(service, ["gs://bkt/exports/"], cap=50)
    assert [d.name for d in plan.datasets] == [f"t{i:03d}.csv" for i in range(5)]
    assert plan.within_limit and plan.limit == 50 and plan.total_bytes == 5 * len(CSV)
    assert store.reads == []  # listing only; not one byte of a file was fetched


def test_over_the_cap_is_an_error_not_a_truncation():
    service, store = service_over(many(30))
    plan = plan_scope(service, ["gs://bkt/exports/"], cap=10)
    assert not plan.within_limit and len(plan.datasets) == 30  # we report the truth, not the first 10
    assert "30 datasets, over the limit of 10" in plan.message and "include/exclude" in plan.message
    assert "Nothing was read" in plan.message and store.reads == []


def test_a_request_can_lower_the_cap_but_never_raise_it():
    service, _ = service_over(many(8))
    assert plan_scope(service, ["gs://bkt/exports/"], ScopeRequest(max_datasets=3), cap=10).limit == 3
    greedy = plan_scope(service, ["gs://bkt/exports/"], ScopeRequest(max_datasets=1000), cap=5)
    assert greedy.limit == 5 and not greedy.within_limit  # asked for 1000, still held to the server cap


def test_include_and_exclude_narrow_containers_by_name_or_uri():
    service, _ = service_over(many(6, prefix="exports/orders_") | many(4, prefix="exports/tmp_") |
                              {("bkt", "exports/customers.csv"): CSV})
    names = lambda scope_req: sorted(d.name for d in plan_scope(service, ["gs://bkt/exports/"], scope_req, cap=50).datasets)  # noqa: E731
    assert names(ScopeRequest(include=["customers.csv"])) == ["customers.csv"]
    assert len(names(ScopeRequest(include=["orders_*"]))) == 6
    assert len(names(ScopeRequest(exclude=["tmp_*"]))) == 7
    assert names(ScopeRequest(include=["gs://bkt/exports/orders_000*"])) == ["orders_000.csv"]  # URI globs work too
    plan = plan_scope(service, ["gs://bkt/exports/"], ScopeRequest(include=["orders_*"], exclude=["*_003*"]), cap=50)
    assert len(plan.datasets) == 5 and plan.excluded == 6  # 4 tmp + customers + the one excluded orders file


def test_explicit_datasets_bypass_patterns_and_duplicates_count_once():
    service, _ = service_over(many(3))
    plan = plan_scope(service, ["gs://bkt/exports/t000.csv", "gs://bkt/exports/"],
                      ScopeRequest(include=["t002*"]), cap=50)
    assert sorted(d.name for d in plan.datasets) == ["t000.csv", "t002.csv"]  # named dataset kept; container filtered
    again = plan_scope(service, ["gs://bkt/exports/t000.csv", "gs://bkt/exports/t000.csv"], cap=50)
    assert len(again.datasets) == 1


def test_allowlist_hides_datasets_from_the_count_and_the_excluded_tally():
    service, _ = service_over(many(10))
    plan = plan_scope(service, ["gs://bkt/exports/"], ScopeRequest(exclude=["t00[0-4]*"]),
                      allow=lambda uri: uri.endswith(("t005.csv", "t006.csv", "t007.csv")), cap=50)
    assert [d.name for d in plan.datasets] == ["t005.csv", "t006.csv", "t007.csv"]
    assert plan.excluded == 0  # the caller cannot learn that other datasets exist


def test_enumeration_ceiling_asks_for_a_narrower_source(monkeypatch):
    monkeypatch.setattr(scope, "DISCOVERY_LIMIT", 10)
    service, _ = service_over(many(40))
    plan = plan_scope(service, ["gs://bkt/exports/"], cap=50)
    assert not plan.within_limit and plan.datasets == [] and "more than 10 datasets" in plan.message


def test_unreadable_sources_are_reported_alongside_the_rest():
    service, _ = service_over(many(2))
    plan = plan_scope(service, ["gs://bkt/exports/", "ftp://nope/x", "not-a-uri"], cap=50)
    assert len(plan.datasets) == 2 and {p.source for p in plan.problems} == {"ftp://nope/x", "not-a-uri"}


# ------------------------- profiling is opt-in -------------------------- #


def test_bigquery_runs_no_queries_unless_profiling_is_requested():
    table = SimpleNamespace(table_id="t", description=None, labels={}, table_constraints=None,
                            schema=[bq_field("id", "INTEGER")])
    client = FakeBigQueryClient(table, [("COUNT(*) AS n FROM", [{"n": 10}]),
                                        ("AS _rows", [{"_rows": 10, "n0": 0, "d0": 10, "lo0": 1, "hi0": 10}])])
    registry = ConnectorRegistry()
    registry.register(BigQueryConnector(client_factory=lambda p: client, max_bytes_billed=0))
    service = RuleService(connectors=registry)
    service.analyze_source("bigquery://p/d/t")  # defaults
    assert ProfileOptions().enabled is False and client.sql == []
    service.analyze_source("bigquery://p/d/t", ProfileOptions(enabled=True))
    assert client.sql, "profiling queries run only when asked for"


def test_snowflake_listing_carries_size_hints_for_planning():
    conn = FakeConnection(("ALTER SESSION", []), ("INFORMATION_SCHEMA.TABLES", [
        {"TABLE_SCHEMA": "PUB", "TABLE_NAME": "ORDERS", "ROW_COUNT": 1_000_000, "BYTES": 5_000_000},
        {"TABLE_SCHEMA": "PUB", "TABLE_NAME": "TINY", "ROW_COUNT": None, "BYTES": None}]))
    registry = ConnectorRegistry()
    registry.register(SnowflakeConnector(connection_factory=lambda a: conn))
    plan = plan_scope(RuleService(connectors=registry), ["snowflake://a/DB/PUB"], cap=50)
    assert [(d.name, d.rows, d.bytes) for d in plan.datasets] == [("ORDERS", 1_000_000, 5_000_000), ("TINY", None, None)]
    assert plan.total_rows == 1_000_000 and plan.total_bytes == 5_000_000


# --------------------------------- API ---------------------------------- #


@pytest.fixture
def api(tmp_path, monkeypatch):
    monkeypatch.setenv("DQMS_MAX_DATASETS", "4")
    key, digest = generate_key()
    path = tmp_path / "auth.yaml"
    path.write_text(yaml.safe_dump({"keys": [{"name": "ci", "sha256": digest, "sources": ["gs://bkt/exports/*"]}]}))
    service, store = service_over(many(12) | {("bkt", "private/secret.csv"): CSV})
    app = create_app(llm_mode=LLMMode.OFF, auth=Authenticator(Authenticator.load_keys(path)),
                     connectors=service.connectors)
    return SimpleNamespace(client=TestClient(app), headers={"Authorization": f"Bearer {key}"}, store=store, app=app, key=key)


def test_plan_endpoint_is_a_dry_run_bounded_by_the_allowlist(api):
    res = api.client.post("/v1/plan", headers=api.headers, json={"sources": ["gs://bkt/exports/"]})
    body = res.json()
    assert res.status_code == 200 and len(body["datasets"]) == 12 and body["limit"] == 4 and not body["within_limit"]
    assert api.store.reads == []
    assert api.client.post("/v1/plan", headers=api.headers, json={"sources": ["gs://bkt/private/"]}).status_code == 403
    assert api.client.post("/v1/plan", json={"sources": ["gs://bkt/exports/"]}).status_code == 401


def test_job_over_the_server_cap_fails_fast_and_reads_nothing(api):
    job_id = api.client.post("/v1/jobs", headers=api.headers, json={"sources": ["gs://bkt/exports/"],
                             "profile": {"enabled": True}}).json()["id"]
    job = api.client.get(f"/v1/jobs/{job_id}", params={"wait": 10}, headers=api.headers).json()
    assert job["status"] == "failed" and job["results"] == [] and job["limit"] == 4
    assert "12 datasets, over the limit of 4" in job["error"]
    assert api.store.reads == []  # not a single file was opened for a job that was never going to be allowed


def test_job_scope_selects_a_subset_within_the_cap(api):
    body = {"sources": ["gs://bkt/exports/"], "scope": {"include": ["t00[0-2]*"]}, "profile": {"enabled": True}}
    job_id = api.client.post("/v1/jobs", headers=api.headers, json=body).json()["id"]
    job = api.client.get(f"/v1/jobs/{job_id}", params={"wait": 20}, headers=api.headers).json()
    assert job["status"] == "succeeded" and job["total"] == 3 and job["excluded"] == 9
    assert sorted(r["source"].rsplit("/", 1)[1] for r in job["results"]) == ["t000.csv", "t001.csv", "t002.csv"]


def test_job_whose_scope_matches_nothing_says_why(api):
    body = {"sources": ["gs://bkt/exports/"], "scope": {"include": ["no_such_*"]}}
    job_id = api.client.post("/v1/jobs", headers=api.headers, json=body).json()["id"]
    job = api.client.get(f"/v1/jobs/{job_id}", params={"wait": 10}, headers=api.headers).json()
    assert job["status"] == "failed" and "matched no datasets (12 excluded" in job["error"]
    assert api.store.reads == []


def test_client_plan_and_scoped_analysis(api):
    client = DqmsClient("http://testserver", api.key, http=api.client)
    plan = client.plan(["gs://bkt/exports/"], include=["t00*"])
    assert len(plan.datasets) == 10 and plan.limit == 4 and not plan.within_limit  # 10 matches > cap of 4
    results = client.analyze_sources(["gs://bkt/exports/"], include=["t001*"])
    assert [r.source for r in results] == ["gs://bkt/exports/t001.csv"]


# --------------------------------- CLI ---------------------------------- #


def test_cli_plan_is_a_dry_run(monkeypatch, capsys):
    service, store = service_over(many(6))
    monkeypatch.setattr("dqms.api.app.default_connectors", lambda: service.connectors)
    assert cli.main(["analyze", "gs://bkt/exports/", "--plan", "--include", "t00[0-2]*"]) == 0
    captured = capsys.readouterr()
    assert captured.out.count("gs://bkt/exports/t00") == 3 and "nothing was read" in captured.err
    assert store.reads == []


def test_cli_refuses_an_oversized_scope_before_reading_anything(monkeypatch, capsys):
    service, store = service_over(many(30))
    monkeypatch.setattr("dqms.api.app.default_connectors", lambda: service.connectors)
    monkeypatch.setenv("DQMS_MAX_DATASETS", "5")
    assert cli.main(["analyze", "gs://bkt/exports/", "--profile", "--no-llm"]) == 2
    err = capsys.readouterr().err
    assert "30 datasets, over the limit of 5" in err and store.reads == []
    assert cli.main(["analyze", "gs://bkt/exports/", "--plan"]) == 2  # a plan over the limit is also non-zero
    assert cli.main(["analyze", "gs://bkt/exports/", "--max-datasets", "5", "--include", "t00[0-4]*", "--no-llm"]) == 0


def test_cli_exclude_and_empty_scope(monkeypatch, capsys):
    service, store = service_over(many(4))
    monkeypatch.setattr("dqms.api.app.default_connectors", lambda: service.connectors)
    assert cli.main(["analyze", "gs://bkt/exports/", "--exclude", "t00[1-3]*", "--plan"]) == 0
    assert capsys.readouterr().out.count("gs://bkt/exports/t0") == 1
    store.reads.clear()
    assert cli.main(["analyze", "gs://bkt/exports/", "--include", "nothing*", "--no-llm"]) == 2
    assert "matched no datasets" in capsys.readouterr().err and store.reads == []


def test_cli_plan_rejects_files(tmp_path, capsys):
    f = tmp_path / "x.sql"
    f.write_text("CREATE TABLE t (id INT);")
    assert cli.main(["analyze", str(f), "--plan"]) == 2
    assert "--plan applies to cloud sources" in capsys.readouterr().err
