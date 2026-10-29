import json

import pytest
import yaml
from fakes import MemStore
from fastapi.testclient import TestClient

from dqms import cli
from dqms.api.app import create_app
from dqms.llm.gateway import LLMMode
from dqms.auth import Authenticator, generate_key, hash_key
from dqms.client import DqmsClient, DqmsError, JobFailed
from dqms.connectors.base import ConnectorRegistry
from dqms.connectors.objectstore import ObjectStoreConnector
from dqms.models import RuleSet

GOOD = b"id,name\n" + b"".join(f"{i},n{i}\n".encode() for i in range(1, 30))
OBJECTS = {("bucket", "exports/a.csv"): GOOD, ("bucket", "exports/b.csv"): GOOD,
           ("bucket", "exports/broken.csv"): b"h1,h2\n", ("bucket", "private/x.csv"): GOOD}


def registry():
    reg = ConnectorRegistry()
    reg.register(ObjectStoreConnector(store_factory=lambda s, a: MemStore(OBJECTS)))
    return reg


@pytest.fixture
def served(tmp_path):
    key, digest = generate_key()
    path = tmp_path / "auth.yaml"
    path.write_text(yaml.safe_dump({"keys": [{"name": "ci", "sha256": digest, "sources": ["gs://bucket/exports/*"]}]}))
    app = create_app(llm_mode=LLMMode.OFF, auth=Authenticator(Authenticator.load_keys(path)), connectors=registry())
    return app, key


@pytest.fixture
def client(served):
    app, key = served
    return DqmsClient("http://testserver", key, http=TestClient(app))


# ------------------------------- client ------------------------------- #


def test_client_analyzes_containers_through_jobs(client):
    results = client.analyze_sources(["gs://bucket/exports/a.csv", "gs://bucket/exports/b.csv"],
                                     profile={"enabled": True}, idempotency_key="nightly-1")
    assert [r.source for r in results] == ["gs://bucket/exports/a.csv", "gs://bucket/exports/b.csv"]
    assert all(r.ruleset.fingerprint and r.profile.row_count == 29 for r in results)


def test_partial_failure_is_visible_on_the_job_but_not_fatal(client):
    job = client.wait_job(client.submit_job(["gs://bucket/exports/"]).id)
    assert job.status.value == "partial"
    assert len(client.analyze_sources(["gs://bucket/exports/"])) == 2  # the two good files


def test_total_failure_raises(client):
    with pytest.raises(JobFailed) as err:
        client.analyze_sources(["gs://bucket/exports/missing.csv"])
    assert err.value.job.results[0].status == "error"


def test_client_surfaces_http_errors(served):
    app, _ = served
    anonymous = DqmsClient("http://testserver", http=TestClient(app))
    with pytest.raises(DqmsError) as err:
        anonymous.discover("gs://bucket/exports/")
    assert err.value.status == 401
    wrong = DqmsClient("http://testserver", "dqms_nope", http=TestClient(app))
    with pytest.raises(DqmsError) as err:
        wrong.discover("gs://bucket/exports/")
    assert err.value.status == 401


def test_client_enforces_server_side_allowlist(client):
    with pytest.raises(DqmsError) as err:
        client.analyze_source("gs://bucket/private/x.csv")
    assert err.value.status == 403
    assert client.discover("gs://bucket/exports/") == ["gs://bucket/exports/a.csv", "gs://bucket/exports/b.csv", "gs://bucket/exports/broken.csv"]


def test_client_uploads_files_and_diffs(client, tmp_path):
    ddl = tmp_path / "t.sql"
    ddl.write_text("CREATE TABLE t (id INT NOT NULL, note VARCHAR(10));")
    first = client.analyze_files([ddl])
    ddl.write_text("CREATE TABLE t (id INT NOT NULL, note VARCHAR(40));")
    second = client.analyze_files([ddl])
    d = client.diff(first.ruleset, second.ruleset)
    assert d["changed"] and d["breaking"] and d["old_fingerprint"] != d["new_fingerprint"]


def test_wait_job_times_out(client, monkeypatch):
    job = client.submit_job(["gs://bucket/exports/a.csv"])
    with pytest.raises(TimeoutError):
        client.wait_job(job.id, timeout=0)


# --------------------------------- CLI -------------------------------- #


def run(*argv):
    return cli.main(list(argv))


def test_cli_analyze_files_writes_ruleset(tmp_path, capsys):
    ddl = tmp_path / "orders.sql"
    ddl.write_text("CREATE TABLE orders (id BIGINT PRIMARY KEY, status VARCHAR(10) NOT NULL);")
    out = tmp_path / "rules.json"
    assert run("analyze", str(ddl), "--no-llm", "--out", str(out)) == 0
    ruleset = RuleSet.model_validate_json(out.read_text())
    assert ruleset.dataset == "orders" and ruleset.fingerprint and any(r.id == "orders.id.unique" for r in ruleset.rules)
    assert "OK" in capsys.readouterr().err


def test_cli_gate_on_findings(tmp_path):
    ddl = tmp_path / "t.sql"
    ddl.write_text("CREATE TABLE t (status VARCHAR(10) NOT NULL);")
    profile = tmp_path / "profile.json"
    profile.write_text(json.dumps({"row_count": 1000, "columns": [{"name": "status", "null_rate": 0.4}]}))
    assert run("analyze", str(ddl), str(profile), "--no-llm") == 0  # informational by default
    assert run("analyze", str(ddl), str(profile), "--no-llm", "--fail-on", "findings") == 1


def test_cli_local_sources_use_discovery_and_report_failures(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr("dqms.api.app.default_connectors", registry)
    out = tmp_path / "out"
    code = run("analyze", "gs://bucket/exports/", "--no-llm", "--profile", "--out", str(out))
    assert code == 2  # broken.csv could not be read: operational failure, but the rest is still produced
    assert sorted(p.name for p in out.iterdir()) == ["gs_bucket_exports_a.csv.rules.json", "gs_bucket_exports_b.csv.rules.json"]
    err = capsys.readouterr().err
    assert "FAILED  gs://bucket/exports/broken.csv" in err and "no data rows" in err
    assert run("analyze", "gs://bucket/exports/a.csv", "--no-llm") == 0


def test_cli_errors_are_exit_2_with_one_line(capsys):
    assert run("analyze", "/no/such/file.sql") == 2
    assert run("analyze", "ftp://host/x") == 2
    err = capsys.readouterr().err
    assert "no such file" in err and "no connector for 'ftp://'" in err and "Traceback" not in err


def test_cli_remote_mode(served, monkeypatch, tmp_path):
    app, key = served
    monkeypatch.setattr("dqms.client.httpx.Client", lambda **kw: TestClient(app, headers=kw.get("headers")))
    monkeypatch.setenv("DQMS_API_KEY", key)
    assert run("analyze", "gs://bucket/exports/a.csv", "--url", "http://testserver", "--profile", "--no-llm") == 0
    monkeypatch.setenv("DQMS_API_KEY", "wrong")
    assert run("analyze", "gs://bucket/exports/a.csv", "--url", "http://testserver") == 2


def test_cli_diff_gates(tmp_path):
    def ruleset(length: int) -> str:
        from dqms.rules.engine import RuleEngine
        from dqms.normalize.ddl import parse_ddl

        return RuleEngine().generate(parse_ddl(f"CREATE TABLE t (id INT NOT NULL, note VARCHAR({length}));")).model_dump_json()

    a, b, c = (tmp_path / n for n in ("a.json", "b.json", "c.json"))
    a.write_text(ruleset(10)); b.write_text(ruleset(10)); c.write_text(ruleset(99))
    assert run("diff", str(a), str(b), "--fail-on-change") == 0
    assert run("diff", str(a), str(c)) == 0  # report only
    assert run("diff", str(a), str(c), "--fail-on-change") == 1
    assert run("diff", str(a), str(c), "--fail-on-breaking") == 1


def test_cli_keygen_produces_matching_hash(capsys):
    assert run("keygen", "--name", "airflow", "--sources", "bigquery://p/*") == 0
    captured = capsys.readouterr()
    key = next(line.strip() for line in captured.err.splitlines() if line.strip().startswith("dqms_"))
    entry = yaml.safe_load(captured.out)["keys"][0]  # stdout alone is a valid key file
    assert entry["sha256"] == hash_key(key) and entry["sources"] == ["bigquery://p/*"] and key not in captured.out
