import yaml
import pytest
from fakes import MemStore
from fastapi.testclient import TestClient

from dqms.api.app import create_app
from dqms.llm.gateway import LLMMode
from dqms.auth import AuthError, Authenticator, generate_key, hash_key
from dqms.connectors.base import ConnectorRegistry, ProfileOptions
from dqms.connectors.objectstore import ObjectStoreConnector
from dqms.jobs import InMemoryJobStore, Job, JobStatus, SqliteJobStore
from dqms.models import Column, Metadata
from dqms.rules.diff import diff_rulesets
from dqms.rules.engine import RuleEngine

GOOD = b"id,name\n" + b"".join(f"{i},n{i}\n".encode() for i in range(1, 30))


def objects():
    return {
        ("bucket", "exports/a.csv"): GOOD,
        ("bucket", "exports/b.csv"): GOOD,
        ("bucket", "exports/broken.csv"): b"only,a,header\n",
        ("bucket", "private/secret.csv"): GOOD,
    }


@pytest.fixture
def env(tmp_path):
    ci_key, ci_hash = generate_key()
    other_key, other_hash = generate_key()
    auth_file = tmp_path / "auth.yaml"
    auth_file.write_text(yaml.safe_dump({"keys": [
        {"name": "ci", "sha256": ci_hash, "sources": ["gs://bucket/exports/*"]},
        {"name": "partial", "sha256": other_hash, "sources": ["gs://bucket/exports/", "gs://bucket/exports/a*"]},
    ]}))
    registry = ConnectorRegistry()
    registry.register(ObjectStoreConnector(store_factory=lambda s, a: MemStore(objects())))
    app = create_app(llm_mode=LLMMode.OFF, auth=Authenticator(Authenticator.load_keys(auth_file)), connectors=registry)
    return SimpleEnv(TestClient(app), {"Authorization": f"Bearer {ci_key}"}, {"Authorization": f"Bearer {other_key}"})


class SimpleEnv:
    def __init__(self, client, ci, partial):
        self.client, self.ci, self.partial = client, ci, partial


def wait(env, job_id, headers):
    res = env.client.get(f"/v1/jobs/{job_id}", params={"wait": 20}, headers=headers)
    assert res.status_code == 200
    return res.json()


# ------------------------------- auth --------------------------------- #


def test_keys_are_hashed_matched_and_scoped(tmp_path):
    key, digest = generate_key()
    assert key.startswith("dqms_") and digest == hash_key(key)
    path = tmp_path / "k.yaml"
    path.write_text(yaml.safe_dump({"keys": [{"name": "ci", "sha256": digest, "sources": ["bigquery://p/*"]}]}))
    auth = Authenticator(Authenticator.load_keys(path))
    who = auth.authenticate(f"Bearer {key}")
    assert who.name == "ci" and who.can_read("bigquery://p/ds/t") and not who.can_read("bigquery://other/ds/t")
    for bad in (None, "", "Basic abc", "Bearer ", "Bearer wrong"):
        with pytest.raises(AuthError):
            auth.authenticate(bad)


def test_invalid_key_never_degrades_to_anonymous():
    auth = Authenticator.anonymous()
    assert auth.authenticate(None).name == "anonymous" and not auth.authenticate(None).can_read("gs://b/x")
    with pytest.raises(AuthError):
        auth.authenticate("Bearer made-up")


def test_service_refuses_to_start_without_auth_config(monkeypatch):
    monkeypatch.delenv("DQMS_AUTH_FILE", raising=False)
    monkeypatch.delenv("DQMS_ALLOW_ANONYMOUS", raising=False)
    with pytest.raises(RuntimeError, match="Authentication is not configured"):
        Authenticator.from_env()


def test_malformed_key_file_is_rejected(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text(yaml.safe_dump({"keys": [{"name": "x", "sha256": "short"}]}))
    with pytest.raises(RuntimeError, match="sha256"):
        Authenticator.load_keys(path)


def test_endpoints_require_a_key(env):
    assert env.client.get("/healthz").status_code == 200  # liveness stays open
    assert env.client.get("/readyz").status_code == 200
    for method, path, body in [("post", "/v1/jobs", {"sources": ["gs://bucket/exports/"]}),
                               ("post", "/v1/sources/analyze", {"source": "gs://bucket/exports/a.csv"}),
                               ("post", "/v1/analyze/json", {"documents": ["x"]})]:
        res = getattr(env.client, method)(path, json=body)
        assert res.status_code == 401 and res.headers["www-authenticate"] == "Bearer"
        bad = getattr(env.client, method)(path, json=body, headers={"Authorization": "Bearer nope"})
        assert bad.status_code == 401


def test_request_id_is_echoed_and_untrusted_ids_replaced(env):
    ok = env.client.get("/healthz", headers={"X-Request-ID": "trace-123"})
    assert ok.headers["x-request-id"] == "trace-123"
    evil = env.client.get("/healthz", headers={"X-Request-ID": "x\nInjected: 1"})
    assert "\n" not in evil.headers["x-request-id"] and evil.headers["x-request-id"] != "x\nInjected: 1"


# --------------------------- sync sources ----------------------------- #


def test_sync_source_analysis_respects_allowlist(env):
    ok = env.client.post("/v1/sources/analyze", headers=env.ci,
                         json={"source": "gs://bucket/exports/a.csv", "profile": {"enabled": True}})
    assert ok.status_code == 200
    body = ok.json()
    assert body["source"] == "gs://bucket/exports/a.csv" and body["profile"]["row_count"] == 29
    assert body["ruleset"]["schema_version"] == "1" and len(body["ruleset"]["fingerprint"]) == 16
    denied = env.client.post("/v1/sources/analyze", headers=env.ci, json={"source": "gs://bucket/private/secret.csv"})
    assert denied.status_code == 403 and "private/secret.csv" in denied.json()["detail"]


def test_container_cannot_be_analyzed_synchronously(env):
    res = env.client.post("/v1/sources/analyze", headers=env.ci, json={"source": "gs://bucket/exports/"})
    assert res.status_code == 400 and "group of datasets" in res.json()["detail"]


def test_discovery_is_filtered_by_allowlist(env):
    res = env.client.post("/v1/sources/discover", headers=env.partial, json={"source": "gs://bucket/exports/"})
    assert [r["uri"] for r in res.json()] == ["gs://bucket/exports/a.csv"]  # b.csv / broken.csv are out of scope


# -------------------------------- jobs -------------------------------- #


def test_job_expands_container_and_reports_partial_failure(env):
    res = env.client.post("/v1/jobs", headers=env.ci, json={"sources": ["gs://bucket/exports/"],
                                                           "profile": {"enabled": True}})
    assert res.status_code == 202 and res.headers["location"] == f"/v1/jobs/{res.json()['id']}"
    job = wait(env, res.json()["id"], env.ci)
    assert job["status"] == "partial" and job["total"] == 3 and job["done"] == 3
    by_source = {r["source"]: r for r in job["results"]}
    assert by_source["gs://bucket/exports/a.csv"]["status"] == "ok"
    assert by_source["gs://bucket/exports/a.csv"]["analysis"]["ruleset"]["rules"]
    assert "no data rows" in by_source["gs://bucket/exports/broken.csv"]["error"]


def test_job_expansion_cannot_widen_access(env):
    res = env.client.post("/v1/jobs", headers=env.partial, json={"sources": ["gs://bucket/exports/"]})
    job = wait(env, res.json()["id"], env.partial)
    assert [r["source"] for r in job["results"]] == ["gs://bucket/exports/a.csv"]
    assert job["status"] == "succeeded"


def test_job_submission_denied_before_any_work(env):
    res = env.client.post("/v1/jobs", headers=env.ci, json={"sources": ["gs://bucket/private/"]})
    assert res.status_code == 403
    res = env.client.post("/v1/jobs", headers=env.ci, json={"sources": ["nonsense"]})
    assert res.status_code == 400


def test_jobs_are_private_to_their_owner(env):
    job_id = env.client.post("/v1/jobs", headers=env.ci, json={"sources": ["gs://bucket/exports/a.csv"]}).json()["id"]
    assert env.client.get(f"/v1/jobs/{job_id}", headers=env.partial).status_code == 404
    assert env.client.get("/v1/jobs/does-not-exist", headers=env.ci).status_code == 404


def test_idempotency_key_prevents_duplicate_jobs(env):
    body = {"sources": ["gs://bucket/exports/a.csv"]}
    first = env.client.post("/v1/jobs", headers={**env.ci, "Idempotency-Key": "run-42"}, json=body)
    second = env.client.post("/v1/jobs", headers={**env.ci, "Idempotency-Key": "run-42"}, json=body)
    other = env.client.post("/v1/jobs", headers={**env.partial, "Idempotency-Key": "run-42"}, json=body)
    assert (first.status_code, second.status_code) == (202, 200)
    assert first.json()["id"] == second.json()["id"] != other.json()["id"]  # keys are scoped per caller


def test_job_with_only_failing_sources_is_failed(env):
    res = env.client.post("/v1/jobs", headers=env.ci, json={"sources": ["gs://bucket/exports/missing.csv"]})
    job = wait(env, res.json()["id"], env.ci)
    assert job["status"] == "failed" and "not found" in job["results"][0]["error"]


# ------------------------------ stores -------------------------------- #


def make_job(**kw):
    return Job(id=kw.pop("id", "j1"), owner="ci", created_at="t", updated_at="t", sources=["gs://b/x.csv"], **kw)


@pytest.mark.parametrize("make_store", [lambda p: InMemoryJobStore(), lambda p: SqliteJobStore(str(p / "jobs.db"))])
def test_store_contract(make_store, tmp_path):
    store = make_store(tmp_path)
    job, created = store.create(make_job(), "key-1")
    again, created_again = store.create(make_job(id="j2"), "key-1")
    assert created and not created_again and again.id == "j1"
    job.status = JobStatus.SUCCEEDED
    store.save(job)
    assert store.get("j1").status is JobStatus.SUCCEEDED and store.get("nope") is None


def test_sqlite_survives_restart_and_fails_orphaned_jobs(tmp_path):
    path = str(tmp_path / "jobs.db")
    store = SqliteJobStore(path)
    running = make_job(id="running", status=JobStatus.RUNNING)
    store.create(running, None)
    store.create(make_job(id="done", status=JobStatus.SUCCEEDED), None)
    reopened = SqliteJobStore(path)
    assert reopened.fail_incomplete() == 1
    assert reopened.get("running").status is JobStatus.FAILED and "restarted" in reopened.get("running").error
    assert reopened.get("done").status is JobStatus.SUCCEEDED


# ------------------------------- drift -------------------------------- #


def rules_for(*columns):
    return RuleEngine().generate(Metadata(name="t", columns=list(columns)))


def test_fingerprint_is_stable_and_tracks_behaviour():
    a = rules_for(Column(name="id", datatype="INT", nullable=False))
    b = rules_for(Column(name="id", datatype="INT", nullable=False, description="same rules, new words"))
    c = rules_for(Column(name="id", datatype="INT", nullable=True))
    assert a.fingerprint == b.fingerprint != c.fingerprint


def test_diff_classifies_drift():
    old = rules_for(Column(name="id", datatype="INT", nullable=False), Column(name="note", datatype="VARCHAR(10)"))
    same = diff_rulesets(old, old)
    assert same.is_empty and not same.breaking

    widened = rules_for(Column(name="id", datatype="INT", nullable=False), Column(name="note", datatype="VARCHAR(50)"))
    d = diff_rulesets(old, widened)
    assert [c.id for c in d.changed] == ["t.note.length.max"] and d.breaking  # a declared limit changed

    # Dropping a declared NOT NULL removes the rule, unless a naming heuristic still implies it,
    # in which case it is *demoted* (declared -> inferred), which is also drift worth flagging.
    removed = diff_rulesets(rules_for(Column(name="qty", datatype="INT", nullable=False)),
                            rules_for(Column(name="qty", datatype="INT", nullable=True)))
    assert [c.id for c in removed.removed] == ["t.qty.not_null"] and removed.breaking
    demoted = diff_rulesets(old, rules_for(Column(name="id", datatype="INT", nullable=True),
                                           Column(name="note", datatype="VARCHAR(10)")))
    change = next(c for c in demoted.changed if c.id == "t.id.not_null")
    assert (change.before["tier"], change.after["tier"]) == ("declared", "inferred") and demoted.breaking

    extra = rules_for(Column(name="id", datatype="INT", nullable=False), Column(name="note", datatype="VARCHAR(10)"),
                      Column(name="code", datatype="INT", nullable=False))
    d = diff_rulesets(old, extra)
    assert {c.id for c in d.added} >= {"t.code.not_null"} and d.breaking  # new required column


def test_diff_endpoint(env):
    old = rules_for(Column(name="id", datatype="INT", nullable=False))
    new = rules_for(Column(name="id", datatype="INT", nullable=True))
    res = env.client.post("/v1/diff", headers=env.ci, json={"old": old.model_dump(mode="json"), "new": new.model_dump(mode="json")})
    body = res.json()
    assert res.status_code == 200 and body["changed"] and body["breaking"]
    assert body["old_fingerprint"] != body["new_fingerprint"]
