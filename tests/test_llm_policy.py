"""The constraint: data does not go to an LLM. These tests pin the controls that enforce it.

Canary strings are planted wherever a data value could hide (profile values, min/max,
patterns, descriptions, uploaded files). Every test asserts they never appear in a prompt.
"""

import json
import logging
import sys

import pytest
from conftest import FIXTURES
from fastapi.testclient import TestClient

from dqms.api.app import create_app
from dqms.auth import Authenticator
from dqms.llm.config import llm_from_env
from dqms.llm.gateway import EgressDenied, LLMGateway, LLMMode, Purpose, build_gateway
from dqms.llm.redact import redact
from dqms.models import Column, ColumnProfile, Metadata, Profile
from dqms.normalize.documents import SourceDoc
from dqms.normalize.files import UnsupportedFile, read_upload
from dqms.normalize.universal import MAX_DOCUMENT_CHARS
from dqms.rules.base import Context
from dqms.rules.llm import structural_view
from dqms.service import NoMetadataError, RuleService

CANARY = "CANARY-7731"


class Recorder:
    def __init__(self, reply=None):
        self.prompts: list[str] = []
        self.reply = {"rules": [], "metadata": None, "profile": None} if reply is None else reply

    def complete_json(self, system, user):
        self.prompts.append(user)
        return self.reply

    @property
    def sent(self) -> str:
        return "\n".join(self.prompts)


def customers():
    meta = Metadata(
        name="customers",
        columns=[
            Column(name="status", datatype="VARCHAR(20)", description="Account status, e.g. jane.doe@example.com or 123-45-6789"),
            Column(name="signup_day", datatype="DATE"),
            Column(name="amount", datatype="NUMERIC"),
            Column(name="note", datatype="STRING"),
            Column(name="birth_date", datatype="DATE", classifications=["PII"]),
        ],
    )
    profile = Profile(row_count=5000, columns=[
        ColumnProfile(column="status", null_rate=0.0, distinct_rate=0.001, distinct_count=2,
                      top_values=[f"{CANARY}-A", f"{CANARY}-B"]),
        ColumnProfile(column="signup_day", min_value="2019-03-14", max_value="2024-10-01"),
        ColumnProfile(column="birth_date", null_rate=0.1, min_value="1931-07-07", max_value="2006-02-02"),
        ColumnProfile(column="amount", min_value=-987654, max_value=123456),
        ColumnProfile(column="note", pattern=f"{CANARY}-*", min_length=1, max_length=40),
    ])
    return meta, profile


# --------------------------- off means off ---------------------------- #


def test_off_mode_has_no_gateway_and_makes_no_calls():
    rec = Recorder()
    assert build_gateway(rec, LLMMode.OFF) is None
    with pytest.raises(ValueError):
        LLMGateway(rec, LLMMode.OFF)  # a gateway in 'off' cannot even be constructed
    service = RuleService(rec, llm_mode=LLMMode.OFF)  # a client alone, without a mode, does nothing
    meta, profile = customers()
    service.analyze_models(meta, profile)
    assert rec.prompts == [] and service.llm_mode is LLMMode.OFF


def test_default_environment_is_off_and_imports_no_llm_sdk(monkeypatch):
    for var in ("DQMS_LLM_MODE", "DQMS_LLM_BASE_URL"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("BASE_URL", "https://some-unrelated-service.example")  # legacy name must not enable anything
    sys.modules.pop("openai", None)
    client, mode = llm_from_env()
    assert (client, mode) == (None, LLMMode.OFF)
    assert "openai" not in sys.modules and "azure.identity" not in sys.modules


@pytest.mark.parametrize("value", ["", "   "])
def test_empty_mode_means_unset_which_is_off(monkeypatch, value):
    monkeypatch.setenv("DQMS_LLM_MODE", value)  # templated deploy configs often leave it blank
    assert llm_from_env() == (None, LLMMode.OFF)


def test_misconfiguration_fails_loudly(monkeypatch):
    monkeypatch.setenv("DQMS_LLM_MODE", "rulez")
    with pytest.raises(RuntimeError, match="invalid"):
        llm_from_env()
    monkeypatch.setenv("DQMS_LLM_MODE", "rules")
    monkeypatch.delenv("DQMS_LLM_BASE_URL", raising=False)
    with pytest.raises(RuntimeError, match="requires DQMS_LLM_BASE_URL"):
        llm_from_env()


def test_request_can_only_narrow_never_widen():
    meta, profile = customers()
    off = Recorder()
    RuleService(off, llm_mode=LLMMode.OFF).analyze_models(meta, profile)  # even with use_llm=True by default
    assert off.prompts == []
    narrowed = Recorder()
    from dqms.rules.engine import EngineOptions

    RuleService(narrowed, llm_mode=LLMMode.RULES).analyze_models(meta, profile, EngineOptions(use_llm=False))
    assert narrowed.prompts == []


# ------------------- structural-only payloads (rules) ------------------ #


def test_statistics_reach_the_model_but_data_does_not():
    rec = Recorder()
    meta, profile = customers()
    RuleService(rec, llm_mode=LLMMode.RULES).analyze_models(meta, profile)
    assert len(rec.prompts) == 1
    assert CANARY not in rec.sent  # enumerated value lists and patterns are data: never sent
    for statistic in ("987654", "123456", "2019-03-14", "2024-10-01"):
        assert statistic in rec.sent  # min/max are profile statistics, which are fine to send
    assert "jane.doe@example.com" not in rec.sent and "123-45-6789" not in rec.sent  # typed into a description
    assert "[REDACTED:email]" in rec.sent and "[REDACTED:ssn]" in rec.sent


def test_sensitive_columns_keep_their_extremes_back():
    rec = Recorder()
    meta, profile = customers()
    RuleService(rec, llm_mode=LLMMode.RULES).analyze_models(meta, profile)
    assert "1931-07-07" not in rec.sent and "2006-02-02" not in rec.sent  # birth_date is classified PII
    birth = next(c for c in json.loads(rec.prompts[0])["columns"] if c["name"] == "birth_date")
    assert birth["null_rate"] == 0.1 and "min_value" not in birth  # its other statistics are still sent


def test_existing_rules_keep_their_params_but_never_value_lists():
    rec = Recorder()
    meta, profile = customers()
    RuleService(rec, llm_mode=LLMMode.RULES).analyze_models(meta, profile)
    existing = json.loads(rec.prompts[0])["existing_rules"]
    assert existing and all(set(r) == {"type", "columns", "params"} for r in existing)
    assert not any("values" in r["params"] for r in existing)  # observed accepted_values carry the value list
    assert any(r["type"] == "range" and r["params"].get("max") == 123456 for r in existing)


def test_payload_is_an_allowlist_so_new_model_fields_cannot_leak():
    meta, profile = customers()
    view = structural_view(Context(meta, profile), [])
    assert set(view) == {"dataset", "columns", "glossary", "row_count", "existing_rules"}
    assert set(view["columns"][0]) == {"name", "datatype", "nullable", "description", "semantic_type",
                                       "classifications", "null_rate", "distinct_rate", "distinct_count",
                                       "min_length", "max_length", "min_value", "max_value"}
    assert "top_values" not in json.dumps(view) and "pattern" not in json.dumps(view)
    assert set(view["dataset"]) == {"name", "description", "domain", "tags"}


def test_profile_statistics_in_the_payload():
    meta, profile = customers()
    payload = structural_view(Context(meta, profile), [])
    status = next(c for c in payload["columns"] if c["name"] == "status")
    assert (status["null_rate"], status["distinct_rate"], status["distinct_count"]) == (0.0, 0.001, 2)
    amount = next(c for c in payload["columns"] if c["name"] == "amount")
    assert (amount["min_value"], amount["max_value"]) == (-987654, 123456)
    assert payload["row_count"] == 5000


# --------------------- documents: mode, data, size --------------------- #

CONTRACT = "# Orders contract\nThe orders dataset has order_id (required, unique) and status (NEW, SHIPPED)."


def test_documents_are_never_sent_in_rules_mode():
    rec = Recorder()
    ddl = SourceDoc("t.sql", "CREATE TABLE t (id INT NOT NULL);")
    secret_doc = SourceDoc("notes.md", f"internal notes {CANARY} about the table")
    analysis = RuleService(rec, llm_mode=LLMMode.RULES).analyze_documents([ddl, secret_doc])
    assert {d.name: d.route for d in analysis.documents} == {"t.sql": "deterministic", "notes.md": "unrecognized"}
    assert CANARY not in rec.sent  # only the structural enrichment call may have happened


def test_documents_are_sent_only_in_full_mode_and_are_scrubbed():
    reply = {"metadata": {"name": "orders", "columns": [{"name": "order_id", "nullable": False}]}, "profile": None}
    rec = Recorder(reply)
    doc = SourceDoc("c.md", CONTRACT + "\nOwner contact: owner@corp.example, token=abcd1234efgh")
    RuleService(rec, llm_mode=LLMMode.FULL).analyze_documents([doc])
    assert "order_id" in rec.sent
    assert "owner@corp.example" not in rec.sent and "abcd1234efgh" not in rec.sent


def test_data_csv_is_not_misread_as_columns_and_never_sent():
    rec = Recorder()
    data = b"id,name,email\n" + b"".join(f"{i},{CANARY}-{i},p{i}@corp.example\n".encode() for i in range(1, 60))
    docs = read_upload("export.csv", data)
    assert isinstance(docs[0].content, str)  # kept as text, NOT turned into a column-per-value dict
    with pytest.raises(NoMetadataError) as err:
        RuleService(rec, llm_mode=LLMMode.FULL).analyze_documents(docs)
    assert "row data" in str(err.value) and rec.prompts == []


@pytest.mark.parametrize("name,body", [
    ("ledger.txt", "\n".join(f"{CANARY}-{i} | acct {1000 + i} | balance {i}.50" for i in range(40))),
    ("rows.jsonl", "\n".join(json.dumps({"id": i, "who": f"{CANARY}-{i}"}) for i in range(40))),
    ("table.md", "| id | person | city |\n|---|---|---|\n" + "\n".join(f"| {i} | {CANARY}-{i} | Paris |" for i in range(10))),
    ("rows.tsv", "id\tperson\tcity\n" + "\n".join(f"{i}\t{CANARY}-{i}\tParis" for i in range(10))),
])
def test_row_data_in_any_text_shape_is_refused_even_in_full_mode(name, body):
    rec = Recorder()
    analysis_docs = read_upload(name, body.encode())
    with pytest.raises(NoMetadataError, match="row data"):
        RuleService(rec, llm_mode=LLMMode.FULL).analyze_documents(analysis_docs)
    assert rec.prompts == []


def test_json_array_of_records_is_rejected_at_upload():
    rows = json.dumps([{"id": i, "who": f"{CANARY}-{i}"} for i in range(50)]).encode()
    with pytest.raises(UnsupportedFile, match="row data"):
        read_upload("people.json", rows)


def test_metadata_shaped_tables_still_pass_the_guard():
    rec = Recorder({"metadata": {"name": "t", "columns": [{"name": "id"}]}, "profile": None})
    table = ("| column | type | description |\n|---|---|---|\n"
             + "\n".join(f"| col{i} | INT | the {i}th column |" for i in range(8)) + "\n| id | INT | key |")
    RuleService(rec, llm_mode=LLMMode.FULL).analyze_documents([SourceDoc("schema.md", table)])
    assert rec.prompts, "a table that describes columns is metadata and may be parsed in full mode"


def test_oversized_document_is_refused_not_truncated():
    rec = Recorder()
    big = SourceDoc("huge.md", "design notes. " * (MAX_DOCUMENT_CHARS // 10))
    with pytest.raises(NoMetadataError) as err:
        RuleService(rec, llm_mode=LLMMode.FULL).analyze_documents([big])
    assert "not sent" in str(err.value) and rec.prompts == []


def test_ddl_is_never_mistaken_for_row_data():
    from dqms.normalize.rowdata import looks_like_row_data

    ddl = "CREATE TABLE t (\n  a INT,\n  b INT,\n  c INT,\n  d INT\n);"
    assert not looks_like_row_data(ddl)


# ---------------------------- the gateway ------------------------------ #


def test_gateway_enforces_purpose_size_and_logs_without_content(caplog):
    rec = Recorder()
    gateway = LLMGateway(rec, LLMMode.RULES, max_payload_chars=200)
    assert gateway.allows(Purpose.RULES) and not gateway.allows(Purpose.DOCUMENT)
    with pytest.raises(EgressDenied, match="not permitted"):
        gateway.complete_json(Purpose.DOCUMENT, "sys", "anything")
    with pytest.raises(EgressDenied, match="nothing was sent"):
        gateway.complete_json(Purpose.RULES, "sys", "x" * 500)
    assert rec.prompts == []

    with caplog.at_level(logging.INFO, logger="dqms.llm.egress"):
        gateway.complete_json(Purpose.RULES, "sys", f"{CANARY} contact bob@corp.example")
    record = next(r for r in caplog.records if r.message == "llm_egress")
    logged = " ".join(str(v) for v in record.__dict__.values())
    assert CANARY not in logged and "bob@corp.example" not in logged  # the audit trail never holds content
    assert record.purpose == "rules" and record.redactions == {"email": 1}
    assert CANARY in rec.prompts[0] and "bob@" not in rec.prompts[0]


def test_full_mode_allows_both_purposes():
    gateway = LLMGateway(Recorder(), LLMMode.FULL)
    assert gateway.allows(Purpose.RULES) and gateway.allows(Purpose.DOCUMENT)


def test_redaction():
    text = ("mail a@b.co, ssn 123-45-6789, card 4111 1111 1111 1111, not a card 4111 1111 1111 1112, "
            "acct 12345678901, AKIAABCDEFGHIJKLMNOP, password=hunter2hunter2, version 3.14, 2024-05-01, id 42")
    out, counts = redact(text)
    assert "a@b.co" not in out and "123-45-6789" not in out and "4111 1111 1111 1111" not in out
    assert "12345678901" not in out and "AKIAABCDEFGHIJKLMNOP" not in out and "hunter2hunter2" not in out
    assert "4111 1111 1111 1112" in out  # fails Luhn, so it is not a card
    assert "3.14" in out and "2024-05-01" in out and "id 42" in out  # ordinary content survives
    assert counts == {"secret": 1, "cloud_key": 1, "email": 1, "ssn": 1, "card": 1, "number": 1}


# ------------------------------ visibility ----------------------------- #


def test_effective_mode_is_visible_to_operators():
    for mode in (LLMMode.OFF, LLMMode.RULES, LLMMode.FULL):
        client = TestClient(create_app(Recorder(), mode, auth=Authenticator.anonymous()))
        assert client.get("/healthz").json()["llm_mode"] == mode.value
    # A client handed to a service without a mode is off, and says so.
    assert TestClient(create_app(Recorder(), auth=Authenticator.anonymous())).get("/healthz").json()["llm_mode"] == "off"


def test_fixtures_use_no_llm_by_default(all_docs):
    rec = Recorder()
    RuleService(rec).analyze_documents(all_docs)  # no mode given
    assert rec.prompts == []
