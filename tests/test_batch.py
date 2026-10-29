"""Data files uploaded with their metadata: classification, pairing, reconciliation, and privacy."""

import io
import json
import logging

import pytest
from fastapi.testclient import TestClient

from dqms import cli
from dqms.api.app import create_app
from dqms.auth import Authenticator
from dqms.batch import MAX_BATCH_BYTES, analyze_uploads, classify, read_uploads
from dqms.connectors.base import ConnectorRegistry
from dqms.llm.gateway import LLMMode
from dqms.models import RuleType, Tier
from dqms.normalize.documents import ingest_tables
from dqms.normalize.files import read_upload
from dqms.service import NoMetadataError, RuleService

CANARY = "CANARY-5519"


def orders_csv(rows=200, dirty=False):
    body = "order_id,status,amount,placed\n" + "".join(
        f"{i},{['NEW', 'SHIPPED'][i % 2]},{i * 3}.5,2024-05-{1 + i % 28:02d}\n" for i in range(1, rows + 1))
    return (body + ("201,NEW,abc,2024-05-01\n" if dirty else "")).encode()


# `segment` has two distinct values, so it is the column where a value list WOULD be collected.
CUSTOMERS_CSV = ("id,email,notes,segment\n" + "".join(f"{i},c{i}@x.io,{CANARY}-{i},{CANARY}-SEG{i % 2}\n" for i in range(1, 60))).encode()
DDL = b"""CREATE TABLE orders (order_id BIGINT PRIMARY KEY, status VARCHAR(10) NOT NULL, amount NUMERIC NOT NULL, placed DATE, note VARCHAR(20));
CREATE TABLE customers (id BIGINT PRIMARY KEY, email VARCHAR(100) NOT NULL, notes VARCHAR(200), segment VARCHAR(40));
COMMENT ON COLUMN orders.status IS 'Allowed values: NEW, SHIPPED';"""


def run(files, service=None, **kw):
    uploads, rejected = read_uploads(files)
    return analyze_uploads(service or RuleService(), uploads, **kw), uploads, rejected


def by_name(batch):
    return {a.metadata.name: a for a in batch.analyses}


class Recorder:
    def __init__(self):
        self.prompts = []

    def complete_json(self, system, user):
        self.prompts.append(user)
        return {"rules": [], "metadata": None, "profile": None}


# --------------------------- classification --------------------------- #


def test_files_are_classified_by_content_not_just_extension():
    kinds = lambda name, data: classify(name, data).kind  # noqa: E731
    assert kinds("orders.csv", orders_csv()) == "data"  # header IS the columns
    assert kinds("cols.csv", b"table_name,column_name,data_type\na,id,INT\n") == "metadata"  # header describes columns
    assert kinds("t.parquet", b"PAR1") == "data"
    assert kinds("schema.sql", DDL) == "metadata"
    assert kinds("rows.jsonl", b'{"id":1,"who":"x"}\n{"id":2,"who":"y"}\n') == "data"
    assert kinds("contract.jsonl", b'{"column_name":"id","data_type":"INT"}\n') == "metadata"
    array = json.dumps([{"id": i, "who": f"p{i}"} for i in range(5)]).encode()
    assert kinds("rows.json", array) == "data" and classify("rows.json", array).ext == "jsonl"
    assert kinds("catalog.json", b'{"table_name":"a","columns":[{"column_name":"id"}]}') == "metadata"


def test_unreadable_files_are_rejected_not_fatal():
    uploads, rejected = read_uploads([("orders.csv", orders_csv()), ("sheet.xlsx", b"PK\x03\x04\x00\x00"), ("empty.sql", b" ")])
    assert [u.name for u in uploads] == ["orders.csv"] and {r.name for r in rejected} == {"sheet.xlsx", "empty.sql"}


# --------------------------- multi-table metadata ---------------------------- #


def test_one_metadata_file_can_describe_many_tables():
    tables = ingest_tables(read_upload("schema.sql", DDL)).tables
    assert {k: [c.name for c in m.columns] for k, m in tables.items()} == {
        "orders": ["order_id", "status", "amount", "placed", "note"], "customers": ["id", "email", "notes", "segment"]}
    assert tables["orders"].column("status").description == "Allowed values: NEW, SHIPPED"  # comment reached the right table


def test_catalog_csv_with_a_table_name_column_splits_into_tables():
    csv = b"table_name,column_name,data_type,is_nullable\norders,id,INT,NO\norders,x,INT,YES\ncustomers,id,VARCHAR,NO\n"
    tables = ingest_tables(read_upload("information_schema.csv", csv)).tables
    assert [c.name for c in tables["orders"].columns] == ["id", "x"] and tables["customers"].column("id").nullable is False


def test_json_tables_list_and_schema_qualified_names():
    doc = b'{"dataset":"d","tables":[{"table_name":"Public.Orders","columns":[{"column_name":"id","data_type":"INT"}]}]}'
    owner = b'{"asset_name":"orders","owner":"Sales Team"}'
    tables = ingest_tables(read_upload("a.json", doc) + read_upload("b.json", owner)).tables
    assert list(tables) == ["orders"] and tables["orders"].owner == "Sales Team"  # same table despite schema/case


def test_nameless_metadata_is_applied_only_when_unambiguous():
    sem = b'{"column_semantics":[{"column":"id","semantic_type":"identifier"}]}'
    one = ingest_tables(read_upload("t.sql", b"CREATE TABLE a (id INT);") + read_upload("sem.json", sem))
    assert one.tables["a"].column("id").semantic_type == "identifier"
    two = ingest_tables(read_upload("t.sql", b"CREATE TABLE a (id INT); CREATE TABLE b (id INT);") + read_upload("sem.json", sem))
    assert two.tables["a"].column("id").semantic_type is None and "does not say which table" in two.notes[0]


def test_single_table_endpoint_refuses_to_silently_drop_extra_tables():
    with pytest.raises(NoMetadataError, match="describes 2 tables"):
        RuleService().analyze_documents(read_upload("schema.sql", DDL))


# --------------------------------- pairing ---------------------------------- #


def test_table_plus_metadata_pair_by_file_name():
    batch, *_ = run([("orders.csv", orders_csv()), ("schema.sql", DDL)])
    orders = by_name(batch)["orders"]
    assert orders.profile.row_count == 200 and orders.profile.table == "orders"
    assert any(p.table == "orders" and p.data_file == "orders.csv" for p in batch.pairing)
    # the metadata is authoritative: declared rules come from it, observed rules from the data's profile
    tiers = {(r.type, r.tier) for r in orders.ruleset.rules if r.columns == ["order_id"]}
    assert (RuleType.UNIQUE, Tier.DECLARED) in tiers
    assert any(r.type is RuleType.ACCEPTED_VALUES and r.tier is Tier.DOCUMENTED for r in orders.ruleset.rules)
    customers = by_name(batch)["customers"]
    assert customers.profile is None and next(p for p in batch.pairing if p.table == "customers").note.startswith("no data file")


def test_dataset_of_several_data_files_with_one_metadata_file():
    batch, *_ = run([("orders.csv", orders_csv()), ("customers.csv", CUSTOMERS_CSV), ("schema.sql", DDL)])
    assert {a.metadata.name: a.profile.row_count for a in batch.analyses} == {"orders": 200, "customers": 59}


def test_a_lone_data_file_and_lone_table_pair_even_if_names_differ():
    batch, *_ = run([("export_2024-05.csv", CUSTOMERS_CSV), ("c.sql", b"CREATE TABLE customers (id BIGINT, email VARCHAR(100), notes TEXT);")])
    assert [a.metadata.name for a in batch.analyses] == ["customers"] and batch.analyses[0].profile.row_count == 59


def test_explicit_pairs_override_naming():
    files = [("a.csv", orders_csv()), ("b.csv", CUSTOMERS_CSV), ("schema.sql", DDL)]
    batch, *_ = run(files, pairs={"a.csv": "customers", "b.csv": "orders"})
    assert by_name(batch)["customers"].profile.row_count == 200 and by_name(batch)["orders"].profile.row_count == 59


def test_unmatched_data_is_analyzed_on_its_own_and_says_so():
    batch, *_ = run([("mystery.csv", CUSTOMERS_CSV), ("orders.csv", orders_csv()), ("schema.sql", DDL)])
    assert "mystery" in by_name(batch)
    row = next(p for p in batch.pairing if p.data_file == "mystery.csv")
    assert "no matching table" in row.note and "customers" in row.note and "orders" in row.note
    bare, *_ = run([("lonely.csv", CUSTOMERS_CSV)])
    assert "no metadata was provided" in bare.pairing[0].note and bare.analyses[0].profile.row_count == 59


def test_nothing_usable_is_an_error():
    uploads, _ = read_uploads([("empty.csv", b"a,b\n")])
    with pytest.raises(NoMetadataError):
        analyze_uploads(RuleService(), uploads)


# ------------------------- metadata vs. what the data shows ------------------------- #


def test_data_is_checked_against_the_metadata():
    batch, *_ = run([("orders.csv", orders_csv(dirty=True)), ("schema.sql", DDL)])
    findings = {(f.kind, f.column) for f in by_name(batch)["orders"].ruleset.findings}
    assert ("type_mismatch", "amount") in findings  # declared NUMERIC; the data holds 'abc'
    assert ("column_missing_from_data", "note") in findings  # described, but absent from the file


def test_columns_only_in_the_data_are_added_and_flagged():
    ddl = b"CREATE TABLE orders (order_id BIGINT PRIMARY KEY);"
    batch, *_ = run([("orders.csv", orders_csv()), ("o.sql", ddl)])
    orders = batch.analyses[0]
    assert {c.name for c in orders.metadata.columns} == {"order_id", "status", "amount", "placed"}
    assert orders.metadata.column("amount").datatype == "FLOAT"  # inferred from the data
    assert any(f.kind == "column_not_in_metadata" and f.column == "status" for f in orders.ruleset.findings)


def test_metadata_spelling_wins_for_column_names_and_statistics_line_up():
    ddl = b'CREATE TABLE orders ("Order_ID" BIGINT PRIMARY KEY, status VARCHAR(10), amount NUMERIC, placed DATE);'
    batch, *_ = run([("orders.csv", orders_csv()), ("o.sql", ddl)])
    stats = batch.analyses[0].profile
    assert stats.column("Order_ID") is not None and stats.column("order_id").column == "Order_ID"
    assert not any(f.kind == "column_not_in_metadata" for f in batch.analyses[0].ruleset.findings)


def test_truncated_data_is_marked_sampled_and_claims_no_uniqueness():
    batch, *_ = run([("orders.csv", orders_csv()), ("schema.sql", DDL)], truncated={"orders.csv"})
    orders = by_name(batch)["orders"]
    assert orders.profile.sampled and any("first part of the file" in n for n in orders.profile.notes)
    assert not any(r.tier is Tier.OBSERVED and r.type in {RuleType.UNIQUE, RuleType.RANGE} for r in orders.ruleset.rules)


def test_parquet_data_pairs_with_metadata():
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    buf = io.BytesIO()
    pq.write_table(pa.table({"id": pa.array(range(1, 301), pa.int64()), "email": [f"e{i}@x.io" for i in range(300)]}), buf)
    ddl = b"CREATE TABLE people (id BIGINT PRIMARY KEY, email VARCHAR(100) NOT NULL);"
    batch, *_ = run([("people.parquet", buf.getvalue()), ("people.sql", ddl)])
    assert batch.analyses[0].profile.row_count == 300 and batch.analyses[0].profile.column("id").max_value == 300


# ------------------------ the data never goes anywhere ------------------------- #


@pytest.mark.parametrize("mode", [LLMMode.RULES, LLMMode.FULL])
@pytest.mark.parametrize("include_values", [False, True])
def test_uploaded_data_never_reaches_an_llm(mode, include_values):
    rec = Recorder()
    service = RuleService(rec, llm_mode=mode)
    run([("customers.csv", CUSTOMERS_CSV), ("schema.sql", DDL)], service=service, include_values=include_values)
    assert rec.prompts, "the structural enrichment call should have happened"
    sent = "\n".join(rec.prompts)
    assert CANARY not in sent and "c17@x.io" not in sent  # not a row value, not a value list (even when collected)
    assert "row_count" in sent  # statistics are fine


def test_value_lists_really_are_collected_in_this_scenario():
    """Guards the canary tests: without this, 'no canary in the prompt' could pass because nothing was collected."""
    batch, *_ = run([("customers.csv", CUSTOMERS_CSV), ("schema.sql", DDL)], include_values=True)
    segment = next(c for c in by_name(batch)["customers"].profile.columns if c.column == "segment")
    assert segment.top_values == [f"{CANARY}-SEG0", f"{CANARY}-SEG1"]


def test_the_response_and_logs_carry_aggregates_never_rows(caplog):
    client = TestClient(create_app(llm_mode=LLMMode.OFF, auth=Authenticator.anonymous(), connectors=ConnectorRegistry()))
    with caplog.at_level(logging.DEBUG):
        res = client.post("/v1/analyze/batch", files=[("files", ("customers.csv", CUSTOMERS_CSV)), ("files", ("schema.sql", DDL))])
    assert res.status_code == 200
    assert CANARY not in res.text and "c17@x.io" not in res.text  # by default no value lists are returned either
    assert CANARY not in caplog.text and "c17@x.io" not in caplog.text


def test_value_lists_are_returned_only_when_asked_for():
    client = TestClient(create_app(llm_mode=LLMMode.OFF, auth=Authenticator.anonymous(), connectors=ConnectorRegistry()))
    files = [("files", ("orders.csv", orders_csv())), ("files", ("schema.sql", DDL))]
    plain = client.post("/v1/analyze/batch", files=files).json()
    asked = client.post("/v1/analyze/batch", files=files, data={"include_values": "true"}).json()
    top = lambda body: next(a for a in body["analyses"] if a["metadata"]["name"] == "orders")["profile"]["columns"]  # noqa: E731
    assert all(c["top_values"] is None for c in top(plain))
    assert next(c for c in top(asked) if c["column"] == "status")["top_values"] == ["NEW", "SHIPPED"]


# ---------------------------------- the API ----------------------------------- #


@pytest.fixture
def client():
    return TestClient(create_app(llm_mode=LLMMode.OFF, auth=Authenticator.anonymous(), connectors=ConnectorRegistry()))


def post(client, files, **data):
    return client.post("/v1/analyze/batch", files=[("files", f) for f in files], data=data)


def test_batch_endpoint_returns_analyses_pairing_and_rejections(client):
    res = post(client, [("orders.csv", orders_csv()), ("schema.sql", DDL), ("sheet.xlsx", b"PK\x03\x04\x00\x00")])
    body = res.json()
    assert res.status_code == 200
    assert {a["metadata"]["name"] for a in body["analyses"]} == {"orders", "customers"}
    assert any(p["table"] == "orders" and p["data_file"] == "orders.csv" for p in body["pairing"])
    assert [r["name"] for r in body["rejected"]] == ["sheet.xlsx"]
    assert any(d["route"] == "data" for a in body["analyses"] for d in a["documents"])


def test_batch_endpoint_validates_input(client):
    assert post(client, [("orders.csv", orders_csv())], pairs="not json").status_code == 400
    assert post(client, [("orders.csv", orders_csv())], pairs='["wrong"]').status_code == 400
    assert post(client, [("empty.sql", b" ")]).status_code == 422
    assert post(client, [("o.csv", orders_csv())], pairs='{"o.csv": "orders"}', truncated='["o.csv"]').status_code == 200


def test_batch_endpoint_caps_the_request_size(client):
    big = b"id,name\n" + b"1,abcdefghij\n" * (MAX_BATCH_BYTES // 12 + 1000)
    res = post(client, [("big.csv", big)])
    assert res.status_code == 413 and "sample" in res.json()["detail"]


def test_batch_endpoint_requires_a_key_when_auth_is_on():
    secured = TestClient(create_app(llm_mode=LLMMode.OFF, auth=Authenticator([]), connectors=ConnectorRegistry()))
    assert secured.post("/v1/analyze/batch", files=[("files", ("a.csv", orders_csv()))]).status_code == 401


def test_the_old_single_table_upload_still_refuses_data_files(client):
    res = client.post("/v1/analyze", files=[("files", ("orders.csv", orders_csv()))])
    assert res.status_code == 422 and "row data" in res.json()["detail"] and "metadata" in res.json()["detail"]


# ------------------------------------ the CLI ------------------------------------ #


def test_cli_pairs_data_with_metadata_locally(tmp_path, capsys):
    (tmp_path / "orders.csv").write_bytes(orders_csv(dirty=True))
    (tmp_path / "orders.sql").write_text("CREATE TABLE orders (order_id BIGINT PRIMARY KEY, status VARCHAR(10) NOT NULL, amount NUMERIC, placed DATE);")
    out = tmp_path / "rules.json"
    code = cli.main(["analyze", str(tmp_path / "orders.csv"), str(tmp_path / "orders.sql"), "--no-llm", "--out", str(out), "--fail-on", "findings"])
    assert code == 1  # the data contradicts the metadata: amount holds 'abc'
    err = capsys.readouterr().err
    assert "type_mismatch" in err and out.exists()
