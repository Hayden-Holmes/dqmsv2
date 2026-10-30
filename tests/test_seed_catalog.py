"""The catalog seeder's data and payloads, checked without touching Google Cloud."""

import importlib.util
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parent.parent / "deploy" / "seed_catalog_demo.py"
spec = importlib.util.spec_from_file_location("seed_catalog_demo", SCRIPT)
seed = importlib.util.module_from_spec(spec)
sys.modules["seed_catalog_demo"] = seed
spec.loader.exec_module(seed)

NOW = datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def data():
    return seed.generate(NOW)


def test_generation_is_deterministic():
    assert seed.generate(NOW).rows == seed.generate(NOW).rows


def test_rows_match_the_declared_schema(data):
    for table in seed.TABLES:
        declared = {c.name for c in table.columns}
        for row in data.rows[table.name]:
            assert set(row) == declared, table.name


def test_planted_problems_are_really_in_the_data(data):
    customers, products = data.rows["customers"], data.rows["products"]
    orders, items = data.rows["orders"], data.rows["order_items"]
    email = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    ids = [c["customer_id"] for c in customers]
    known = {c["customer_id"] for c in customers}
    skus = {p["sku"] for p in products}
    order_ids = {o["order_id"] for o in orders}
    now = NOW.strftime("%Y-%m-%dT%H:%M:%SZ")

    actual = {
        ("customers", "email", "null email"): sum(c["email"] is None for c in customers),
        ("customers", "email", "malformed email"): sum(c["email"] is not None and not email.match(c["email"]) for c in customers),
        ("customers", "customer_id", "duplicated key"): len(ids) - len(set(ids)),
        ("products", "unit_price", "price of zero"): sum(float(p["unit_price"]) <= 0 for p in products),
        ("orders", "order_ts", "order placed in the future"): sum(o["order_ts"] > now for o in orders),
        ("orders", "customer_id", "customer that does not exist"): sum(o["customer_id"] not in known for o in orders),
        ("orders", "order_total", "negative total"): sum(float(o["order_total"]) < 0 for o in orders),
        ("orders", "shipped_ts", "shipped before it was ordered"): sum(
            o["shipped_ts"] is not None and o["shipped_ts"] < o["order_ts"] for o in orders),
        ("order_items", "quantity", "quantity of zero or less"): sum(i["quantity"] <= 0 for i in items),
        ("order_items", "discount_pct", "discount above 100%"): sum(i["discount_pct"] > 1 for i in items),
        ("order_items", "sku", "product that does not exist"): sum(i["sku"] not in skus for i in items),
    }
    claimed = {(i["table"], i["column"], i["problem"]): i["rows"] for i in data.issues}
    for key, count in actual.items():
        assert claimed[key] == count, key
    assert all(i["order_id"] in order_ids for i in items)  # no accidental orphans beyond the planted ones


def test_every_issue_names_a_real_column(data):
    columns = {(t.name, c.name) for t in seed.TABLES for c in t.columns}
    assert all((i["table"], i["column"]) in columns for i in data.issues)


def test_foreign_keys_and_primary_keys_reference_real_columns():
    by_name = {t.name: {c.name for c in t.columns} for t in seed.TABLES}
    for t in seed.TABLES:
        assert set(t.primary_key) <= by_name[t.name]
        for column, ref_table, ref_column in t.foreign_keys:
            assert column in by_name[t.name] and ref_column in by_name[ref_table]


def test_constraint_ddl_order_and_text():
    ddl = seed.constraint_ddl("p", "d", seed.TABLES[3])
    assert ddl[0].startswith("ALTER TABLE `p.d.order_items` ADD PRIMARY KEY (order_id, line_no) NOT ENFORCED")
    assert any("FOREIGN KEY (sku) REFERENCES `p.d.products`(sku) NOT ENFORCED" in s for s in ddl)


def test_aspect_keys_cover_table_and_every_column():
    table = seed.TABLES[0]
    aspects = seed.table_aspects("proj", "US", table)
    assert "proj.us.dqms-demo-governance" in aspects
    for col in table.columns:
        assert f"proj.us.dqms-demo-column-semantics@Schema.{col.name}" in aspects
    assert len(aspects) == 1 + len(table.columns)
    assert aspects["proj.us.dqms-demo-column-semantics@Schema.email"]["data"]["classification"] == "confidential"


def test_aspect_data_only_uses_fields_the_aspect_type_defines():
    fields = {name: (ftype, enum) for name, ftype, _, _, enum in seed.GOVERNANCE_FIELDS}
    sem = {name: (ftype, enum) for name, ftype, _, _, enum in seed.SEMANTICS_FIELDS}
    for table in seed.TABLES:
        for key, aspect in seed.table_aspects("p", "US", table).items():
            allowed = sem if "@Schema." in key else fields
            for name, value in aspect["data"].items():
                assert name in allowed, (key, name)
                ftype, enum = allowed[name]
                if ftype == "enum":
                    assert value in enum, (key, name, value)


def test_required_aspect_fields_are_always_supplied():
    required_gov = {n for n, _, _, req, _ in seed.GOVERNANCE_FIELDS if req}
    required_sem = {n for n, _, _, req, _ in seed.SEMANTICS_FIELDS if req}
    for table in seed.TABLES:
        for key, aspect in seed.table_aspects("p", "US", table).items():
            need = required_sem if "@Schema." in key else required_gov
            assert need <= set(aspect["data"]), key


def test_aspect_type_body_shape():
    body = seed.aspect_type_body("x", "X", "d", seed.GOVERNANCE_FIELDS)
    template = body["metadataTemplate"]
    assert template["type"] == "record"
    assert [f["index"] for f in template["recordFields"]] == list(range(1, len(seed.GOVERNANCE_FIELDS) + 1))
    domain = next(f for f in template["recordFields"] if f["name"] == "data_domain")
    assert domain["type"] == "enum" and domain["enumValues"][0] == {"name": "customer", "index": 1}


def test_glossary_terms_used_by_columns_exist():
    used = {c.term for t in seed.TABLES for c in t.columns if c.term}
    assert used <= set(seed.GLOSSARY_TERMS)
    assert {cat for cat, _ in seed.GLOSSARY_TERMS.values()} <= set(seed.GLOSSARY_CATEGORIES)


def test_entry_name_and_location_mapping():
    assert seed.entry_name("p", "US", "d", "t") == (
        "projects/p/locations/us/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/p/datasets/d/tables/t")
    assert seed.dataplex_location("asia-northeast1") == "asia-northeast1"


def test_bq_type_handles_arrays():
    assert seed.bq_type(seed.Col("tags", "ARRAY<STRING>", "x")) == ("STRING", "REPEATED")
    assert seed.bq_type(seed.Col("id", "STRING", "x", "REQUIRED")) == ("STRING", "REQUIRED")


def test_column_description_carries_term_and_values():
    text = seed.column_description(seed.TABLES[0].columns[3])
    assert "Glossary: Country Code." in text and "Allowed values: US,CA" in text


def test_dry_run_needs_no_google_libraries_or_credentials(capsys):
    assert seed.main(["--project", "p", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "BigQuery dataset p.dqms_demo" in out and "Known data problems" in out


def test_field_types_are_ones_the_dataplex_api_accepts():
    # The API rejects 'integer'/'boolean'; its names are int and bool.
    accepted = {"string", "int", "bool", "double", "datetime", "enum"}
    for _, ftype, *_ in seed.GOVERNANCE_FIELDS + seed.SEMANTICS_FIELDS:
        assert ftype in accepted, ftype


class RecordingApi:
    """Stands in for Dataplex: records calls, and returns attached aspects on read-back only if asked to."""

    def __init__(self, read_back=True, patch_status=200):
        self.calls, self.read_back, self.patch_status, self.stored = [], read_back, patch_status, {}

    def call(self, method, url, **kw):
        self.calls.append((method, url, kw))
        if method == "PATCH":
            self.stored = kw["json"]["aspects"] if self.patch_status < 400 else {}
            return self.patch_status, {} if self.patch_status < 400 else {"error": {"message": "denied"}}
        return 200, {"aspects": self.stored if self.read_back else {}}


def test_aspect_keys_are_sent_as_a_repeated_parameter_not_one_comma_joined_value():
    api, failures = RecordingApi(), seed.Failures()
    seed.attach_aspects(api, "p", "d", "US", failures)
    patches = [c for c in api.calls if c[0] == "PATCH"]
    assert len(patches) == len(seed.TABLES) and failures.items == []
    for _, url, kw in patches:
        keys = [v for k, v in kw["params"] if k == "aspectKeys"]
        assert keys == list(kw["json"]["aspects"]) and not any("," in k for k in keys)
        assert "aspectKeys" not in url


def test_attach_reads_back_and_fails_loudly_when_nothing_stuck(capsys):
    failures = seed.Failures()
    seed.attach_aspects(RecordingApi(read_back=False), "p", "d", "US", failures)
    assert len(failures.items) == len(seed.TABLES) and "only 0 of" in failures.items[0]


def test_rejected_update_is_reported_without_a_readback():
    api, failures = RecordingApi(patch_status=403), seed.Failures()
    seed.attach_aspects(api, "p", "d", "US", failures)
    assert len(failures.items) == len(seed.TABLES) and not any(c[0] == "GET" for c in api.calls)
