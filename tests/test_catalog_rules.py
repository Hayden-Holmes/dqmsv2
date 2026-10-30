"""Catalog aspects -> metadata -> rules, stated-rule parsing, and range tidying."""

import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from fakes import FakeBigQueryClient, bq_field

from dqms.catalog import CatalogError, apply_aspects, entry_name, fetch_entry_aspects
from dqms.connectors.base import ProfileOptions
from dqms.connectors.bigquery import BigQueryConnector
from dqms.connectors.rowprofile import RowProfiler
from dqms.connectors.uri import parse_source
from dqms.emitters.sql import emit_sql_checks
from dqms.models import Column, ColumnProfile, ForeignKey, Metadata, Profile, Rule, RuleType, Tier
from dqms.rules.base import Context
from dqms.rules.engine import RuleEngine
from dqms.rules.post import drop_blessed_violations, drop_subsumed_ranges, reconcile
from dqms.rules.statements import rules_from_statement

SEED = Path(__file__).parent.parent / "deploy" / "seed_catalog_demo.py"
spec = importlib.util.spec_from_file_location("seed_catalog_demo", SEED)
seed = importlib.util.module_from_spec(spec)
sys.modules.setdefault("seed_catalog_demo", seed)
spec.loader.exec_module(seed)


def meta(*cols: Column, **kw) -> Metadata:
    return Metadata(name="t", columns=list(cols), **kw)


def by_type(rules, type_):
    return [r for r in rules if r.type is type_]


# ------------------------------ aspect mapping ------------------------------ #


def aspect(path: str, **data):
    return {"aspectType": "projects/1/locations/us/aspectTypes/x", "path": path, "data": data}


def test_table_and_column_aspects_fold_into_metadata():
    md = meta(Column(name="email", datatype="STRING", description="Contact."), Column(name="id", datatype="STRING"))
    notes = apply_aspects(md, {
        "p.us.gov": aspect("", data_steward="dana", owner_team="crm", data_domain="customer", criticality="high",
                           contains_pii=True),
        "p.us.sem@Schema.email": aspect("Schema.email", semantic_type="Email", classification="Confidential",
                                        glossary_term="Customer Email", business_rule="Must be a valid email address",
                                        valid_values=["a", "b"]),
    })
    assert (md.steward, md.owner, md.domain) == ("dana", "crm", "customer")
    assert {"criticality:high", "contains_pii:true"} <= set(md.tags)
    email = md.column("email")
    assert (email.semantic_type, email.classifications, email.term) == ("email", ["confidential"], "Customer Email")
    assert email.rules == ["Must be a valid email address", "Allowed values: a, b"]
    assert notes == []


def test_field_names_are_matched_by_meaning_and_unknowns_reported():
    md = meta(Column(name="a", datatype="STRING"))
    notes = apply_aspects(md, {
        "p.us.x@Schema.a": aspect("Schema.a", Sensitivity="PII", validation_rule="At least 3", Weird_Field="x"),
        "p.us.x@Schema.ghost": aspect("Schema.ghost", rule="Unique"),
    })
    col = md.column("a")
    assert col.classifications == ["pii"] and col.rules == ["At least 3"]
    assert any("Weird_Field" in n for n in notes) and any("Schema.ghost" in n for n in notes)


def test_system_aspects_and_nested_paths_are_ignored():
    md = meta(Column(name="a", datatype="STRING", description="orig"))
    apply_aspects(md, {
        "dataplex-types.global.overview": aspect("", content="<p>hi</p>"),
        "p.us.x@Schema.a.b": aspect("Schema.a.b", rule="Unique"),
    })
    assert md.column("a").rules == [] and md.tags == []


def test_entry_name_format():
    assert entry_name("p", "US", "d", "t") == (
        "projects/p/locations/us/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/p/datasets/d/tables/t")


class FakeSession:
    def __init__(self, status=200, body=None):
        self.status, self.body, self.calls = status, body or {}, []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        return SimpleNamespace(status_code=self.status, json=lambda: self.body, text=str(self.body))


def test_fetch_reads_the_table_entry_with_all_aspects():
    session = FakeSession(body={"aspects": {"k": {"data": {}}}})
    assert fetch_entry_aspects("p", "US", "d", "t", session=session) == {"k": {"data": {}}}
    url, params = session.calls[0]
    assert url.endswith("/entries/bigquery.googleapis.com/projects/p/datasets/d/tables/t") and params == {"view": "ALL"}


@pytest.mark.parametrize("status,hint", [(403, "catalogViewer"), (404, "catalog yet")])
def test_fetch_failures_explain_the_fix(status, hint):
    session = FakeSession(status, {"error": {"message": "nope"}})
    with pytest.raises(CatalogError, match=hint):
        fetch_entry_aspects("p", "US", "d", "t", session=session)


def bq_connector(catalog):
    table = SimpleNamespace(table_id="t", description=None, labels={}, table_constraints=None, location="US",
                            schema=[bq_field("qty", "INTEGER")])
    client = FakeBigQueryClient(table)
    return BigQueryConnector(client_factory=lambda p: client, max_bytes_billed=0, catalog=catalog), client


def test_connector_applies_aspects_and_passes_the_tables_location():
    seen = []
    connector, _ = bq_connector(lambda *a: seen.append(a) or {"p.us.x@Schema.qty": aspect("Schema.qty", rule="At least 1")})
    snap = connector.fetch(parse_source("bigquery://p/d/t"), ProfileOptions())
    assert seen == [("p", "US", "d", "t")] and snap.metadata.column("qty").rules == ["At least 1"]


def test_unreadable_catalog_costs_a_warning_not_the_table():
    def broken(*a):
        raise CatalogError("HTTP 403")

    snap = bq_connector(broken)[0].fetch(parse_source("bigquery://p/d/t"), ProfileOptions())
    assert [c.name for c in snap.metadata.columns] == ["qty"]
    assert any("Knowledge Catalog aspects were not read" in w and "403" in w for w in snap.warnings)


def test_injected_client_means_no_real_catalog_call():
    table = SimpleNamespace(table_id="t", description=None, labels={}, table_constraints=None, schema=[])
    connector = BigQueryConnector(client_factory=lambda p: FakeBigQueryClient(table), max_bytes_billed=0)
    assert connector.fetch(parse_source("bigquery://p/d/t"), ProfileOptions()).warnings == []


# ------------------------------ stated rules ------------------------------ #

COLS = {c.name: c for c in (
    Column(name="qty", datatype="INT64"), Column(name="price", datatype="NUMERIC"), Column(name="cost", datatype="NUMERIC"),
    Column(name="placed", datatype="TIMESTAMP"), Column(name="shipped", datatype="TIMESTAMP"),
    Column(name="note", datatype="STRING"))}
LOWER = {k.lower(): v for k, v in COLS.items()}


def stated(col, text):
    return [(r.type, tuple(r.columns), r.params) for r in rules_from_statement(COLS[col], text, LOWER)]


@pytest.mark.parametrize("col,text,expected", [
    ("qty", "Must be at least 1", [(RuleType.RANGE, ("qty",), {"min": 1})]),
    ("price", "Must be greater than 0", [(RuleType.RANGE, ("price",), {"min": 0, "exclusive_min": True})]),
    ("price", "Must be zero or greater", [(RuleType.RANGE, ("price",), {"min": 0})]),
    ("qty", "at least 1 and at most 99", [(RuleType.RANGE, ("qty",), {"min": 1, "max": 99})]),
    ("qty", "Must be less than 10", [(RuleType.RANGE, ("qty",), {"max": 10, "exclusive_max": True})]),
    ("placed", "Cannot be in the future", [(RuleType.NOT_FUTURE, ("placed",), {})]),
    ("shipped", "Must not be before placed", [(RuleType.COMPARE, ("placed", "shipped"), {"operator": "<="})]),
    ("shipped", "Must be after placed", [(RuleType.COMPARE, ("placed", "shipped"), {"operator": "<"})]),
    ("placed", "Must be before shipped", [(RuleType.COMPARE, ("placed", "shipped"), {"operator": "<"})]),
    ("placed", "Must not be after shipped", [(RuleType.COMPARE, ("placed", "shipped"), {"operator": "<="})]),
    ("cost", "Must not exceed price", [(RuleType.COMPARE, ("cost", "price"), {"operator": "<="})]),
    ("cost", "at most price", [(RuleType.COMPARE, ("cost", "price"), {"operator": "<="})]),
    ("note", "Must be a valid email address", [(RuleType.REGEX, ("note",), {"pattern": r"^[^@\s]+@[^@\s]+\.[^@\s]+$", "format": "email"})]),
    ("qty", "Unique, never reused", [(RuleType.UNIQUE, ("qty",), {})]),
    ("note", "Required", [(RuleType.NOT_NULL, ("note",), {})]),
])
def test_statements_become_rules(col, text, expected):
    assert stated(col, text) == expected


@pytest.mark.parametrize("col,text", [
    ("qty", "Must exist in customers"),
    ("shipped", "Must not be before ghost_column"),   # not a column of this table
    ("shipped", "Must not be before note"),           # a string is not comparable to a timestamp
    ("note", "Must be at least 5"),                   # numeric bound on a string column
    ("qty", "Required if the order is paid"),
    ("qty", "Some prose with nothing checkable"),
])
def test_unrecognised_or_unsafe_statements_make_no_rule(col, text):
    assert stated(col, text) == []


def test_stated_rules_are_documented_tier_with_the_sentence_as_evidence():
    (r,) = rules_from_statement(COLS["qty"], "Must be at least 1", LOWER)
    assert r.tier is Tier.DOCUMENTED and r.confidence == 0.9 and "Must be at least 1" in r.evidence[0]


def test_compare_statements_merge_with_naming_pair_inference_on_the_same_columns():
    md = meta(Column(name="start_at", datatype="TIMESTAMP"),
              Column(name="end_at", datatype="TIMESTAMP", rules=["Must not be before start_at"]))
    compares = by_type(RuleEngine().generate(md).rules, RuleType.COMPARE)
    assert len(compares) == 1 and compares[0].tier is Tier.DOCUMENTED


def test_allowed_values_and_range_in_aspect_rules_use_the_description_parsers_too():
    md = meta(Column(name="s", datatype="STRING", rules=["Allowed values: a, b, c"]),
              Column(name="d", datatype="FLOAT64", rules=["Between 0 and 1"]))
    rs = RuleEngine().generate(md).rules
    assert by_type(rs, RuleType.ACCEPTED_VALUES)[0].params == {"values": ["a", "b", "c"]}
    assert [r.params for r in by_type(rs, RuleType.RANGE) if r.tier is Tier.DOCUMENTED] == [{"min": 0, "max": 1}]


# ------------------------------ identifiers ------------------------------ #


def test_semantic_identifier_that_is_a_foreign_key_or_composite_part_is_not_unique():
    md = meta(Column(name="parent_id", datatype="STRING", semantic_type="identifier"),
              Column(name="own_id", datatype="STRING", semantic_type="identifier"),
              foreign_keys=[ForeignKey(column="parent_id", references_table="p", references_column="id")])
    rs = RuleEngine().generate(md).rules
    unique = {r.columns[0] for r in by_type(rs, RuleType.UNIQUE)}
    assert unique == {"own_id"}
    assert {r.columns[0] for r in by_type(rs, RuleType.NOT_NULL)} == {"parent_id", "own_id"}


# ------------------------------ range tidying ------------------------------ #


def rng(tier, col="x", **params):
    return Rule(type=RuleType.RANGE, columns=[col], params=params, tier=tier, confidence=0.5, rationale="r")


def ctx_with(**stats):
    return Context(meta(Column(name="x", datatype="INT64")), Profile(row_count=500, columns=[ColumnProfile(column="x", **stats)]))


def test_observed_range_is_dropped_when_the_data_breaks_a_stronger_range():
    observed, stated_ = rng(Tier.OBSERVED, min=-15, max=90), rng(Tier.DOCUMENTED, min=0)
    kept = drop_blessed_violations(ctx_with(min_value=-15, max_value=90), [observed, stated_])
    assert kept == [stated_]


def test_observed_range_is_kept_when_nothing_contradicts_it():
    observed = rng(Tier.OBSERVED, min=1, max=4)
    stated_ = rng(Tier.DOCUMENTED, min=0)
    assert drop_blessed_violations(ctx_with(min_value=1, max_value=4), [observed, stated_]) == [observed, stated_]


def test_exclusive_bound_is_violated_by_a_value_equal_to_it():
    stated_ = rng(Tier.DOCUMENTED, min=0, exclusive_min=True)
    findings = reconcile(ctx_with(min_value=0, max_value=5), [stated_])
    assert len(findings) == 1 and "not above" in findings[0].message
    assert reconcile(ctx_with(min_value=1, max_value=5), [stated_]) == []


def test_tighter_authoritative_range_removes_the_looser_one():
    loose, strict = rng(Tier.INFERRED, min=0), rng(Tier.DOCUMENTED, min=0, exclusive_min=True)
    assert drop_subsumed_ranges([loose, strict]) == [strict]
    assert drop_subsumed_ranges([rng(Tier.SEMANTIC, min=0, max=100), rng(Tier.DOCUMENTED, min=0, max=1)])[0].params["max"] == 1


def test_observed_and_inferred_ranges_never_make_others_redundant():
    a, b = rng(Tier.OBSERVED, min=5, max=9), rng(Tier.INFERRED, min=0)
    assert drop_subsumed_ranges([a, b]) == [a, b]


def test_inferred_range_contradicted_by_the_profile_is_reported_softly():
    findings = reconcile(ctx_with(min_value=-2, max_value=5), [rng(Tier.INFERRED, min=0)])
    assert findings[0].kind == "profile_contradicts_inferred_rule" and "inferred from the column name" in findings[0].message


def test_exclusive_max_is_emitted_in_sql():
    md = meta(Column(name="x", datatype="INT64"))
    rs = RuleEngine().generate(Metadata(name="t", columns=[Column(name="x", datatype="INT64", rules=["Must be less than 10"])]))
    (check,) = [c for c in emit_sql_checks(rs, md, table="p.d.t") if c.sql and "COUNT" in c.sql and "x" in c.sql
                and c.rule_id.startswith("t.x.range")]
    assert "`x` >= 10" in check.sql  # a value equal to an exclusive upper bound is a violation


# ------------------------------ the seeded demo, end to end ------------------------------ #


def run_seed():
    data = seed.generate(datetime.now(timezone.utc))
    results = {}
    for t in seed.TABLES:
        cols = []
        for c in t.columns:
            kind, mode = seed.bq_type(c)
            cols.append(Column(name=c.name, datatype="ARRAY" if mode == "REPEATED" else kind,
                               nullable=None if mode == "REPEATED" else mode != "REQUIRED",
                               description=seed.column_description(c)))
        md = Metadata(name=t.name, description=t.description, columns=cols, primary_keys=list(t.primary_key),
                      foreign_keys=[ForeignKey(column=a, references_table=b, references_column=c) for a, b, c in t.foreign_keys])
        aspects = {k: {**v, "path": k.split("@", 1)[1] if "@" in k else ""} for k, v in seed.table_aspects("p", "US", t).items()}
        notes = apply_aspects(md, aspects)
        names = [c.name for c in t.columns if not c.type.startswith("ARRAY")]
        rp = RowProfiler(names)
        for row in data.rows[t.name]:
            rp.add([row[n] for n in names])
        _, profile = rp.finish(sampled=False, include_values=True, total_rows=len(data.rows[t.name]))
        results[t.name] = (md, RuleEngine().generate(md, profile), notes)
    return results


@pytest.fixture(scope="module")
def seeded():
    return run_seed()


def rule_exists(seeded, table, type_, column, **params):
    rules = seeded[table][1].rules
    return any(r.type is type_ and ",".join(r.columns) == column and all(r.params.get(k) == v for k, v in params.items())
               for r in rules)


def messages(seeded, table):
    return [f.message for f in seeded[table][1].findings]


def test_every_aspect_field_the_seeder_writes_is_understood(seeded):
    assert all(notes == [] for _, _, notes in seeded.values())


def test_aspects_yield_the_rules_that_were_missing(seeded):
    assert rule_exists(seeded, "customers", RuleType.NOT_FUTURE, "signup_date")
    assert rule_exists(seeded, "orders", RuleType.NOT_FUTURE, "order_ts")
    assert rule_exists(seeded, "orders", RuleType.COMPARE, "order_ts,shipped_ts", operator="<=")
    assert rule_exists(seeded, "products", RuleType.COMPARE, "unit_cost,unit_price", operator="<=")
    assert rule_exists(seeded, "products", RuleType.RANGE, "unit_price", min=0, exclusive_min=True)
    assert rule_exists(seeded, "order_items", RuleType.RANGE, "quantity", min=1)
    assert rule_exists(seeded, "orders", RuleType.RANGE, "order_total", min=0)


def test_the_loose_inferred_ranges_are_gone_where_a_stated_one_exists(seeded):
    quantity = [r for r in seeded["order_items"][1].rules if r.type is RuleType.RANGE and r.columns == ["quantity"]]
    assert [r.params for r in quantity] == [{"min": 1}]
    # min 0 (semantic) and "greater than 0" (stated) have different shapes, so only the tidy-up removes the loose one
    for table in ("products", "order_items"):
        price = [r.params for r in seeded[table][1].rules
                 if r.type is RuleType.RANGE and r.columns == ["unit_price"] and r.tier is not Tier.OBSERVED]
        assert price == [{"min": 0, "exclusive_min": True}], table


def test_observed_ranges_no_longer_bless_the_bad_values(seeded):
    for table, column in (("orders", "order_total"), ("order_items", "quantity"), ("order_items", "unit_price")):
        observed = [r for r in seeded[table][1].rules if r.type is RuleType.RANGE and r.columns == [column] and r.tier is Tier.OBSERVED]
        assert observed == [], (table, column)


def test_profile_findings_cover_the_aggregate_visible_problems(seeded):
    assert any("customer_id must be unique" in m for m in messages(seeded, "customers"))
    assert any("future values" in m and m.startswith("signup_date") for m in messages(seeded, "customers"))
    assert any("'ZZ'" in m for m in messages(seeded, "customers"))
    assert any("unit_price minimum 0" in m for m in messages(seeded, "products"))
    assert any(m.startswith("order_ts has future") for m in messages(seeded, "orders"))
    assert any("SHIPPD" in m for m in messages(seeded, "orders"))
    assert any("order_total minimum -15" in m for m in messages(seeded, "orders"))
    items = messages(seeded, "order_items")
    assert any("quantity minimum -2" in m for m in items) and any("maximum 1.5" in m for m in items)


def test_no_false_alarms_on_the_clean_columns(seeded):
    assert not any(m.startswith("shipped_ts has future") for m in messages(seeded, "orders"))
    assert not any("order_id must be unique" in m for m in messages(seeded, "orders"))
    assert not any(m.startswith(("customer_id must", "sku must")) for m in messages(seeded, "order_items"))
