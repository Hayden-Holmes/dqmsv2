from dqms.normalize.ddl import parse_ddl
from dqms.normalize.documents import SourceDoc, ingest
from dqms.normalize.files import UnsupportedFile, read_upload
from dqms.normalize.profile import normalize_profile

import pytest


def test_ddl_parses_constraints_types_and_comments():
    meta = parse_ddl(
        """CREATE TABLE orders (
            order_id BIGINT PRIMARY KEY,
            customer_id BIGINT NOT NULL REFERENCES customers(id),
            code CHAR(3) NOT NULL,
            note VARCHAR(50),
            amount NUMERIC(10,2),
            UNIQUE (code)
        );
        COMMENT ON COLUMN orders.note IS 'It''s free text';"""
    )
    assert meta.name == "orders" and meta.primary_keys == ["order_id"]
    cols = {c.name: c for c in meta.columns}
    assert cols["order_id"].nullable is False
    assert cols["customer_id"].nullable is False
    assert cols["note"].nullable is True and cols["note"].max_length == 50
    assert cols["code"].unique is True and cols["code"].datatype == "CHAR(3)"
    assert cols["amount"].datatype == "NUMERIC(10,2)" and cols["amount"].max_length is None
    assert cols["note"].description == "It's free text"
    assert [(f.column, f.references_table, f.references_column) for f in meta.foreign_keys] == [
        ("customer_id", "customers", "id")
    ]


def test_ddl_composite_primary_key_and_table_level_fk():
    meta = parse_ddl(
        "CREATE TABLE li (a INT, b INT, c INT, PRIMARY KEY (a, b), "
        "CONSTRAINT fk FOREIGN KEY (c) REFERENCES other (id));"
    )
    assert meta.primary_keys == ["a", "b"]
    assert meta.foreign_keys[0].references_table == "other"


def test_profile_aliases_and_scales():
    profile = normalize_profile(
        {
            "row_count": 200,
            "columns": [
                {"name": "a", "completeness": 98, "uniqueness": 0.5, "mystery": 7},
                {"name": "b", "null_percentage": 25, "unique_count": 10},
                {"column": "c", "null_count": 50, "distinct_values": ["x", "y"], "pattern": "*@*"},
            ],
        }
    )
    a, b, c = profile.columns
    assert round(a.null_rate, 4) == 0.02 and a.distinct_rate == 0.5 and a.extra == {"mystery": 7}
    assert b.null_rate == 0.25 and b.distinct_count == 10
    assert c.null_rate == 0.25 and c.top_values == ["x", "y"] and c.pattern == "*@*"


def test_fixtures_ingest_deterministically(all_docs):
    result = ingest(all_docs)
    assert result.unrecognized == 0
    assert all(r.route == "deterministic" for r in result.reports)
    meta = result.metadata
    assert meta.name == "customer"
    assert meta.owner == "CRM Team" and "pii" in meta.tags
    assert meta.primary_keys == ["customer_id"]
    assert len(meta.upstream) == 1 and len(meta.downstream) == 2
    assert meta.column("date_of_birth").semantic_type == "birth_date"
    assert "Sensitive Personal Data" in meta.column("date_of_birth").classifications
    assert result.profile.row_count == 150000


def test_descriptions_from_different_sources_are_combined(all_docs):
    status = ingest(all_docs).metadata.column("customer_status")
    assert "Lifecycle status" in status.description  # catalog
    assert "ACTIVE, INACTIVE, SUSPENDED" in status.description  # DDL comment


def test_conflicting_facts_are_reported_not_silently_resolved():
    a = SourceDoc("a", {"table_name": "t", "columns": [{"column_name": "x", "data_type": "INT", "is_nullable": False}]})
    b = SourceDoc("b", {"table_name": "t", "columns": [{"column_name": "x", "data_type": "INT", "is_nullable": True}]})
    result = ingest([a, b])
    assert result.metadata.column("x").nullable is False  # first wins
    assert any(f.kind == "source_conflict" and f.column == "x" for f in result.findings)


def test_unrecognized_without_fallback_is_reported():
    result = ingest([SourceDoc("notes.md", "# some prose with no schema in it")])
    assert result.reports[0].route == "unrecognized"


def test_read_upload_formats():
    assert read_upload("x.json", b'{"table_name": "t"}')[0].content == {"table_name": "t"}
    assert read_upload("x.yaml", b"table_name: t\ncolumns: []")[0].content["table_name"] == "t"
    csv_doc = read_upload("p.csv", b"column,null_rate,distinct_rate\nid,0,1\n")[0].content
    assert csv_doc["columns"][0] == {"column": "id", "null_rate": 0, "distinct_rate": 1}
    assert isinstance(read_upload("schema.sql", b"CREATE TABLE t (a INT);")[0].content, str)
    assert read_upload("noext", b'{"asset_name": "x"}')[0].content == {"asset_name": "x"}
    with pytest.raises(UnsupportedFile):
        read_upload("a.xlsx", b"PK\x03\x04\x00\x00binary")
    with pytest.raises(UnsupportedFile):
        read_upload("empty.json", b"  ")


def test_csv_information_schema_is_recognized_without_llm():
    csv = b"column_name,data_type,is_nullable\nid,BIGINT,NO\nemail,VARCHAR,YES\n"
    result = ingest([SourceDoc("cols.csv", read_upload("cols.csv", csv)[0].content)])
    assert result.reports[0].route == "deterministic"
    assert result.metadata.column("id").nullable is False
    assert result.metadata.column("email").nullable is True
