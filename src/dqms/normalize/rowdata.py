"""Tell *metadata* apart from *row data*, so data is never misread as columns or sent to an LLM.

Uploads are for metadata (schemas, catalogs, contracts). A table whose header describes
columns ('column_name, data_type, ...') is metadata; a table whose header *is* the columns
('id, name, email') holds records. This is a heuristic, so it fails closed: an unusual
metadata file may be refused with an explanation, but a data file is not processed.
It is a backstop; the controls that matter are the LLM mode and the structural-only payload.
"""

from __future__ import annotations

import csv
import json
import re
from typing import Any

from dqms.normalize.ddl import looks_like_ddl

MIN_RECORDS = 3
ROW_DATA_MESSAGE = (
    "this looks like row data (records), not metadata, so it was not processed or sent to any LLM. "
    "Uploads are for schemas, catalogs and contracts. To profile data, point the service at where "
    "it lives (e.g. gs://..., bigquery://...) so only aggregates are computed in place."
)

_IDENTIFIERS = {"name", "column", "column_name", "field", "field_name", "attribute", "col"}
_DESCRIPTORS = {
    "type", "data_type", "datatype", "description", "desc", "comment", "comments", "nullable", "is_nullable",
    "required", "mandatory", "primary_key", "is_primary_key", "pk", "fk", "unique", "is_unique", "max_length",
    "length", "semantic_type", "classification", "sensitivity", "null_rate", "distinct_rate", "distinct_count",
    "min", "max", "min_value", "max_value", "pattern", "allowed_values", "enum", "default", "constraint",
    "table", "table_name", "owner", "steward", "tags", "term", "definition", "row_count",
}
# Keys of the structured metadata documents the deterministic parsers understand.
DOCUMENT_KEYS = {
    "columns", "table_name", "asset_name", "entity", "dataset", "upstream", "downstream",
    "existing_validations", "terms", "classifications", "column_semantics", "column_profiles",
}
_SEPARATOR_ROW = re.compile(r"^[\s|:\-+]+$")


def metadata_shaped(headers: list[str]) -> bool:
    """True if a header row (or record keys) describes columns rather than being columns."""
    keys = {h.strip().lower() for h in headers}
    descriptors = keys & _DESCRIPTORS
    return len(descriptors) >= 2 or (bool(keys & _IDENTIFIERS) and bool(descriptors))


def records_look_like_metadata(keys: list[str]) -> bool:
    lowered = {k.strip().lower() for k in keys}
    return metadata_shaped(keys) or bool(lowered & DOCUMENT_KEYS)


def looks_like_row_data(text: str) -> bool:
    if looks_like_ddl(text):
        return False
    header = _record_header(text)
    return header is not None and not records_look_like_metadata(header)


def _record_header(text: str) -> list[str] | None:
    """Header/keys of tabular or record-per-line content with at least MIN_RECORDS records."""
    sample = text[:400_000]
    stripped = sample.lstrip()

    if stripped.startswith("["):  # JSON array of objects
        try:
            loaded = json.loads(text)
        except json.JSONDecodeError:
            loaded = None
        if _all_dicts(loaded) and len(loaded) >= MIN_RECORDS:
            return list(loaded[0])

    lines = [ln for ln in sample.splitlines() if ln.strip()]
    if len(lines) >= MIN_RECORDS and lines[0].lstrip().startswith("{"):  # JSON Lines
        records = [_try_json(ln) for ln in lines[:200]]
        dicts = [r for r in records if isinstance(r, dict)]
        if len(dicts) >= MIN_RECORDS and len(dicts) >= 0.9 * len(records):
            return list(dicts[0])

    if len(lines) > MIN_RECORDS:  # header + at least MIN_RECORDS rows
        for delimiter in (",", "\t", ";", "|"):
            rows = _delimited(lines[:200], delimiter)
            if rows is not None:
                return rows[0]
    return None


def _delimited(lines: list[str], delimiter: str) -> list[list[str]] | None:
    cleaned = []
    for ln in lines:
        if delimiter == "|":  # markdown table: drop edge pipes and the |---|---| rule
            if _SEPARATOR_ROW.match(ln):
                continue
            ln = ln.strip().strip("|")
        cleaned.append(ln)
    try:
        rows = list(csv.reader(cleaned, delimiter=delimiter))
    except csv.Error:
        return None
    widths = [len(r) for r in rows]
    if not widths:
        return None
    modal = max(set(widths), key=widths.count)
    if modal < 2 or sum(1 for w in widths if w == modal) < 0.9 * len(widths):
        return None
    return [[cell.strip() for cell in r] for r in rows if len(r) == modal]


def _all_dicts(value: Any) -> bool:
    return isinstance(value, list) and bool(value) and all(isinstance(v, dict) for v in value)


def _try_json(line: str) -> Any:
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None
