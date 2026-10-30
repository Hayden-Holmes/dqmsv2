"""Knowledge Catalog (Dataplex) aspects -> canonical metadata.

Aspect types are defined by each organisation, so field names are matched by meaning, not by one
fixed schema: `business_rule`, `rule` and `validation_rule` all become stated rules; `data_steward`
and `steward` both set the steward. Fields that match nothing are reported, never guessed at.
Only metadata is read here (aspects are annotations on tables and columns); no table data is touched.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

from dqms.connectors.errors import ConnectorError
from dqms.models import Metadata

DATAPLEX = "https://dataplex.googleapis.com/v1"
SYSTEM_PROJECT = "dataplex-types"  # Google's own aspect types (schema, overview, ...), not business metadata
MAX_TAG_CHARS = 100

CatalogReader = Callable[[str, str, str, str], dict[str, Any]]  # (project, bq location, dataset, table) -> entry aspects


class CatalogError(ConnectorError):
    pass


_TABLE_FIELDS = {
    "owner": {"owner", "owner_team", "data_owner", "team"},
    "steward": {"data_steward", "steward", "custodian", "data_custodian"},
    "domain": {"data_domain", "domain", "business_domain"},
}
_COLUMN_FIELDS = {
    "semantic_type": {"semantic_type", "semantic", "data_semantic", "semantic_class"},
    "classification": {"classification", "data_classification", "sensitivity", "sensitivity_level"},
    "term": {"glossary_term", "business_term", "term"},
    "rules": {"business_rule", "business_rules", "rule", "rules", "validation_rule", "validation_rules"},
    "values": {"valid_values", "allowed_values", "accepted_values", "enum_values"},
    "definition": {"definition", "business_description", "business_definition"},
    "pii": {"pii", "contains_pii", "is_pii"},
}


def entry_name(project: str, location: str, dataset: str, table: str) -> str:
    return (f"projects/{project}/locations/{location.lower()}/entryGroups/@bigquery/entries/"
            f"bigquery.googleapis.com/projects/{project}/datasets/{dataset}/tables/{table}")


def fetch_entry_aspects(project: str, location: str, dataset: str, table: str, session: Any = None) -> dict[str, Any]:
    """The aspects attached to one BigQuery table's catalog entry, keyed as the API returns them."""
    if session is None:
        try:
            import google.auth
            from google.auth.transport.requests import AuthorizedSession
        except ImportError as exc:
            raise CatalogError("reading the catalog needs: pip install 'dqms[bigquery]'") from exc
        try:
            credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        except Exception as exc:
            raise CatalogError(f"no Google credentials for the catalog: {exc}") from exc
        session = AuthorizedSession(credentials)
    url = f"{DATAPLEX}/{entry_name(project, location, dataset, table)}"
    try:
        response = session.get(url, params={"view": "ALL"}, timeout=30)
    except Exception as exc:
        raise CatalogError(f"catalog request failed: {type(exc).__name__}: {str(exc)[:200]}") from exc
    if response.status_code == 200:
        return response.json().get("aspects") or {}
    try:
        message = response.json().get("error", {}).get("message", "")[:240]
    except ValueError:
        message = response.text[:240]
    hint = {
        403: " Grant the service account roles/dataplex.catalogViewer and enable dataplex.googleapis.com.",
        404: " The table may not be in the catalog yet, or the Dataplex API is not enabled.",
    }.get(response.status_code, "")
    raise CatalogError(f"catalog returned HTTP {response.status_code}: {message}{hint}")


def apply_aspects(metadata: Metadata, aspects: dict[str, Any]) -> list[str]:
    """Fold aspects into `metadata` in place. Returns notes (unmapped fields, unknown columns)."""
    columns = {c.name.lower(): c for c in metadata.columns}
    unmapped: set[str] = set()
    unknown: set[str] = set()
    for key, aspect in aspects.items():
        if not isinstance(aspect, dict) or not isinstance(aspect.get("data"), dict):
            continue
        if key.split(".", 1)[0] == SYSTEM_PROJECT or str(aspect.get("aspectType", "")).startswith(f"projects/{SYSTEM_PROJECT}"):
            continue
        path = aspect.get("path") or (key.split("@", 1)[1] if "@" in key else "")
        data = aspect["data"]
        if not path:
            unmapped |= _table_level(metadata, data)
        elif path.startswith("Schema.") and "." not in path[len("Schema."):]:
            column = columns.get(path[len("Schema."):].lower())
            if column is None:
                unknown.add(path)
            else:
                unmapped |= _column_level(column, data)
        # nested (struct) columns and other paths are not mapped
    notes = []
    if unmapped:
        notes.append("aspect fields not used: " + ", ".join(sorted(unmapped)))
    if unknown:
        notes.append("aspects on columns the table does not have: " + ", ".join(sorted(unknown)))
    return notes


def _field(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")


def _texts(value: Any) -> list[str]:
    items = value if isinstance(value, list) else [value]
    return [str(v).strip() for v in items if isinstance(v, (str, int, float)) and not isinstance(v, bool) and str(v).strip()]


def _table_level(metadata: Metadata, data: dict[str, Any]) -> set[str]:
    unmapped: set[str] = set()
    for raw, value in data.items():
        name, text = _field(raw), _texts(value)
        target = next((t for t, names in _TABLE_FIELDS.items() if name in names), None)
        if target and text:
            setattr(metadata, target, text[0])
        elif isinstance(value, bool):
            metadata.tags.append(f"{name}:{str(value).lower()}")
        elif text and all(len(t) <= MAX_TAG_CHARS for t in text):
            metadata.tags.append(f"{name}:{', '.join(text)}")
        elif text:
            unmapped.add(raw)
    return unmapped


def _column_level(column, data: dict[str, Any]) -> set[str]:
    unmapped: set[str] = set()
    for raw, value in data.items():
        name = _field(raw)
        kind = next((k for k, names in _COLUMN_FIELDS.items() if name in names), None)
        if kind == "pii":
            if value is True:
                column.classifications.append("pii")
            continue
        text = _texts(value)
        if not text:
            continue
        if kind == "semantic_type":
            column.semantic_type = column.semantic_type or _field(text[0])
        elif kind == "classification":
            column.classifications += [t.lower() for t in text if t.lower() not in column.classifications]
        elif kind == "term":
            column.term = column.term or text[0]
        elif kind == "rules":
            column.rules += [t for t in text if t not in column.rules]
        elif kind == "values":
            values = text if isinstance(value, list) else [v.strip() for v in text[0].split(",") if v.strip()]
            column.rules.append("Allowed values: " + ", ".join(values))
        elif kind == "definition":
            column.description = text[0] if not column.description else f"{column.description} {text[0]}"
        else:
            unmapped.add(raw)
    return unmapped
