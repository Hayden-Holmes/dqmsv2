"""Universal normalizer: turn *any* metadata document into canonical models using an LLM.

This is the path that makes drag-and-drop work for formats we have no parser
for (data contracts, wiki pages, vendor exports, prose). Safeguards:

* the model must emit the canonical schema, which is validated by pydantic;
* every extracted fact is grounded against the source text, so facts the
  document never mentioned are dropped instead of trusted;
* long inputs are chunked and the partial results merged.
"""

from __future__ import annotations

from typing import Any

from dqms.llm.gateway import EgressDenied, LLMGateway, Purpose
from dqms.models import Column, Finding, Metadata, Profile
from dqms.normalize.documents import merge_metadata
from dqms.normalize.rowdata import ROW_DATA_MESSAGE, looks_like_row_data

CHUNK_CHARS = 24_000
MAX_DOCUMENT_CHARS = 200_000  # metadata documents are small; anything bigger is far more likely data

SYSTEM_PROMPT = """You extract data-quality-relevant metadata from documents of ANY format \
(DDL, JSON, YAML, CSV, XML, markdown, data contracts, catalog exports, prose).

Reply with ONE JSON object:
{
  "metadata": null | {
    "name": "dataset/table name or null",
    "description": null,
    "owner": null, "steward": null, "domain": null, "tags": [],
    "primary_keys": ["column"],
    "foreign_keys": [{"column": "", "references_table": "", "references_column": ""}],
    "upstream": [{"system": null, "table": ""}], "downstream": [{"system": null, "table": ""}],
    "glossary": [{"term": "", "definition": ""}],
    "existing_validations": [{"column": "", "rule": ""}],
    "columns": [{
      "name": "", "datatype": "as written in the source, e.g. VARCHAR(100)",
      "nullable": true|false|null, "unique": true|false|null, "max_length": null,
      "description": null,
      "semantic_type": "snake_case slug if the source states one, else null",
      "classifications": [], "allowed_values": null | ["..."]
    }]
  },
  "profile": null | {
    "row_count": null,
    "columns": [{"column": "", "null_rate": null, "distinct_count": null, "distinct_rate": null,
                 "min_value": null, "max_value": null, "min_length": null, "max_length": null,
                 "top_values": null, "pattern": null}]
  }
}

Rules:
- Record ONLY what the document states. Use null / [] when it does not say. Never guess nullability,
  uniqueness, types or value lists from column names.
- nullable=false only for explicit NOT NULL / required / mandatory; nullable=true only if stated optional/nullable.
- Rates are fractions 0..1 (convert percentages).
- "profile" is for observed statistics only (counts, rates, min/max, value lists), never for requirements.
- If the document contains neither, reply {"metadata": null, "profile": null}.
"""


class LLMNormalizer:
    def __init__(self, gateway: LLMGateway):
        self._gateway = gateway

    def __call__(self, name: str, text: str) -> tuple[Metadata | None, Profile | None]:
        # Refuse before anything is chunked or sent; the gateway enforces mode/size/scrubbing again.
        if len(text) > MAX_DOCUMENT_CHARS:
            raise EgressDenied(
                f"document is {len(text):,} characters (limit {MAX_DOCUMENT_CHARS:,}); not sent to any LLM"
            )
        if looks_like_row_data(text):
            raise EgressDenied(ROW_DATA_MESSAGE)
        metadata: Metadata | None = None
        profile: Profile | None = None
        findings: list[Finding] = []

        for chunk in _chunks(text):
            reply = self._gateway.complete_json(Purpose.DOCUMENT, SYSTEM_PROMPT, f"Document name: {name}\n\n{chunk}")
            if not isinstance(reply, dict):
                continue
            partial_meta = _safe(Metadata, reply.get("metadata"))
            partial_prof = _safe(Profile, reply.get("profile"))
            if partial_meta is not None:
                partial_meta = ground_metadata(partial_meta, text)
                if metadata is None:
                    metadata = partial_meta
                else:
                    merge_metadata(metadata, partial_meta, findings)
            if partial_prof is not None:
                partial_prof = ground_profile(partial_prof, text)
                if profile is None:
                    profile = partial_prof
                else:
                    seen = {c.column.lower() for c in profile.columns}
                    profile.columns += [c for c in partial_prof.columns if c.column.lower() not in seen]
                    profile.row_count = profile.row_count or partial_prof.row_count

        if metadata is not None and not (metadata.columns or metadata.name or metadata.glossary):
            metadata = None
        if profile is not None and not profile.columns:
            profile = None
        return metadata, profile


# --------------------------------------------------------------------------- #
# Grounding
# --------------------------------------------------------------------------- #


def ground_metadata(meta: Metadata, source: str) -> Metadata:
    """Drop anything the source text does not support."""
    haystack = source.lower()
    present = lambda s: bool(s) and str(s).lower() in haystack  # noqa: E731

    columns: list[Column] = []
    for col in meta.columns:
        if not present(col.name):
            continue
        if col.allowed_values:
            col.allowed_values = [v for v in col.allowed_values if present(v)] or None
        if col.datatype and not present(col.datatype.split("(")[0]):
            col.datatype = None
        columns.append(col)
    names = {c.name.lower() for c in columns}

    meta.columns = columns
    meta.primary_keys = [k for k in meta.primary_keys if k.lower() in names]
    meta.foreign_keys = [f for f in meta.foreign_keys if f.column.lower() in names and present(f.references_table)]
    meta.existing_validations = [
        v for v in meta.existing_validations if v.column is None or v.column.lower() in names
    ]
    for attr in ("owner", "steward", "domain"):
        if getattr(meta, attr) and not present(getattr(meta, attr)):
            setattr(meta, attr, None)
    if meta.name and not present(meta.name):
        meta.name = ""
    return meta


def ground_profile(profile: Profile, source: str) -> Profile:
    haystack = source.lower()
    profile.columns = [c for c in profile.columns if c.column.lower() in haystack]
    return profile


def _safe(model: type, data: Any):
    if not isinstance(data, dict):
        return None
    try:
        return model.model_validate(data)
    except Exception:
        return None


def _chunks(text: str) -> list[str]:
    if len(text) <= CHUNK_CHARS:
        return [text]
    chunks, current, size = [], [], 0
    for line in text.splitlines(keepends=True):
        if size + len(line) > CHUNK_CHARS and current:
            chunks.append("".join(current))
            current, size = [], 0
        current.append(line)
        size += len(line)
    if current:
        chunks.append("".join(current))
    return chunks
