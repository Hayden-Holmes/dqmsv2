"""LLM enrichment: propose *additional* rules the deterministic tiers cannot see.

What the model receives is an explicit allowlist of **metadata and profile statistics**:
names, types, nullability, descriptions, semantic types, classification labels, glossary,
tags, and per-column aggregates (null/distinct rates, distinct count, min/max, lengths).
It never receives *data*: no enumerated value lists, no row-level content, and no value
lists inside the params of existing rules. For columns classified sensitive (or covered by
a policy tag) even min/max are withheld. The payload is built field by field below, not by
dumping models, so a field added to a model later cannot leak by default.

The model's output is untrusted: schema-validated, restricted to known columns, capped
at INFERRED confidence, and dropped if malformed.
"""

from __future__ import annotations

import json
from typing import Any

from dqms.llm.gateway import LLMGateway, Purpose
from dqms.models import Rule, RuleType, Tier
from dqms.rules.base import Context
from dqms.rules.post import is_well_formed

MAX_CONFIDENCE = 0.6
_SENSITIVE = ("pii", "sensitive", "confidential", "restricted", "secret")

SYSTEM_PROMPT = """You are a senior data quality architect. Propose validation rules that a data \
pipeline could enforce, based ONLY on the supplied structural metadata (names, types, constraints, \
descriptions, glossary) and profile statistics. You are not given any row data.

Reply with ONE JSON object: {"rules": [{"type": ..., "columns": [...], "params": {...}, "rationale": "..."}]}

Allowed types and params:
- not_null, unique, not_future: {}
- composite_unique: {} (columns >= 2)
- length: {"min"|"max"|"exact": int}
- accepted_values: {"values": [...]}
- regex: {"pattern": "<python regex>"}
- range: {"min"?: number, "max"?: number}
- age_range: {"min_years"?: n, "max_years"?: n}
- compare: {"operator": "<"|"<="|">"|">="|"="|"!="}, columns = [left, right]

Principles:
- Do NOT repeat rules listed under "existing_rules".
- Prefer precision over quantity. Skip anything you cannot justify from the evidence given.
- Use cross-column rules (date ordering, amounts vs limits, totals) only when column names or
  descriptions clearly imply the relationship.
- Never invent columns. Use exact column names.
- The rationale must cite the evidence (description, name, glossary, or a statistic).
"""


class LLMRuleEnricher:
    name = "llm"

    def __init__(self, gateway: LLMGateway):
        self._gateway = gateway

    def generate(self, ctx: Context, existing: list[Rule] | None = None) -> list[Rule]:
        payload = structural_view(ctx, existing or [])
        reply = self._gateway.complete_json(Purpose.RULES, SYSTEM_PROMPT, json.dumps(payload))
        raw_rules = reply.get("rules", []) if isinstance(reply, dict) else []
        known = {c.name.lower() for c in ctx.metadata.columns}
        out: list[Rule] = []
        for item in raw_rules:
            rule = _to_rule(item)
            if rule is not None and is_well_formed(rule, known):
                out.append(rule)
        return out


def structural_view(ctx: Context, existing: list[Rule]) -> dict[str, Any]:
    """The complete, allowlisted description of a dataset that may leave the service."""
    meta, profile = ctx.metadata, ctx.profile
    columns = []
    for c in meta.columns:
        entry: dict[str, Any] = {
            "name": c.name,
            "datatype": c.datatype,
            "nullable": c.nullable,
            "description": c.description,
            "semantic_type": c.semantic_type,
            "classifications": list(c.classifications),
        }
        stats = profile.column(c.name) if profile else None
        if stats is not None:
            entry["null_rate"] = stats.null_rate
            entry["distinct_rate"] = stats.distinct_rate
            entry["distinct_count"] = stats.distinct_count
            entry["min_length"] = stats.min_length
            entry["max_length"] = stats.max_length
            if not _is_sensitive(c.classifications):
                entry["min_value"] = stats.min_value
                entry["max_value"] = stats.max_value
        columns.append(entry)
    return {
        "dataset": {"name": meta.name, "description": meta.description, "domain": meta.domain, "tags": list(meta.tags)},
        "columns": columns,
        "glossary": [{"term": g.term, "definition": g.definition} for g in meta.glossary],
        "row_count": profile.row_count if profile else None,
        # Params are kept (ranges, lengths, ...) minus value lists, which are data.
        "existing_rules": [
            {"type": r.type.value, "columns": list(r.columns), "params": {k: v for k, v in r.params.items() if k != "values"}}
            for r in existing
        ],
    }


def _is_sensitive(classifications: list[str]) -> bool:
    return any(k in label.lower() for label in classifications for k in _SENSITIVE)


def _to_rule(item: Any) -> Rule | None:
    if not isinstance(item, dict):
        return None
    try:
        return Rule(
            type=RuleType(item["type"]),
            columns=[str(c) for c in item["columns"]],
            params=item.get("params") or {},
            tier=Tier.INFERRED,
            confidence=MAX_CONFIDENCE,
            rationale=str(item.get("rationale") or "Proposed by LLM from metadata."),
            evidence=["llm"],
        )
    except (KeyError, ValueError, TypeError):
        return None
