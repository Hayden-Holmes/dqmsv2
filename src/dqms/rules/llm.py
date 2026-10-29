"""LLM enrichment: propose *additional* rules the deterministic tiers cannot see.

The model gets metadata and profile aggregates (never row data; values for
PII-classified columns are withheld), plus the rules already generated so it
only adds new ones. Its output is untrusted: it is schema-validated, restricted
to known columns, capped at INFERRED confidence, and dropped if malformed.
"""

from __future__ import annotations

import json
from typing import Any

from dqms.llm.client import LLMClient
from dqms.models import Rule, RuleType, Tier
from dqms.rules.base import Context
from dqms.rules.post import is_well_formed

MAX_CONFIDENCE = 0.6
_SENSITIVE = ("pii", "sensitive", "confidential", "restricted", "secret")

SYSTEM_PROMPT = """You are a senior data quality architect. Propose validation rules that a data \
pipeline could enforce, based ONLY on the supplied metadata and profile statistics.

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
- The rationale must cite the evidence (description, name, glossary, or statistic).
"""


class LLMRuleEnricher:
    name = "llm"

    def __init__(self, client: LLMClient):
        self._client = client

    def generate(self, ctx: Context, existing: list[Rule] | None = None) -> list[Rule]:
        reply = self._client.complete_json(SYSTEM_PROMPT, json.dumps(_payload(ctx, existing or []), default=str))
        raw_rules = reply.get("rules", []) if isinstance(reply, dict) else []
        known = {c.name.lower() for c in ctx.metadata.columns}
        out: list[Rule] = []
        for item in raw_rules:
            rule = _to_rule(item)
            if rule is not None and is_well_formed(rule, known):
                out.append(rule)
        return out


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


def _payload(ctx: Context, existing: list[Rule]) -> dict[str, Any]:
    meta = ctx.metadata
    columns = []
    for c in meta.columns:
        sensitive = any(k in cls.lower() for cls in c.classifications for k in _SENSITIVE)
        entry: dict[str, Any] = {
            "name": c.name, "datatype": c.datatype, "nullable": c.nullable, "description": c.description,
            "semantic_type": c.semantic_type,
        }
        if ctx.profile and (stats := ctx.profile.column(c.name)):
            entry["profile"] = {
                "null_rate": stats.null_rate, "distinct_rate": stats.distinct_rate,
                "min_value": None if sensitive else stats.min_value,
                "max_value": None if sensitive else stats.max_value,
                "values": None if sensitive else stats.top_values,
            }
        columns.append(entry)
    return {
        "dataset": {"name": meta.name, "description": meta.description, "domain": meta.domain, "tags": meta.tags},
        "columns": columns,
        "glossary": [g.model_dump() for g in meta.glossary],
        "row_count": ctx.profile.row_count if ctx.profile else None,
        "existing_rules": [
            {"type": r.type.value, "columns": r.columns, "params": r.params} for r in existing
        ],
    }
