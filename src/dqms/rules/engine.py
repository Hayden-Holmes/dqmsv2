"""Rule engine: run generators, merge, annotate, reconcile."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field

from dqms.llm.gateway import LLMGateway, Purpose
from dqms.models import TIER_ORDER, Finding, Metadata, Profile, Rule, RuleSet
from dqms.rules.base import Context, RuleGenerator
from dqms.rules.cross import CrossColumnGenerator
from dqms.rules.declared import DeclaredGenerator
from dqms.rules.documented import DocumentedGenerator
from dqms.rules.llm import LLMRuleEnricher
from dqms.rules.observed import ObservedGenerator
from dqms.rules.post import assign_severity, drop_blessed_violations, drop_subsumed_ranges, mark_enforced, merge, reconcile
from dqms.rules.semantic import SemanticGenerator


@dataclass
class EngineOptions:
    min_confidence: float = 0.0
    use_llm: bool = True  # can only narrow the server's LLM policy, never widen it
    min_profile_rows: int = 100
    disabled_generators: set[str] = field(default_factory=set)


class RuleEngine:
    def __init__(self, llm: LLMGateway | None = None, generators: list[RuleGenerator] | None = None):
        self._generators: list[RuleGenerator] = generators or [
            DeclaredGenerator(),
            DocumentedGenerator(),
            SemanticGenerator(),
            ObservedGenerator(),
            CrossColumnGenerator(),
        ]
        self._enricher = LLMRuleEnricher(llm) if llm is not None and llm.allows(Purpose.RULES) else None

    def generate(
        self,
        metadata: Metadata,
        profile: Profile | None = None,
        options: EngineOptions | None = None,
        findings: list[Finding] | None = None,
    ) -> RuleSet:
        options = options or EngineOptions()
        ctx = Context(metadata, profile, options.min_profile_rows)
        findings = list(findings or [])

        rules: list[Rule] = []
        for gen in self._generators:
            if gen.name not in options.disabled_generators:
                rules += gen.generate(ctx)

        llm_used = False
        if self._enricher is not None and options.use_llm:
            try:
                rules += self._enricher.generate(ctx, merge(rules))
                llm_used = True
            except Exception as exc:  # enrichment is additive; never lose the deterministic result
                findings.append(Finding(kind="llm_enrichment_failed", message=str(exc)))

        merged = drop_subsumed_ranges(drop_blessed_violations(ctx, merge(rules)))
        mark_enforced(merged, metadata.existing_validations)
        assign_severity(merged)
        findings += reconcile(ctx, merged)

        kept = [r for r in merged if r.confidence >= options.min_confidence]
        order = {c.name.lower(): i for i, c in enumerate(metadata.columns)}
        kept.sort(key=lambda r: (order.get(r.columns[0].lower(), 10**6), TIER_ORDER.index(r.tier), r.type.value))
        for r in kept:
            r.id = _rule_id(metadata.name, r)

        return RuleSet(
            dataset=metadata.name, fingerprint=fingerprint(kept), rules=kept, findings=findings, llm_used=llm_used
        )


def fingerprint(rules: list[Rule]) -> str:
    """Hash of what the rules *check* (identity, params, tier). Wording, evidence and
    confidence are excluded so cosmetic changes don't look like drift."""
    canonical = sorted((r.id, r.tier.value, json.dumps(r.params, sort_keys=True, default=str)) for r in rules)
    return hashlib.sha256(json.dumps(canonical).encode()).hexdigest()[:16]


def _rule_id(dataset: str, r: Rule) -> str:
    cols = "+".join(r.columns)
    suffix = ""
    if r.type.value in {"length", "range"}:
        suffix = "." + "_".join(sorted(r.params))
    elif r.type.value == "compare":
        suffix = f".{r.params.get('operator', '')}"
    raw = f"{dataset}.{cols}.{r.type.value}{suffix}"
    return re.sub(r"[^A-Za-z0-9_.+<>=!-]", "_", raw).lower()
