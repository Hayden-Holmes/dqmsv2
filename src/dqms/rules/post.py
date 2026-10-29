"""Post-processing applied to the combined output of all generators."""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import date

from dqms.models import (
    TIER_ORDER,
    ExistingValidation,
    Finding,
    Rule,
    RuleType,
    Severity,
    Tier,
)
from dqms.rules.base import Context

CORROBORATION_BONUS = 0.1
MAX_CORROBORATED = 0.95


# --------------------------------------------------------------------------- #
# Merge
# --------------------------------------------------------------------------- #


def merge(rules: list[Rule]) -> list[Rule]:
    """Collapse rules with the same identity. Strongest tier's params win; evidence is pooled.

    Independent tiers agreeing on the same rule raises confidence (never above
    the corroborated cap, and never for rules that are already declared).
    """
    groups: dict[tuple, list[Rule]] = defaultdict(list)
    for r in rules:
        groups[r.key].append(r)

    merged: list[Rule] = []
    for group in groups.values():
        group.sort(key=lambda r: (TIER_ORDER.index(r.tier), -r.confidence))
        primary = group[0].model_copy(deep=True)
        for other in group[1:]:
            primary.evidence += [e for e in other.evidence if e not in primary.evidence]
        primary.confidence = max(r.confidence for r in group)
        if primary.tier is not Tier.DECLARED and len({r.tier for r in group}) > 1:
            primary.confidence = min(MAX_CORROBORATED, primary.confidence + CORROBORATION_BONUS)
        merged.append(primary)
    return merged


# --------------------------------------------------------------------------- #
# Severity
# --------------------------------------------------------------------------- #


def assign_severity(rules: list[Rule]) -> None:
    for r in rules:
        if r.tier in {Tier.DECLARED, Tier.DOCUMENTED}:
            r.severity = Severity.ERROR
        elif r.tier is Tier.SEMANTIC:
            r.severity = Severity.WARNING
        elif r.tier is Tier.OBSERVED:
            r.severity = Severity.INFO
        else:
            r.severity = Severity.WARNING if r.confidence >= 0.55 else Severity.INFO


# --------------------------------------------------------------------------- #
# Already enforced at the source
# --------------------------------------------------------------------------- #

_VOCAB: dict[str, RuleType] = {
    "not_null": RuleType.NOT_NULL, "non_null": RuleType.NOT_NULL, "is_not_null": RuleType.NOT_NULL,
    "required": RuleType.NOT_NULL, "completeness": RuleType.NOT_NULL,
    "unique": RuleType.UNIQUE, "uniqueness": RuleType.UNIQUE, "primary_key": RuleType.UNIQUE,
    "valid_email": RuleType.REGEX, "email": RuleType.REGEX, "regex": RuleType.REGEX,
    "pattern": RuleType.REGEX, "format": RuleType.REGEX,
    "allowed_values": RuleType.ACCEPTED_VALUES, "accepted_values": RuleType.ACCEPTED_VALUES,
    "enum": RuleType.ACCEPTED_VALUES, "in_set": RuleType.ACCEPTED_VALUES,
    "range": RuleType.RANGE, "between": RuleType.RANGE, "non_negative": RuleType.RANGE,
    "length": RuleType.LENGTH, "max_length": RuleType.LENGTH,
    "foreign_key": RuleType.FOREIGN_KEY, "referential_integrity": RuleType.FOREIGN_KEY,
    "not_future": RuleType.NOT_FUTURE, "datatype": RuleType.DATATYPE, "type": RuleType.DATATYPE,
}


def mark_enforced(rules: list[Rule], existing: list[ExistingValidation]) -> None:
    covered: set[tuple[RuleType, str]] = set()
    for v in existing:
        rule_type = _VOCAB.get(re.sub(r"[\s-]+", "_", v.rule.strip().lower()))
        if rule_type and v.column:
            covered.add((rule_type, v.column.lower()))
    for r in rules:
        if len(r.columns) == 1 and (r.type, r.columns[0].lower()) in covered:
            r.already_enforced = True


# --------------------------------------------------------------------------- #
# Well-formedness (safety net for LLM-proposed rules)
# --------------------------------------------------------------------------- #

_OPERATORS = {"<", "<=", ">", ">=", "=", "!="}


def is_well_formed(r: Rule, known_columns: set[str]) -> bool:
    if not r.columns or any(c.lower() not in known_columns for c in r.columns):
        return False
    p = r.params
    num = lambda v: isinstance(v, (int, float)) and not isinstance(v, bool)  # noqa: E731
    match r.type:
        case RuleType.COMPARE:
            return len(r.columns) == 2 and p.get("operator") in _OPERATORS
        case RuleType.COMPOSITE_UNIQUE:
            return len(r.columns) >= 2
        case RuleType.LENGTH:
            return any(num(p.get(k)) for k in ("min", "max", "exact"))
        case RuleType.RANGE:
            return any(num(p.get(k)) for k in ("min", "max"))
        case RuleType.ACCEPTED_VALUES:
            return bool(p.get("values")) or bool(p.get("reference_set"))
        case RuleType.REGEX:
            if not isinstance(p.get("pattern"), str):
                return False
            try:
                re.compile(p["pattern"])
                return True
            except re.error:
                return False
        case RuleType.AGE_RANGE:
            return any(num(p.get(k)) for k in ("min_years", "max_years"))
        case RuleType.FOREIGN_KEY:
            return bool(p.get("references_table") and p.get("references_column"))
        case RuleType.DATATYPE:
            return bool(p.get("type"))
        case _:
            return len(r.columns) == 1


# --------------------------------------------------------------------------- #
# Reconcile rules with profile evidence
# --------------------------------------------------------------------------- #


def reconcile(ctx: Context, rules: list[Rule]) -> list[Finding]:
    """Flag where authoritative rules (declared/documented) disagree with the profile."""
    if ctx.profile is None:
        return []
    findings: list[Finding] = []
    today = date.today().isoformat()

    for r in rules:
        if r.tier not in {Tier.DECLARED, Tier.DOCUMENTED, Tier.SEMANTIC} or len(r.columns) != 1:
            continue
        stats = ctx.profile.column(r.columns[0])
        if stats is None:
            continue
        col, p = r.columns[0], r.params

        if r.type is RuleType.NOT_NULL and stats.null_rate:
            findings.append(_f(col, "profile_violates_rule",
                               f"{col} is required but the profile shows a null rate of {stats.null_rate:.2%}."))
        elif r.type is RuleType.UNIQUE and stats.distinct_rate is not None and stats.distinct_rate < 1.0:
            findings.append(_f(col, "profile_violates_rule",
                               f"{col} must be unique but the distinct rate is {stats.distinct_rate:.4f}."))
        elif r.type is RuleType.ACCEPTED_VALUES and p.get("values") and stats.top_values:
            outside = sorted(set(stats.top_values) - set(map(str, p["values"])))
            if outside:
                findings.append(_f(col, "profile_violates_rule",
                                   f"{col} contains values outside the allowed set: {outside}."))
        elif r.type is RuleType.RANGE:
            lo, hi = stats.min_value, stats.max_value
            if _num(lo) and _num(p.get("min")) and lo < p["min"]:
                findings.append(_f(col, "profile_violates_rule", f"{col} minimum {lo} is below the required {p['min']}."))
            if _num(hi) and _num(p.get("max")) and hi > p["max"]:
                findings.append(_f(col, "profile_violates_rule", f"{col} maximum {hi} exceeds the allowed {p['max']}."))
        elif r.type is RuleType.LENGTH and stats.max_length is not None:
            limit = p.get("max") or p.get("exact")
            if _num(limit) and stats.max_length > limit:
                findings.append(_f(col, "profile_violates_rule",
                                   f"{col} has values of length {stats.max_length}, above the limit {limit}."))
        elif r.type is RuleType.NOT_FUTURE and isinstance(stats.max_value, str) and stats.max_value[:10] > today:
            findings.append(_f(col, "profile_violates_rule", f"{col} has future values (max {stats.max_value})."))
    return findings


def _f(column: str, kind: str, message: str) -> Finding:
    return Finding(column=column, kind=kind, message=message)


def _num(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)
