"""Rules implied by a column's semantic type, or (at lower confidence) its name."""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass

from dqms.models import Column, Rule, RuleType, Tier
from dqms.rules.base import Context, is_temporal, rule, type_family

EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
PHONE_PATTERN = r"^\+?[0-9\s().-]{7,20}$"
URL_PATTERN = r"^https?://\S+$"

SEMANTIC_CONFIDENCE = 0.85
NAME_CONFIDENCE = 0.55


@dataclass(frozen=True)
class Spec:
    key: str
    slugs: frozenset[str]
    name: re.Pattern
    families: frozenset[str]  # empty = any type; "unknown" types are always allowed
    build: Callable[[Context, Column, Tier, float, list[str]], list[Rule]]


def _email(ctx, col, tier, conf, ev):
    return [rule(RuleType.REGEX, col.name, tier, conf, f"{col.name} holds email addresses.", ev,
                 pattern=EMAIL_PATTERN, format="email")]


def _birth(ctx, col, tier, conf, ev):
    return [
        rule(RuleType.NOT_FUTURE, col.name, tier, conf, f"{col.name} is a date of birth and cannot be in the future.", ev),
        rule(RuleType.AGE_RANGE, col.name, tier, conf, f"{col.name} should imply a realistic age.", ev, max_years=120),
    ]


def _identifier(ctx, col, tier, conf, ev):
    rules = [rule(RuleType.NOT_NULL, col.name, tier, conf, f"{col.name} identifies records and must be present.", ev)]
    meta, lowered = ctx.metadata, col.name.lower()
    # A reference to another table's key, or one part of a composite key, repeats by design.
    repeats = any(fk.column.lower() == lowered for fk in meta.foreign_keys) or (
        len(meta.primary_keys) > 1 and lowered in {k.lower() for k in meta.primary_keys})
    if not repeats:
        rules.append(rule(RuleType.UNIQUE, col.name, tier, conf, f"{col.name} identifies records and must be unique.", ev))
    return rules


def _non_negative(label: str):
    def build(ctx, col, tier, conf, ev):
        return [rule(RuleType.RANGE, col.name, tier, conf, f"{col.name} is a {label} and should not be negative.", ev, min=0)]
    return build


def _percentage(ctx, col, tier, conf, ev):
    return [rule(RuleType.RANGE, col.name, tier, conf, f"{col.name} is a percentage (0-100).", ev, min=0, max=100)]


def _not_future(ctx, col, tier, conf, ev):
    return [rule(RuleType.NOT_FUTURE, col.name, tier, conf, f"{col.name} records when something happened and should not be in the future.", ev)]


def _country(ctx, col, tier, conf, ev):
    return [
        rule(RuleType.REGEX, col.name, tier, conf, f"{col.name} is a two-letter country code.", ev, pattern=r"^[A-Z]{2}$"),
        rule(RuleType.LENGTH, col.name, tier, conf, f"{col.name} is a two-letter country code.", ev, exact=2),
    ]


def _phone(ctx, col, tier, conf, ev):
    return [rule(RuleType.REGEX, col.name, tier, conf, f"{col.name} holds phone numbers.", ev, pattern=PHONE_PATTERN, format="phone")]


def _url(ctx, col, tier, conf, ev):
    return [rule(RuleType.REGEX, col.name, tier, conf, f"{col.name} holds URLs.", ev, pattern=URL_PATTERN, format="url")]


def _re(pattern: str) -> re.Pattern:
    return re.compile(pattern, re.IGNORECASE)


_TS_SUFFIX = r"(?:_at|_on|_date|_ts|_time|_timestamp)"

SPECS: list[Spec] = [
    Spec("email", frozenset({"email", "email_address"}), _re(r"(^|_)e?mail(_address)?$"), frozenset({"string"}), _email),
    Spec("birth_date", frozenset({"birth_date", "date_of_birth", "dob", "birthdate"}),
         _re(r"(^|_)(dob|birth_?date|date_of_birth|birthday)$"), frozenset({"date", "timestamp"}), _birth),
    Spec("identifier", frozenset({"identifier", "id", "primary_key"}), _re(r"$^"), frozenset(), _identifier),  # name rule handled separately
    Spec("monetary", frozenset({"monetary_amount", "amount", "currency_amount", "money", "price"}),
         _re(r"(^|_)(amount|amt|price|cost|fee)$"), frozenset({"numeric"}), _non_negative("monetary amount")),
    Spec("count", frozenset({"count", "quantity"}), _re(r"(^|_)(count|cnt|qty|quantity)$"),
         frozenset({"numeric"}), _non_negative("count")),
    Spec("percentage", frozenset({"percentage", "percent"}), _re(r"(^|_)(pct|percent|percentage)$"),
         frozenset({"numeric"}), _percentage),
    Spec("event_time", frozenset({"timestamp", "datetime", "event_time", "created_timestamp", "updated_timestamp"}),
         _re(rf"^(created|updated|modified|inserted|loaded)({_TS_SUFFIX})?$"),
         frozenset({"date", "timestamp"}), _not_future),
    Spec("country_code", frozenset({"iso_country_code", "country_code"}), _re(r"(^|_)country_code$"),
         frozenset({"string"}), _country),
    Spec("phone", frozenset({"phone", "phone_number"}), _re(r"(^|_)(phone|mobile|tel)(_number|_no)?$"),
         frozenset({"string"}), _phone),
    Spec("url", frozenset({"url", "uri", "website"}), _re(r"(^|_)(url|website)$"), frozenset({"string"}), _url),
]


def is_birth_date(col: Column) -> bool:
    spec = next(s for s in SPECS if s.key == "birth_date")
    return (col.semantic_type in spec.slugs) or bool(spec.name.search(col.name))


class SemanticGenerator:
    name = "semantic"

    def generate(self, ctx: Context) -> list[Rule]:
        rules: list[Rule] = []
        table = ctx.metadata.name.lower()
        for col in ctx.metadata.columns:
            family = type_family(col)
            # A semantic type we recognize is authoritative; one we don't (e.g. a
            # generic "date") must not block name-based detection.
            semantic_known = any(col.semantic_type in s.slugs for s in SPECS)
            matched = False
            for spec in SPECS:
                if spec.families and family != "unknown" and family not in spec.families:
                    continue
                if semantic_known and col.semantic_type in spec.slugs:
                    rules += spec.build(ctx, col, Tier.SEMANTIC, SEMANTIC_CONFIDENCE,
                                        [f"semantic_type: {col.semantic_type}"])
                    matched = True
                    break
                if not semantic_known and spec.name.search(col.name):
                    rules += spec.build(ctx, col, Tier.INFERRED, NAME_CONFIDENCE,
                                        [f"column name: {col.name}"])
                    matched = True
                    break
            if not matched and not semantic_known and _is_own_id(col.name, table):
                rules += _identifier(ctx, col, Tier.INFERRED, NAME_CONFIDENCE, [f"column name: {col.name}"])
        return rules


def _is_own_id(column: str, table: str) -> bool:
    lowered = column.lower()
    return lowered == "id" or (bool(table) and lowered in {f"{table}_id", f"{table}id"})


__all__ = ["SemanticGenerator", "SPECS", "is_birth_date", "is_temporal", "EMAIL_PATTERN"]
