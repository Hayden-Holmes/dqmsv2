"""Rules stated in column descriptions / comments (value lists, ISO codes, bounds)."""

from __future__ import annotations

import re

from dqms.models import Rule, RuleType, Tier
from dqms.rules.base import Context, rule, type_family
from dqms.rules.statements import rules_from_statement

_ENUM = re.compile(
    r"(?:allowed|valid|accepted|permitted|possible)\s+values?\s*(?:are|is)?\s*[:\-]?\s*(?P<list>[^\n]+)"
    r"|one\s+of\s*[:\-]?\s*(?P<list2>[^\n]+)",
    re.IGNORECASE,
)
_BETWEEN = re.compile(
    r"between\s+(?P<lo>-?\d+(?:\.\d+)?)\s+(?:and|to)\s+(?P<hi>-?\d+(?:\.\d+)?)", re.IGNORECASE
)
_NON_NEGATIVE = re.compile(r"\bnon-?\s?negative\b|\bgreater than or equal to (?:0|zero)\b", re.IGNORECASE)
_POSITIVE = re.compile(r"\b(?:strictly\s+)?positive\b|\bgreater than (?:0|zero)\b", re.IGNORECASE)

_REFERENCE_CODES = [
    # (pattern, reference set id, format regex, fixed length)
    (re.compile(r"ISO\s*3166-?1?\s*alpha-?2", re.IGNORECASE), "iso_3166_1_alpha_2", r"^[A-Z]{2}$", 2),
    (re.compile(r"ISO\s*3166-?1?\s*alpha-?3", re.IGNORECASE), "iso_3166_1_alpha_3", r"^[A-Z]{3}$", 3),
    (re.compile(r"ISO\s*4217", re.IGNORECASE), "iso_4217", r"^[A-Z]{3}$", 3),
]


class DocumentedGenerator:
    name = "documented"

    def generate(self, ctx: Context) -> list[Rule]:
        rules: list[Rule] = []
        columns = {c.name.lower(): c for c in ctx.metadata.columns}
        for col in ctx.metadata.columns:
            # Stated business rules (e.g. from catalog aspects) are read like descriptions, plus full sentences.
            for text in [t for t in [col.description, *col.rules] if t]:
                rules += self._from_text(col, text)
            for statement in col.rules:
                rules += rules_from_statement(col, statement, columns)
        return rules

    def _from_text(self, col, text: str) -> list[Rule]:
        rules: list[Rule] = []
        evidence = [f"text: {text!r}"]
        family = type_family(col)

        values = _extract_values(text)
        if values:
            rules.append(
                rule(RuleType.ACCEPTED_VALUES, col.name, Tier.DOCUMENTED, 0.9,
                     f"The description of {col.name} lists its allowed values.", evidence, values=values)
            )

        for pattern, ref_set, fmt, length in _REFERENCE_CODES:
            if pattern.search(text):
                rules += [
                    rule(RuleType.ACCEPTED_VALUES, col.name, Tier.DOCUMENTED, 0.9,
                         f"{col.name} is documented as a standard code list ({ref_set}).", evidence,
                         reference_set=ref_set),
                    rule(RuleType.REGEX, col.name, Tier.DOCUMENTED, 0.9,
                         f"{col.name} must match the {ref_set} code format.", evidence, pattern=fmt),
                    rule(RuleType.LENGTH, col.name, Tier.DOCUMENTED, 0.9,
                         f"{ref_set} codes are exactly {length} characters.", evidence, exact=length),
                ]
                break

        if family in {"numeric", "unknown"}:
            between = _BETWEEN.search(text)
            if between:
                rules.append(
                    rule(RuleType.RANGE, col.name, Tier.DOCUMENTED, 0.85,
                         f"The description of {col.name} states its bounds.", evidence,
                         min=_num(between.group("lo")), max=_num(between.group("hi")))
                )
            elif _NON_NEGATIVE.search(text):
                rules.append(
                    rule(RuleType.RANGE, col.name, Tier.DOCUMENTED, 0.85,
                         f"{col.name} is documented as non-negative.", evidence, min=0)
                )
            elif _POSITIVE.search(text):
                rules.append(
                    rule(RuleType.RANGE, col.name, Tier.DOCUMENTED, 0.85,
                         f"{col.name} is documented as positive.", evidence, min=0, exclusive_min=True)
                )
        return rules

def _extract_values(text: str) -> list[str] | None:
    match = _ENUM.search(text)
    if not match:
        return None
    raw = match.group("list") or match.group("list2") or ""
    raw = raw.strip().rstrip(".;")
    parts = [p.strip().strip("'\"`") for p in re.split(r",|\bor\b|\||/", raw)]
    values = [p for p in parts if p]
    # Guard against prose: real enumerations are short tokens.
    if not (2 <= len(values) <= 50) or any(len(v) > 40 or len(v.split()) > 3 for v in values):
        return None
    return list(dict.fromkeys(values))


def _num(text: str) -> int | float:
    value = float(text)
    return int(value) if value.is_integer() else value
