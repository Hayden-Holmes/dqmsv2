"""Rules written as plain sentences, e.g. a steward's "Must not be before order_ts".

Deterministic phrase matching only: a sentence that is not recognised produces nothing rather than a
guess. Cross-column statements only count when the other name is a real column of the same table.
"""

from __future__ import annotations

import re

from dqms.models import Column, Rule, RuleType, Tier
from dqms.rules.base import is_temporal, rule, type_family
from dqms.rules.semantic import EMAIL_PATTERN

CONFIDENCE = 0.9
_NUMBER = r"-?\d+(?:\.\d+)?"
_WORD = r"[A-Za-z_][A-Za-z0-9_]*"

_FUTURE = re.compile(
    r"\b(?:cannot|can\s*not|can't|must\s+not|should\s+not|may\s+not)\s+be\s+(?:in\s+)?the\s+future\b|\bnot\s+in\s+the\s+future\b",
    re.IGNORECASE,
)
_ORDER = re.compile(
    rf"\b(?P<modal>must|should|cannot|can\s*not|can't)\s+(?P<neg>not\s+)?be\s+(?P<eq>on\s+or\s+)?"
    rf"(?P<dir>before|after|earlier\s+than|later\s+than|prior\s+to)\s+(?P<ref>{_WORD})",
    re.IGNORECASE,
)
_EXCEED = re.compile(rf"\b(?:must|should|cannot|can\s*not|can't)\s+(?:not\s+)?exceed\s+(?P<ref>{_WORD})", re.IGNORECASE)
_BOUND = re.compile(
    r"(?P<phrase>greater\s+than\s+or\s+equal\s+to|at\s+least|no\s+less\s+than|not\s+less\s+than|minimum\s+of|>=|"
    r"greater\s+than|more\s+than|above|>|"
    r"less\s+than\s+or\s+equal\s+to|at\s+most|no\s+more\s+than|not\s+more\s+than|no\s+greater\s+than|maximum\s+of|<=|"
    rf"less\s+than|below|under|<)\s*(?P<arg>{_NUMBER}|{_WORD})",
    re.IGNORECASE,
)
_OR_MORE = re.compile(rf"(?P<n>{_NUMBER}|zero)\s+or\s+(?P<dir>more|greater|higher|less|fewer|lower)\b", re.IGNORECASE)
_EMAIL = re.compile(r"\bvalid\s+e-?mail\b", re.IGNORECASE)
_UNIQUE = re.compile(r"^\s*(?:must\s+be\s+)?unique\b|\b(?:must|should)\s+be\s+unique\b", re.IGNORECASE)
_REQUIRED = re.compile(
    r"\b(?:required|mandatory)\b(?!\s+(?:if|when|for|unless|only))|\b(?:must|should)\s+not\s+be\s+(?:null|empty|blank)\b|\bcannot\s+be\s+(?:null|empty|blank)\b",
    re.IGNORECASE,
)

# phrase -> (bound, inclusive)
_PHRASES = {
    "greater than or equal to": ("min", True), "at least": ("min", True), "no less than": ("min", True),
    "not less than": ("min", True), "minimum of": ("min", True), ">=": ("min", True),
    "greater than": ("min", False), "more than": ("min", False), "above": ("min", False), ">": ("min", False),
    "less than or equal to": ("max", True), "at most": ("max", True), "no more than": ("max", True),
    "not more than": ("max", True), "no greater than": ("max", True), "maximum of": ("max", True), "<=": ("max", True),
    "less than": ("max", False), "below": ("max", False), "under": ("max", False), "<": ("max", False),
}


def rules_from_statement(col: Column, text: str, columns: dict[str, Column]) -> list[Rule]:
    """Rules stated in one sentence about `col`. `columns` maps lower-case name -> Column for the table."""
    evidence = [f"stated rule: {text!r}"]
    family = type_family(col)
    out: list[Rule] = []

    def add(type_: RuleType, cols: list[str] | str, why: str, **params) -> None:
        out.append(rule(type_, cols, Tier.DOCUMENTED, CONFIDENCE, why, evidence, **params))

    def compare(left: str, op: str, right: str, why: str) -> None:
        add(RuleType.COMPARE, [left, right], why, operator=op)

    def ordered(ref: str, op: str, col_is_left: bool) -> None:
        """Record `col op ref` (or `ref op col`) as an ascending comparison: [smaller, larger] with < or <=."""
        other = columns.get(ref.lower())
        if other is None or other.name == col.name or not _comparable(col, other):
            return
        pair = (col.name, other.name) if col_is_left else (other.name, col.name)
        compare(pair[0], op, pair[1], f"It is stated that {pair[0]} must be {'before' if op == '<' else 'no later than'} {pair[1]}.")

    if _FUTURE.search(text) and family in {"date", "timestamp", "unknown"}:
        add(RuleType.NOT_FUTURE, col.name, f"{col.name} is stated to never be in the future.")

    for m in _ORDER.finditer(text):
        negated = bool(m.group("neg")) or m.group("modal").lower().startswith("can")
        before = re.sub(r"\s+", " ", m.group("dir").lower()) in {"before", "earlier than", "prior to"}
        inclusive = bool(m.group("eq")) or negated  # "not before X" means >= X, "not after X" means <= X
        op = "<=" if inclusive else "<"
        if before and not negated:
            ordered(m.group("ref"), op, col_is_left=True)   # col < ref
        elif before:
            ordered(m.group("ref"), op, col_is_left=False)  # not before: ref <= col
        elif not negated:
            ordered(m.group("ref"), op, col_is_left=False)  # after: ref < col
        else:
            ordered(m.group("ref"), op, col_is_left=True)   # not after: col <= ref

    for m in _EXCEED.finditer(text):
        ordered(m.group("ref"), "<=", col_is_left=True)

    bounds: dict[str, int | float] = {}
    exclusive: dict[str, bool] = {}
    for m in _BOUND.finditer(text):
        bound, inclusive = _PHRASES[re.sub(r"\s+", " ", m.group("phrase").lower())]
        arg = m.group("arg")
        if re.fullmatch(_NUMBER, arg):
            if family in {"numeric", "unknown"}:
                bounds[bound], exclusive[bound] = _num(arg), not inclusive
        elif arg.lower() in columns and family in {"numeric", "unknown"}:
            # "at most unit_price" -> col <= ref; "greater than cost" -> cost < col
            ordered(arg, "<=" if inclusive else "<", col_is_left=(bound == "max"))
    for m in _OR_MORE.finditer(text):
        if family in {"numeric", "unknown"}:
            n = 0 if m.group("n").lower() == "zero" else _num(m.group("n"))
            bound = "min" if m.group("dir").lower() in {"more", "greater", "higher"} else "max"
            bounds[bound], exclusive[bound] = n, False
    if bounds:
        params = dict(bounds)
        params |= {f"exclusive_{b}": True for b, strict in exclusive.items() if strict}
        add(RuleType.RANGE, col.name, f"{col.name} has a stated numeric bound.", **params)

    if _EMAIL.search(text) and family in {"string", "unknown"}:
        add(RuleType.REGEX, col.name, f"{col.name} is stated to hold valid email addresses.", pattern=EMAIL_PATTERN, format="email")
    if _UNIQUE.search(text):
        add(RuleType.UNIQUE, col.name, f"{col.name} is stated to be unique.")
    if _REQUIRED.search(text):
        add(RuleType.NOT_NULL, col.name, f"{col.name} is stated to be required.")
    return out


def _comparable(a: Column, b: Column) -> bool:
    fa, fb = type_family(a), type_family(b)
    if "unknown" in (fa, fb):
        return True
    return (is_temporal(a) and is_temporal(b)) or fa == fb == "numeric"


def _num(text: str) -> int | float:
    value = float(text)
    return int(value) if value.is_integer() else value
