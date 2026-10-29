"""Last-line scrub of text bound for an LLM: obvious personal data and credentials.

This is a backstop, not the control. The control is that payloads are built from
structural metadata only (see rules/llm.py); this catches what a human typed into a
column description or a document ("e.g. jane@corp.com", "password=...").
"""

from __future__ import annotations

import re

_SECRET = re.compile(r"(?i)\b(api[_-]?key|secret|token|passw(?:or)?d|pwd|authorization)\b(\s*[:=]\s*|\s+)(\"?)[^\s\"',;]{4,}")
_EMAIL = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
_SSN = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
_CARD = re.compile(r"\b(?:\d[ -]?){12,18}\d\b")
_CLOUD_KEY = re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b|\bAIza[0-9A-Za-z_\-]{35}\b|\bgh[pousr]_[0-9A-Za-z]{30,}\b")
_LONG_NUMBER = re.compile(r"\b\d{10,}\b")  # account / phone / national-id style numbers


def redact(text: str) -> tuple[str, dict[str, int]]:
    """Return (scrubbed text, how many of each kind were replaced). Never echoes what it removed."""
    counts: dict[str, int] = {}

    def sub(kind: str, pattern: re.Pattern, source: str, check=None) -> str:
        def repl(match: re.Match) -> str:
            if check is not None and not check(match.group(0)):
                return match.group(0)
            counts[kind] = counts.get(kind, 0) + 1
            return f"[REDACTED:{kind}]"

        return pattern.sub(repl, source)

    text = sub("secret", _SECRET, text)
    text = sub("cloud_key", _CLOUD_KEY, text)
    text = sub("email", _EMAIL, text)
    text = sub("ssn", _SSN, text)
    text = sub("card", _CARD, text, check=_luhn)
    text = sub("number", _LONG_NUMBER, text)
    return text, counts


def _luhn(candidate: str) -> bool:
    digits = [int(c) for c in candidate if c.isdigit()]
    if not 13 <= len(digits) <= 19:
        return False
    total = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2:
            d = d * 2 - 9 if d > 4 else d * 2
        total += d
    return total % 10 == 0
