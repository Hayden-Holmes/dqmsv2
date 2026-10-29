"""The single gate in front of any LLM.

Nothing else in the codebase holds an LLM client: the engine and the normalizer receive
an `LLMGateway`, and the gateway enforces, in one place:

  mode    off    no LLM is ever constructed or called (the default)
          rules  the model sees *structural metadata only* (names, types, constraints,
                 descriptions); never profile values, never document contents
          full   additionally lets unknown-format metadata *documents* be sent for parsing
  purpose a call declares why it is calling; a purpose the mode does not allow is refused
  size    payloads over the cap are refused, not truncated
  scrub   obvious personal data / credentials are redacted from what is sent
  audit   every call is logged (purpose, size, hash, redaction counts), never its content

`off` is a deployment property, not a request property: a request can only ask for *less*
LLM use than the server allows.
"""

from __future__ import annotations

import hashlib
import logging
from enum import Enum
from typing import Any

from dqms.llm.client import LLMClient
from dqms.llm.redact import redact

log = logging.getLogger("dqms.llm.egress")
DEFAULT_MAX_PAYLOAD_CHARS = 60_000


class LLMMode(str, Enum):
    OFF = "off"
    RULES = "rules"
    FULL = "full"


class Purpose(str, Enum):
    RULES = "rules"  # propose extra rules from structural metadata
    DOCUMENT = "document"  # parse an unknown-format metadata document


class EgressDenied(RuntimeError):
    """The gateway refused to send something. The message is safe to show to the caller."""


_ALLOWED = {
    LLMMode.OFF: frozenset(),
    LLMMode.RULES: frozenset({Purpose.RULES}),
    LLMMode.FULL: frozenset({Purpose.RULES, Purpose.DOCUMENT}),
}


class LLMGateway:
    def __init__(self, client: LLMClient, mode: LLMMode, max_payload_chars: int = DEFAULT_MAX_PAYLOAD_CHARS):
        if mode is LLMMode.OFF:
            raise ValueError("an LLMGateway cannot exist in 'off' mode; use build_gateway()")
        self._client, self.mode, self._max = client, mode, max_payload_chars

    def allows(self, purpose: Purpose) -> bool:
        return purpose in _ALLOWED[self.mode]

    def complete_json(self, purpose: Purpose, system: str, user: str) -> Any:
        if not self.allows(purpose):
            raise EgressDenied(f"LLM purpose '{purpose.value}' is not permitted in mode '{self.mode.value}'")
        scrubbed, counts = redact(user)
        if len(scrubbed) > self._max:
            raise EgressDenied(
                f"payload is {len(scrubbed):,} characters, over the {self._max:,} limit; nothing was sent"
            )
        log.info("llm_egress", extra={
            "purpose": purpose.value, "mode": self.mode.value, "chars": len(scrubbed),
            "sha256": hashlib.sha256(scrubbed.encode()).hexdigest()[:16], "redactions": counts,
        })
        return self._client.complete_json(system, scrubbed)


def build_gateway(client: LLMClient | None, mode: LLMMode) -> LLMGateway | None:
    """None when LLM use is off or unconfigured, so downstream code has no client to misuse."""
    if client is None or mode is LLMMode.OFF:
        return None
    return LLMGateway(client, mode)
