"""LLM configuration from the environment. Off unless an operator explicitly turns it on."""

from __future__ import annotations

import os

from dqms.llm.client import LLMClient
from dqms.llm.gateway import LLMMode


def llm_from_env() -> tuple[LLMClient | None, LLMMode]:
    """
    DQMS_LLM_MODE      off (default) | rules | full      see dqms.llm.gateway
    DQMS_LLM_BASE_URL  Azure OpenAI v1 endpoint          required when the mode is not off
    DQMS_LLM_MODEL     deployment name (default gpt-4.1)

    In `off` nothing is imported or constructed. A bad value or missing endpoint raises, so a
    typo can never silently change what leaves the service (nor silently disable what you wanted).
    """
    raw = (os.getenv("DQMS_LLM_MODE") or "off").strip().lower() or "off"  # empty (templated configs) means unset
    try:
        mode = LLMMode(raw)
    except ValueError:
        raise RuntimeError(f"DQMS_LLM_MODE={raw!r} is invalid; use one of: off, rules, full") from None
    if mode is LLMMode.OFF:
        return None, mode
    base_url = os.getenv("DQMS_LLM_BASE_URL")
    if not base_url:
        raise RuntimeError(f"DQMS_LLM_MODE={mode.value} requires DQMS_LLM_BASE_URL")
    from dqms.llm.client import AzureOpenAIClient

    return AzureOpenAIClient(base_url, os.getenv("DQMS_LLM_MODEL", "gpt-4.1")), mode
