"""LLM access behind a tiny interface so providers and tests are swappable."""

from __future__ import annotations

import json
import re
from typing import Any, Protocol


class LLMClient(Protocol):
    def complete_json(self, system: str, user: str) -> Any:
        """Return the model's reply parsed as JSON. Raises ValueError if it cannot be parsed."""
        ...


def parse_json_reply(content: str | None) -> Any:
    if content is None:
        raise ValueError("model returned no content")
    text = content.strip()
    fenced = re.match(r"^```(?:json)?\s*(.*?)\s*```$", text, re.DOTALL)
    if fenced:
        text = fenced.group(1)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Models occasionally wrap JSON in prose; take the outermost object.
        start, end = text.find("{"), text.rfind("}")
        if start != -1 and end > start:
            return json.loads(text[start : end + 1])
        raise ValueError(f"model reply is not valid JSON: {text[:200]!r}")


class AzureOpenAIClient:
    """Azure OpenAI via Entra ID (no API keys in config). Requires the `azure-llm` extra."""

    def __init__(self, base_url: str, deployment: str, retries: int = 1):
        from azure.identity import DefaultAzureCredential, get_bearer_token_provider
        from openai import OpenAI

        token_provider = get_bearer_token_provider(
            DefaultAzureCredential(), "https://cognitiveservices.azure.com/.default"
        )
        self._client = OpenAI(base_url=base_url, api_key=token_provider)
        self._deployment = deployment
        self._retries = retries

    def complete_json(self, system: str, user: str) -> Any:
        messages = [{"role": "system", "content": system}, {"role": "user", "content": user}]
        last_error: Exception | None = None
        for _ in range(self._retries + 1):
            response = self._client.chat.completions.create(
                model=self._deployment,
                messages=messages,
                temperature=0,
                response_format={"type": "json_object"},
            )
            content = response.choices[0].message.content
            try:
                return parse_json_reply(content)
            except ValueError as exc:
                last_error = exc
                messages += [
                    {"role": "assistant", "content": content or ""},
                    {"role": "user", "content": f"That was not valid JSON ({exc}). Reply with ONLY the JSON object."},
                ]
        raise ValueError(f"LLM did not return valid JSON after retries: {last_error}")
