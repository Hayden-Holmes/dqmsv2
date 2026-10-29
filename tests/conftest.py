from pathlib import Path
from typing import Any

import pytest

from dqms.normalize.documents import SourceDoc
from dqms.normalize.files import read_upload

FIXTURES = Path(__file__).parent / "fixtures"


def load_fixture_docs(*names: str) -> list[SourceDoc]:
    """Fixtures are stored as .txt; give them the extension a user's file would have."""
    docs: list[SourceDoc] = []
    for path in sorted(FIXTURES.glob("*.txt")):
        if names and path.stem not in names:
            continue
        real_name = "ddl.sql" if path.stem == "ddl" else f"{path.stem}.json"
        docs += read_upload(real_name, path.read_bytes())
    return docs


@pytest.fixture
def all_docs() -> list[SourceDoc]:
    return load_fixture_docs()


class FakeLLM:
    """Scripted LLM: returns queued replies (or calls a function) and records prompts."""

    def __init__(self, *replies: Any, fn=None):
        self.replies = list(replies)
        self.fn = fn
        self.calls: list[tuple[str, str]] = []

    def complete_json(self, system: str, user: str) -> Any:
        self.calls.append((system, user))
        if self.fn:
            return self.fn(system, user)
        reply = self.replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply
