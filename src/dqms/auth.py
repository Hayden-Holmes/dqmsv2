"""Caller authentication and per-key source allowlists.

The service reads cloud data with *its own* credentials, so a caller's API key must
also bound *which sources* it may ask for; otherwise any key holder could profile
anything the service account can see. Keys are stored only as SHA-256 hashes.

DQMS_AUTH_FILE (YAML):
    keys:
      - name: airflow-prod
        sha256: <hex of the key>          # produce with: dqms keygen
        sources: ["bigquery://my-proj/*", "gs://my-bucket/exports/*"]

DQMS_ALLOW_ANONYMOUS=1 lets unauthenticated callers upload files (local development);
anonymous callers never get source access.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
from dataclasses import dataclass, field
from fnmatch import fnmatchcase
from pathlib import Path

import yaml


class AuthError(Exception):
    pass


@dataclass(frozen=True)
class Principal:
    name: str
    sources: tuple[str, ...] = field(default_factory=tuple)

    def can_read(self, source: str) -> bool:
        return any(fnmatchcase(source, pattern) for pattern in self.sources)

    def can_browse(self, container: str) -> bool:
        """May this caller open a container in the explorer? Yes if their allowlist could match
        anything inside it (the container itself, or a pattern that points deeper)."""
        base = container.rstrip("/")
        return any(
            fnmatchcase(base + "/", p) or fnmatchcase(base, p) or p.startswith(base + "/") for p in self.sources
        )

    def roots(self) -> list[str]:
        return explorer_roots(self.sources)


ANONYMOUS = Principal("anonymous", ())


@dataclass
class _Key:
    name: str
    sha256: str
    sources: tuple[str, ...]


class Authenticator:
    def __init__(self, keys: list[_Key] | None = None, allow_anonymous: bool = False):
        self._keys = keys or []
        self._allow_anonymous = allow_anonymous

    @classmethod
    def anonymous(cls) -> "Authenticator":
        return cls([], allow_anonymous=True)

    @classmethod
    def from_env(cls) -> "Authenticator":
        allow = os.getenv("DQMS_ALLOW_ANONYMOUS") == "1"
        path = os.getenv("DQMS_AUTH_FILE")
        if not path and not allow:
            raise RuntimeError(
                "Authentication is not configured. Set DQMS_AUTH_FILE to a key file "
                "(create keys with `dqms keygen`), or DQMS_ALLOW_ANONYMOUS=1 for local development."
            )
        keys = cls.load_keys(Path(path)) if path else []
        return cls(keys, allow)

    @staticmethod
    def load_keys(path: Path) -> list[_Key]:
        raw = yaml.safe_load(path.read_text()) or {}
        keys = []
        for entry in raw.get("keys", []):
            digest = str(entry.get("sha256", "")).lower()
            if len(digest) != 64 or not entry.get("name"):
                raise RuntimeError(f"{path}: each key needs a name and a 64-char sha256")
            keys.append(_Key(entry["name"], digest, tuple(entry.get("sources", []))))
        return keys

    def authenticate(self, authorization: str | None) -> Principal:
        if not authorization:
            if self._allow_anonymous:
                return ANONYMOUS
            raise AuthError("missing credentials: send 'Authorization: Bearer <api key>'")
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() != "bearer" or not token.strip():
            raise AuthError("malformed Authorization header; expected 'Bearer <api key>'")
        digest = hash_key(token.strip())
        match = None
        for key in self._keys:  # no early exit: constant work regardless of which key matches
            if hmac.compare_digest(key.sha256, digest):
                match = key
        if match is None:
            raise AuthError("invalid API key")  # an invalid key never degrades to anonymous
        return Principal(match.name, match.sources)


def explorer_roots(patterns: tuple[str, ...] | list[str]) -> list[str]:
    """Where the explorer starts: the deepest literal prefix of each allowlist pattern.

    `bigquery://proj/*` -> `bigquery://proj`;  `gs://b/sales/orders_*` -> `gs://b/sales`;
    an exact dataset `bigquery://p/d/t` -> its parent `bigquery://p/d`.
    """
    roots: set[str] = set()
    for pattern in patterns:
        wildcard = next((i for i, c in enumerate(pattern) if c in "*?["), None)
        head = pattern if wildcard is None else pattern[:wildcard]
        head = head.rstrip("/") if head.endswith("/") and wildcard is not None else head.rsplit("/", 1)[0]
        scheme, sep, rest = head.partition("://")
        authority = rest.split("/", 1)[0]
        if sep and authority and not any(c in authority for c in "*?["):
            roots.add(head)
    return sorted(roots)


def hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def generate_key() -> tuple[str, str]:
    """(plaintext key to hand to the caller, sha256 to store in the auth file)."""
    key = "dqms_" + secrets.token_urlsafe(32)
    return key, hash_key(key)
