"""Source URIs: one plain string names any dataset, so it works in config files and pipeline code.

    bigquery://project/dataset/table          bigquery://project/dataset  (container)
    snowflake://account/database/schema/table
    databricks://workspace-host/catalog/schema/table
    gs://bucket/path/file.parquet             gs://bucket/path/           (container)
    s3://bucket/key                           az://account/container/path
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import unquote, urlsplit

from dqms.connectors.errors import ConnectorError


@dataclass(frozen=True)
class SourceURI:
    scheme: str
    authority: str  # project / account / bucket / workspace host
    parts: tuple[str, ...]  # remaining path segments
    raw: str

    @property
    def path(self) -> str:
        return "/".join(self.parts)

    @property
    def trailing_slash(self) -> bool:
        return self.raw.endswith("/")

    def __str__(self) -> str:
        return self.raw


def parse_source(text: str) -> SourceURI:
    if not isinstance(text, str) or "://" not in text:
        raise ConnectorError(f"not a source URI: {text!r} (expected e.g. bigquery://project/dataset/table)")
    split = urlsplit(text.strip())
    if not split.scheme or not split.netloc:
        raise ConnectorError(f"source URI needs a scheme and authority: {text!r}")
    if split.query or split.fragment:
        raise ConnectorError("source URIs do not take query strings or fragments; pass options separately")
    parts = tuple(unquote(p) for p in split.path.split("/") if p)
    if any(p in {".", ".."} for p in parts):
        raise ConnectorError("path traversal segments are not allowed in source URIs")
    return SourceURI(split.scheme.lower(), unquote(split.netloc), parts, text.strip())
