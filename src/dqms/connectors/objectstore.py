"""Cloud storage connector (GCS, S3, Azure Blob): profile files in place with ranged reads.

URIs:  gs://bucket/path/file.csv   s3://bucket/key.parquet   az://account/container/path/file.jsonl
A trailing slash (or a path with no file extension) is a container: it expands to the files in it.

Per-file handling:
  CSV/TSV, JSONL   read up to `max_bytes` (and `max_rows`), infer types, aggregate profile; marked
                   `sampled` when the file was larger than what was read.
  Parquet          schema and statistics come from the footer alone (exact, no data scan). Needs
                   `pip install 'dqms[files]'`.
Credentials are the service's own (ADC / AWS default chain / DefaultAzureCredential).
"""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass
from typing import Any, Callable, Protocol

from dqms.connectors.base import BrowseResult, ConnectorError, DatasetRef, ExplorerNode, ProfileOptions, Snapshot
from dqms.connectors.rowprofile import RowProfiler
from dqms.connectors.uri import SourceURI
from dqms.models import Column, ColumnProfile, Metadata, Profile

SUPPORTED = {"csv", "tsv", "jsonl", "ndjson", "parquet"}
READ_AHEAD = 256 * 1024
BROWSE_LISTING_LIMIT = 5000  # objects scanned to build one folder view


@dataclass
class ObjectInfo:
    key: str
    size: int


class ObjectStore(Protocol):
    def list(self, container: str, prefix: str, limit: int) -> list[ObjectInfo]: ...

    def read_range(self, container: str, key: str, start: int, length: int) -> bytes: ...


StoreFactory = Callable[[str, str], ObjectStore]  # (scheme, authority) -> store


class ObjectStoreConnector:
    schemes = ("gs", "s3", "az")

    def __init__(self, store_factory: StoreFactory | None = None):
        self._factory = store_factory or default_store

    # ----------------------------------------------------------------- #

    def is_container(self, uri: SourceURI) -> bool:
        _, key = _locate(uri)
        return uri.trailing_slash or not key or _ext(key) not in SUPPORTED

    def discover(self, uri: SourceURI, limit: int = 500) -> list[DatasetRef]:
        container, key = _locate(uri)
        prefix = key if not key or key.endswith("/") else key + "/"
        store = self._factory(uri.scheme, uri.authority)
        found: list[DatasetRef] = []
        for obj in store.list(container, prefix, limit * 4):  # over-fetch: not every object is a data file
            if _ext(obj.key) in SUPPORTED and obj.size > 0:
                found.append(DatasetRef(uri=_uri(uri, container, obj.key), name=obj.key.rsplit("/", 1)[-1], bytes=obj.size))
                if len(found) >= limit:
                    break
        return found

    def browse(self, uri: SourceURI, limit: int = 500) -> BrowseResult:
        """One folder level: sub-folders that contain data files, and the data files directly inside.
        Listing only; no object is read. Very large folders list partially (`truncated`)."""
        if uri.scheme == "az" and not uri.parts:
            raise ConnectorError("include the container: az://account/container")
        container, key = _locate(uri)
        if key and _ext(key) in SUPPORTED and not uri.trailing_slash:
            raise ConnectorError("a file has no children")
        prefix = key if not key or key.endswith("/") else key + "/"
        listing = self._factory(uri.scheme, uri.authority).list(container, prefix, BROWSE_LISTING_LIMIT)
        folders: dict[str, bool] = {}  # name -> contains at least one data file
        files: list[ExplorerNode] = []
        for obj in listing:
            rest = obj.key[len(prefix):]
            if not rest:
                continue
            is_data = _ext(obj.key) in SUPPORTED and obj.size > 0
            if "/" in rest:
                name = rest.split("/", 1)[0]
                folders[name] = folders.get(name, False) or is_data
            elif is_data:
                files.append(ExplorerNode(uri=_uri(uri, container, obj.key), name=rest, kind="dataset", bytes=obj.size))
        nodes = [ExplorerNode(uri=_uri(uri, container, f"{prefix}{n}/"), name=n + "/", kind="container")
                 for n, has_data in sorted(folders.items()) if has_data] + sorted(files, key=lambda n: n.name)
        return BrowseResult(nodes=nodes[:limit], truncated=len(nodes) > limit or len(listing) >= BROWSE_LISTING_LIMIT)

    def fetch(self, uri: SourceURI, profile: ProfileOptions) -> Snapshot:
        container, key = _locate(uri)
        ext = _ext(key)
        if ext not in SUPPORTED:
            raise ConnectorError(f"unsupported file type '.{ext}'; supported: {', '.join(sorted(SUPPORTED))}")
        store = self._factory(uri.scheme, uri.authority)
        size = next((o.size for o in store.list(container, key, 5) if o.key == key), None)
        if size is None:
            raise ConnectorError(f"object not found: {uri.raw}")
        name = key.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        if ext == "parquet":
            return _parquet(store, container, key, size, name, profile)
        data = store.read_range(container, key, 0, min(size, profile.max_bytes))
        truncated = size > profile.max_bytes
        if truncated:
            data = data[: data.rfind(b"\n") + 1] or data  # drop the cut-off final row
        return _rows(data, ext, name, truncated, profile)


# ------------------------------------------------------------------- #
# Row-oriented formats
# ------------------------------------------------------------------- #


def _rows(data: bytes, ext: str, name: str, truncated: bool, profile: ProfileOptions) -> Snapshot:
    text = data.decode("utf-8-sig", errors="replace")
    if ext in {"jsonl", "ndjson"}:
        profiler, hit_cap = _read_jsonl(text, profile.max_rows)
    else:
        profiler, hit_cap = _read_csv(text, "\t" if ext == "tsv" else None, profile.max_rows)
    if profiler.rows == 0:
        raise ConnectorError(f"{name}: no data rows found")
    columns, stats = profiler.finish(sampled=truncated or hit_cap, include_values=profile.include_values)
    return Snapshot(Metadata(name=name, columns=columns), stats if profile.enabled else None)


def _read_csv(text: str, delimiter: str | None, max_rows: int) -> tuple[RowProfiler, bool]:
    if delimiter is None:
        try:
            delimiter = csv.Sniffer().sniff(text[:8192], delimiters=",;|\t").delimiter
        except csv.Error:
            delimiter = ","
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    header = next(reader, None)
    if not header:
        raise ConnectorError("file has no header row")
    names = _unique_names(header)
    profiler = RowProfiler(names, parse_text=True)
    for row in reader:
        if profiler.rows >= max_rows:
            return profiler, True
        if not row:
            continue
        row = (row + [None] * len(names))[: len(names)]  # ragged rows: pad / truncate
        profiler.add(row)
    return profiler, False


def _read_jsonl(text: str, max_rows: int) -> tuple[RowProfiler, bool]:
    records: list[dict[str, Any]] = []
    names: dict[str, None] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue  # a bad line should not sink the file; the row count will show it
        if isinstance(obj, dict):
            records.append(obj)
            names.update(dict.fromkeys(obj))
        if len(records) >= max_rows:
            break
    profiler = RowProfiler(list(names), parse_text=False)
    for record in records:
        profiler.add([record.get(n) for n in names])
    return profiler, len(records) >= max_rows


def _unique_names(header: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    names = []
    for i, raw in enumerate(header):
        base = raw.strip() or f"column_{i + 1}"
        count = seen.get(base, 0)
        seen[base] = count + 1
        names.append(base if count == 0 else f"{base}_{count + 1}")
    return names


# ------------------------------------------------------------------- #
# Parquet (footer only)
# ------------------------------------------------------------------- #


def _parquet(store: ObjectStore, container: str, key: str, size: int, name: str, profile: ProfileOptions) -> Snapshot:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise ConnectorError("Parquet support needs: pip install 'dqms[files]'") from exc

    pf = pq.ParquetFile(RangeFile(store, container, key, size))
    md = pf.metadata
    columns = [
        Column(name=f.name, datatype=_arrow_type(f.type, pa), nullable=f.nullable) for f in pf.schema_arrow
    ]
    meta = Metadata(name=name, columns=columns)
    if not profile.enabled:
        return Snapshot(meta, None)

    chunks: dict[str, list[Any]] = {}
    for g in range(md.num_row_groups):
        group = md.row_group(g)
        for c in range(group.num_columns):
            chunk = group.column(c)
            chunks.setdefault(chunk.path_in_schema, []).append(chunk.statistics)

    stats = Profile(row_count=md.num_rows, total_rows=md.num_rows, sampled=False)
    for col in columns:
        entry = ColumnProfile(column=col.name)
        per_group = chunks.get(col.name)  # nested columns have dotted paths and are skipped
        if per_group and all(s is not None and s.has_null_count for s in per_group) and md.num_rows:
            entry.null_rate = sum(s.null_count for s in per_group) / md.num_rows
        # Min/max only for non-string types: string extremes would expose raw values.
        if per_group and col.datatype in {"INTEGER", "FLOAT", "NUMERIC", "DATE", "TIMESTAMP"} \
                and all(s is not None and s.has_min_max for s in per_group):
            entry.min_value = _plain(min(s.min for s in per_group))
            entry.max_value = _plain(max(s.max for s in per_group))
        stats.columns.append(entry)
    return Snapshot(meta, stats)


def _arrow_type(t: Any, pa: Any) -> str:
    types = pa.types
    if types.is_integer(t):
        return "INTEGER"
    if types.is_floating(t):
        return "FLOAT"
    if types.is_decimal(t):
        return "NUMERIC"
    if types.is_string(t) or types.is_large_string(t):
        return "STRING"
    if types.is_boolean(t):
        return "BOOLEAN"
    if types.is_date(t):
        return "DATE"
    if types.is_timestamp(t):
        return "TIMESTAMP"
    if types.is_struct(t) or types.is_map(t):
        return "STRUCT"
    if types.is_list(t) or types.is_large_list(t):
        return "ARRAY"
    return str(t).upper()


def _plain(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if value is not None and not isinstance(value, (int, float, str, bool)):
        return float(value)
    return value


class RangeFile(io.RawIOBase):
    """Seekable read-only file over ranged GETs, with a small read-ahead cache."""

    def __init__(self, store: ObjectStore, container: str, key: str, size: int):
        self._store, self._container, self._key, self._size = store, container, key, size
        self._pos = 0
        self._cache_start, self._cache = 0, b""

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        base = {io.SEEK_SET: 0, io.SEEK_CUR: self._pos, io.SEEK_END: self._size}[whence]
        self._pos = max(0, base + offset)
        return self._pos

    def readinto(self, buffer) -> int:
        want = min(len(buffer), max(0, self._size - self._pos))
        if want == 0:
            return 0
        end = self._cache_start + len(self._cache)
        if not (self._cache_start <= self._pos and self._pos + want <= end):
            fetch = min(self._size - self._pos, max(want, READ_AHEAD))
            self._cache = self._store.read_range(self._container, self._key, self._pos, fetch)
            self._cache_start = self._pos
        offset = self._pos - self._cache_start
        chunk = self._cache[offset : offset + want]
        buffer[: len(chunk)] = chunk
        self._pos += len(chunk)
        return len(chunk)


# ------------------------------------------------------------------- #
# Helpers + real stores
# ------------------------------------------------------------------- #


def _locate(uri: SourceURI) -> tuple[str, str]:
    """(container, key). Azure puts the container in the path; GCS/S3 use the authority."""
    if uri.scheme == "az":
        if not uri.parts:
            raise ConnectorError("expected az://account/container/path")
        return uri.parts[0], "/".join(uri.parts[1:])
    return uri.authority, uri.path


def _uri(base: SourceURI, container: str, key: str) -> str:
    if base.scheme == "az":
        return f"az://{base.authority}/{container}/{key}"
    return f"{base.scheme}://{container}/{key}"


def _ext(key: str) -> str:
    return key.rsplit(".", 1)[-1].lower() if "." in key.rsplit("/", 1)[-1] else ""


def default_store(scheme: str, authority: str) -> ObjectStore:
    if scheme == "gs":
        return GcsStore()
    if scheme == "s3":
        return S3Store()
    return AzureBlobStore(authority)


class GcsStore:
    def __init__(self) -> None:
        try:
            from google.cloud import storage
        except ImportError as exc:
            raise ConnectorError("GCS support needs: pip install 'dqms[gcs]'") from exc
        self._client = storage.Client()

    def list(self, container: str, prefix: str, limit: int) -> list[ObjectInfo]:
        return [ObjectInfo(b.name, b.size or 0) for b in self._client.list_blobs(container, prefix=prefix, max_results=limit)]

    def read_range(self, container: str, key: str, start: int, length: int) -> bytes:
        return self._client.bucket(container).blob(key).download_as_bytes(start=start, end=start + length - 1)


class S3Store:
    def __init__(self) -> None:
        try:
            import boto3
        except ImportError as exc:
            raise ConnectorError("S3 support needs: pip install 'dqms[s3]'") from exc
        self._s3 = boto3.client("s3")

    def list(self, container: str, prefix: str, limit: int) -> list[ObjectInfo]:
        res = self._s3.list_objects_v2(Bucket=container, Prefix=prefix, MaxKeys=min(limit, 1000))
        return [ObjectInfo(o["Key"], o["Size"]) for o in res.get("Contents", [])]

    def read_range(self, container: str, key: str, start: int, length: int) -> bytes:
        res = self._s3.get_object(Bucket=container, Key=key, Range=f"bytes={start}-{start + length - 1}")
        return res["Body"].read()


class AzureBlobStore:
    def __init__(self, account: str) -> None:
        try:
            from azure.identity import DefaultAzureCredential
            from azure.storage.blob import BlobServiceClient
        except ImportError as exc:
            raise ConnectorError("Azure Blob support needs: pip install 'dqms[azure]'") from exc
        self._svc = BlobServiceClient(f"https://{account}.blob.core.windows.net", credential=DefaultAzureCredential())

    def list(self, container: str, prefix: str, limit: int) -> list[ObjectInfo]:
        out: list[ObjectInfo] = []
        for blob in self._svc.get_container_client(container).list_blobs(name_starts_with=prefix):
            out.append(ObjectInfo(blob.name, blob.size or 0))
            if len(out) >= limit:
                break
        return out

    def read_range(self, container: str, key: str, start: int, length: int) -> bytes:
        return self._svc.get_container_client(container).download_blob(key, offset=start, length=length).readall()
