import abc
import os
import json
import io
import fnmatch
import logging
from enum import Enum
from pathlib import Path
from typing import List, Union

import pandas as pd
import numpy as np

class StoreType(str, Enum):
    FS = "fs"
    GCS = "gcs"
    S3 = "s3"
    VB = "vb"

class StorageBackend(abc.ABC):
    @abc.abstractmethod
    def exists(self, path: str) -> bool:
        pass

    @abc.abstractmethod
    def mkdir(self, path: str, parents: bool = False, exist_ok: bool = False):
        pass

    @abc.abstractmethod
    def glob(self, path: str, pattern: str) -> List[str]:
        pass

    @abc.abstractmethod
    def read_text(self, path: str) -> str:
        pass

    @abc.abstractmethod
    def write_text(self, path: str, text: str):
        pass

    @abc.abstractmethod
    def read_bytes(self, path: str) -> bytes:
        pass

    @abc.abstractmethod
    def write_bytes(self, path: str, data: bytes):
        pass
    
    @abc.abstractmethod
    def append_bytes(self, path: str, data: bytes):
        pass

    def joinpath(self, *parts) -> str:
        # Override if backend uses different logic
        return "/".join(str(p).strip("/") for p in parts)

class FileSystemStore(StorageBackend):
    def _to_path(self, path: str) -> Path:
        return Path(path).expanduser()

    def exists(self, path: str) -> bool:
        return self._to_path(path).exists()

    def mkdir(self, path: str, parents: bool = False, exist_ok: bool = False):
        self._to_path(path).mkdir(parents=parents, exist_ok=exist_ok)

    def glob(self, path: str, pattern: str) -> List[str]:
        p = self._to_path(path)
        if not p.exists() or not p.is_dir():
            return []
        return [str(x) for x in p.glob(pattern)]

    def read_text(self, path: str) -> str:
        return self._to_path(path).read_text(encoding="utf-8")

    def write_text(self, path: str, text: str):
        self._to_path(path).write_text(text, encoding="utf-8")

    def read_bytes(self, path: str) -> bytes:
        return self._to_path(path).read_bytes()

    def write_bytes(self, path: str, data: bytes):
        self._to_path(path).write_bytes(data)

    def append_bytes(self, path: str, data: bytes):
        with self._to_path(path).open("ab") as f:
            f.write(data)

    def joinpath(self, *parts) -> str:
        # Windows compatibility or generic path concat
        return str(Path(parts[0]).joinpath(*parts[1:]))


class GCSStore(StorageBackend):
    def __init__(self, bucket_path: str):
        # bucket_path may include a base prefix, e.g. "mybucket" or "mybucket/some/dir"
        if not isinstance(bucket_path, str) or not bucket_path:
            raise ValueError(f"invalid bucket_path for GCSStore: {bucket_path!r}")
        parts = bucket_path.split("/", 1)
        self.bucket_name = parts[0]
        self.prefix = parts[1].rstrip("/") if len(parts) == 2 else ""
        self._init_client()

    def _init_client(self):
        # support two authentication modes:
        # 1. service account JSON supplied via GCS_SC_JSON (existing behaviour)
        # 2. HMAC key/secret pair supplied via GCS_KEY_ID and GCS_KEY_SECRET
        #    (useful when running in environments that do not support
        #    service accounts).
        # We support two authentication modes: HMAC key pair and service
        # account JSON.  Historically the presence of HMAC variables would
        # cause us to use `gcsfs.GCSFileSystem`, but this upstream library
        # interprets *any* dict token as a credentials blob and therefore
        # happily tried to parse our service account JSON as an OAuth token,
        # leading to ``KeyError('refresh_token')``.  To avoid accidental
        # mis‑configuration we now prioritise the explicit JSON credential
        # and only fall back to HMAC when no JSON is provided.
        json_string = os.getenv("GCS_SC_JSON")
        if json_string:
            # use service account path even if HMAC variables are also set
            if os.getenv("GCS_KEY_ID") and os.getenv("GCS_KEY_SECRET"):
                logging.getLogger(__name__).warning(
                    "both GCS_SC_JSON and GCS_KEY_ID/GCS_KEY_SECRET are set; "
                    "using service account JSON and ignoring HMAC keys"
                )
            from google.cloud import storage
            from google.oauth2 import service_account

            try:
                info = json.loads(json_string)
            except Exception as e:  # json.JSONDecodeError or similar
                raise ValueError("GCS_SC_JSON is not valid JSON") from e

            try:
                credentials = service_account.Credentials.from_service_account_info(info)
            except KeyError as e:
                raise ValueError(
                    f"invalid service account JSON for GCS (missing key {e!r}); "
                    "are you using the correct credentials or should you switch to fs?"
                ) from e
            self.client = storage.Client(credentials=credentials, project=info.get('project_id'))
            self.bucket = self.client.get_bucket(self.bucket_name)
            self.use_gcsfs = False
            return

        # if no JSON provided, consider HMAC
        key_id = os.getenv("GCS_KEY_ID")
        key_secret = os.getenv("GCS_KEY_SECRET")
        if key_id and key_secret:
            # switch to gcsfs for HMAC-based access; the object store paths
            # are constructed as "bucket_name/path" and the filesystem handles
            # the low-level GET/PUT operations.
            try:
                import gcsfs
            except ImportError:
                raise ImportError("gcsfs is required for HMAC GCS auth")
            token = {"access_key": key_id, "secret_key": key_secret}
            # project may be optional for HMAC
            project = os.getenv("GCS_PROJECT")
            self.fs = gcsfs.GCSFileSystem(project=project, token=token)
            self.use_gcsfs = True
            return

        # neither credentials form was available
        raise ValueError("GCS_SC_JSON environment variable not set and HMAC keys missing.")

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop('client', None)
        state.pop('bucket', None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._init_client()

    def _normalize_path(self, path: str) -> str:
        # strip leading slash and any leading bucket name
        p = str(path).lstrip("/")
        if p.startswith(f"{self.bucket_name}/"):
            p = p[len(self.bucket_name) + 1 :]
        # now add configured prefix if present
        if self.prefix:
            p = f"{self.prefix}/{p}" if p else self.prefix
        return p

    def joinpath(self, *parts) -> str:
        # override to handle prefix and ignore bucket name or its appearance
        # at the start of a segment, including when a prefix is appended.
        cleaned: list[str] = []
        for idx, p in enumerate(parts):
            s = str(p).strip("/")
            if idx == 0:
                # remove leading bucket name if present, with or without prefix
                if s == self.bucket_name:
                    continue
                if s.startswith(self.bucket_name + "/"):
                    s = s[len(self.bucket_name) + 1 :]
                    # fall through; may still have prefix which we'll keep
            cleaned.append(s)
        path = "/".join(cleaned)
        if self.prefix:
            # avoid adding prefix multiple times
            if path.startswith(self.prefix + "/") or path == self.prefix:
                return path
            if path:
                return f"{self.prefix}/{path}"
            return self.prefix
        return path

    def exists(self, path: str) -> bool:
        p = self._normalize_path(path)
        if getattr(self, "use_gcsfs", False):
            return self.fs.exists(f"{self.bucket_name}/{p}")
        blob = self.bucket.blob(p)
        return blob.exists()

    def mkdir(self, path: str, parents: bool = False, exist_ok: bool = False):
        # GCS is an object store, directories are conceptual
        pass

    def glob(self, path: str, pattern: str) -> List[str]:
        # Very basic glob implementation for GCS
        prefix = self._normalize_path(path)
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        results = []
        if getattr(self, "use_gcsfs", False):
            listing = self.fs.ls(f"{self.bucket_name}/{prefix}", detail=True)
            for entry in listing:
                name = entry["name"] if isinstance(entry, dict) else entry
                tail = name[len(self.bucket_name) + 1 + len(prefix) :]
                if "/" not in tail and fnmatch.fnmatch(tail, pattern):
                    results.append(name[len(self.bucket_name) + 1 :])
            return results

        blobs = self.bucket.list_blobs(prefix=prefix)
        for blob in blobs:
            name_tail = blob.name[len(prefix):]
            if not "/" in name_tail and fnmatch.fnmatch(name_tail, pattern):
                results.append(blob.name)
        return results

    def read_text(self, path: str) -> str:
        p = self._normalize_path(path)
        if getattr(self, "use_gcsfs", False):
            with self.fs.open(f"{self.bucket_name}/{p}", "r") as f:
                return f.read()
        blob = self.bucket.blob(p)
        return blob.download_as_text(encoding="utf-8")

    def write_text(self, path: str, text: str):
        p = self._normalize_path(path)
        if getattr(self, "use_gcsfs", False):
            with self.fs.open(f"{self.bucket_name}/{p}", "w") as f:
                f.write(text)
            return
        blob = self.bucket.blob(p)
        blob.upload_from_string(text, content_type="text/plain")

    def read_bytes(self, path: str) -> bytes:
        p = self._normalize_path(path)
        if getattr(self, "use_gcsfs", False):
            with self.fs.open(f"{self.bucket_name}/{p}", "rb") as f:
                return f.read()
        blob = self.bucket.blob(p)
        return blob.download_as_bytes()

    def write_bytes(self, path: str, data: bytes):
        p = self._normalize_path(path)
        if getattr(self, "use_gcsfs", False):
            with self.fs.open(f"{self.bucket_name}/{p}", "wb") as f:
                f.write(data)
            return
        blob = self.bucket.blob(p)
        blob.upload_from_string(data, content_type="application/octet-stream")

    def append_bytes(self, path: str, data: bytes):
        # GCS objects are immutable. Append requires reading, cat, and rewrite.
        p = self._normalize_path(path)
        if getattr(self, "use_gcsfs", False):
            full = f"{self.bucket_name}/{p}"
            if self.fs.exists(full):
                existing = self.fs.open(full, "rb").read()
                new_data = existing + data
            else:
                new_data = data
            with self.fs.open(full, "wb") as f:
                f.write(new_data)
            return
        blob = self.bucket.blob(p)
        if blob.exists():
            existing = blob.download_as_bytes()
            new_data = existing + data
        else:
            new_data = data
        blob.upload_from_string(new_data, content_type="application/octet-stream")


def get_storage(store_type: str, data_path: str = None) -> StorageBackend:
    if store_type == StoreType.FS.value:
        return FileSystemStore()
    elif store_type == StoreType.GCS.value:
        if not data_path:
            raise ValueError("--data_path must be specified as bucket name for GCS")
        return GCSStore(data_path)
    elif store_type in (StoreType.S3.value, StoreType.VB.value):
        raise NotImplementedError(f"Store type '{store_type}' is not yet implemented.")
    else:
        raise ValueError(f"Unknown store type: {store_type}")
