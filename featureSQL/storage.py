import abc
import os
import json
import io
import fnmatch
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
        parts = bucket_path.split("/", 1)
        self.bucket_name = parts[0]
        self.prefix = parts[1].rstrip("/") if len(parts) == 2 else ""
        self._init_client()

    def _init_client(self):
        from google.cloud import storage
        from google.oauth2 import service_account

        json_string = os.getenv("GCS_SC_JSON")
        if not json_string:
            raise ValueError("GCS_SC_JSON environment variable not set.")
        
        info = json.loads(json_string)
        credentials = service_account.Credentials.from_service_account_info(info)
        self.client = storage.Client(credentials=credentials, project=info.get('project_id'))
        self.bucket = self.client.get_bucket(self.bucket_name)

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
        blob = self.bucket.blob(self._normalize_path(path))
        return blob.exists()

    def mkdir(self, path: str, parents: bool = False, exist_ok: bool = False):
        # GCS is an object store, directories are conceptual
        pass

    def glob(self, path: str, pattern: str) -> List[str]:
        # Very basic glob implementation for GCS
        prefix = self._normalize_path(path)
        if prefix and not prefix.endswith("/"):
            prefix += "/"
            
        blobs = self.bucket.list_blobs(prefix=prefix)
        results = []
        for blob in blobs:
            # We matched the prefix, now apply fnmatch to the tail
            name_tail = blob.name[len(prefix):]
            if not "/" in name_tail and fnmatch.fnmatch(name_tail, pattern):
                results.append(blob.name)
            # If pattern handles recursive "**" or something, it requires more logic
            # For our usage, mostly `glob("*.csv")` inside a directory. 
        return results

    def read_text(self, path: str) -> str:
        blob = self.bucket.blob(self._normalize_path(path))
        return blob.download_as_text(encoding="utf-8")

    def write_text(self, path: str, text: str):
        blob = self.bucket.blob(self._normalize_path(path))
        blob.upload_from_string(text, content_type="text/plain")

    def read_bytes(self, path: str) -> bytes:
        blob = self.bucket.blob(self._normalize_path(path))
        return blob.download_as_bytes()

    def write_bytes(self, path: str, data: bytes):
        blob = self.bucket.blob(self._normalize_path(path))
        blob.upload_from_string(data, content_type="application/octet-stream")

    def append_bytes(self, path: str, data: bytes):
        # GCS objects are immutable. Append requires reading, cat, and rewrite.
        blob = self.bucket.blob(self._normalize_path(path))
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
