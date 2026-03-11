import os
import io
import json
import pytest
from unittest.mock import Mock, patch
import pandas as pd

from featureSQL.storage import get_storage, FileSystemStore, GCSStore, StoreType
from featureSQL.dump_bin import pathlib_suffix


# simple in-memory blob/bucket used by several tests
class DummyBlob:
    def __init__(self, name, content=b""):
        self.name = name
        self._content = content
    def exists(self):
        # treat blob as existing only if it has non-empty content
        return bool(self._content)
    def download_as_text(self, encoding="utf-8"):
        return self._content.decode(encoding)
    def download_as_bytes(self):
        return self._content
    def upload_from_string(self, data, content_type=None):
        self._content = data if isinstance(data, bytes) else data.encode("utf-8")

class DummyBucket:
    def __init__(self):
        self.blobs = {}
    def blob(self, name):
        if name not in self.blobs:
            self.blobs[name] = DummyBlob(name)
        return self.blobs[name]
    def list_blobs(self, prefix=None):
        if prefix is None:
            return list(self.blobs.values())
        return [b for n, b in self.blobs.items() if n.startswith(prefix)]

def test_file_system_store(tmp_path):
    store = get_storage("fs")
    assert isinstance(store, FileSystemStore)
    
    # test text
    p = tmp_path / "test.txt"
    store.write_text(str(p), "hello")
    assert store.exists(str(p))
    assert store.read_text(str(p)) == "hello"

    # test bytes
    p2 = tmp_path / "test.bin"
    store.write_bytes(str(p2), b"world")
    assert store.read_bytes(str(p2)) == b"world"
    store.append_bytes(str(p2), b"!")
    assert store.read_bytes(str(p2)) == b"world!"

@patch("google.cloud.storage.Client")
@patch("google.oauth2.service_account.Credentials.from_service_account_info")
def test_gcs_store(mock_creds, mock_client, monkeypatch):
    mock_env = {
        "project_id": "test_project",
    }
    monkeypatch.setenv("GCS_SC_JSON", json.dumps(mock_env))

    # set up mocks
    mock_bucket = Mock()
    mock_client_instance = Mock()
    mock_client_instance.get_bucket.return_value = mock_bucket
    mock_client.return_value = mock_client_instance
    
    mock_blob = Mock()
    mock_bucket.blob.return_value = mock_blob
    mock_blob.exists.return_value = True
    mock_blob.download_as_text.return_value = "hello gcs"

    store = get_storage(StoreType.GCS.value, "my_bucket")
    assert isinstance(store, GCSStore)
    
    # write text
    store.write_text("test.txt", "hello gcs")
    mock_bucket.blob.assert_called_with("test.txt")
    mock_blob.upload_from_string.assert_called_with("hello gcs", content_type="text/plain")

    # read text
    txt = store.read_text("test.txt")
    assert txt == "hello gcs"
    mock_blob.download_as_text.assert_called()

def test_get_storage_invalid():
    with pytest.raises(NotImplementedError):
        get_storage("s3", "my_bucket")
    
    with pytest.raises(NotImplementedError):
        get_storage("vb", "my_bucket")

    with pytest.raises(ValueError):
        get_storage("unknown")


def test_dumpdataall_gcs_errors(monkeypatch):
    """DumpDataAll should raise when prefix contains no matching CSVs."""
    # patch client to dummy implementation with no blobs
    class FakeClient:
        def __init__(self):
            self.buckets = {}
        def get_bucket(self, name):
            b = self.buckets.get(name)
            if not b:
                b = DummyBucket()
                self.buckets[name] = b
            return b

    monkeypatch.setenv("GCS_SC_JSON", json.dumps({"project_id": "p"}))
    monkeypatch.setattr("google.cloud.storage.Client", lambda *args, **kw: FakeClient())
    monkeypatch.setattr("google.oauth2.service_account.Credentials.from_service_account_info", lambda info: None)

    from featureSQL.dump_bin import DumpDataAll
    with pytest.raises(FileNotFoundError):
        DumpDataAll(data_path="nope", dump_dir="mybucket", store_type="gcs")


def test_get_source_data_malformed_dates(tmp_path):
    """_get_source_data should coerce bad date strings and drop them."""
    from featureSQL.dump_bin import DumpDataAll
    # prepare a csv with one good date and one row containing only a time
    p = tmp_path / "foo.csv"
    p.write_text("symbol,date\nA,2020-01-01 09:30:00-04:00\nA, 09:30:00-04:00\n")
    dumper = DumpDataAll(data_path=str(p), dump_dir=str(tmp_path / "out"), store_type="fs", max_workers=1)
    df = dumper._get_source_data(str(p))
    assert pd.api.types.is_datetime64_dtype(df["date"])
    # malformed row should be dropped
    assert len(df) == 1
    assert df["date"].iloc[0].strftime("%Y-%m-%d") == "2020-01-01"


def test_dumpdataall_gcs_roundtrip(monkeypatch):
    """A minimal GCS-backed dataset should produce calendar and instruments."""
    class FakeClient:
        def __init__(self):
            self.buckets = {}
        def get_bucket(self, name):
            if name not in self.buckets:
                self.buckets[name] = DummyBucket()
            return self.buckets[name]

    monkeypatch.setenv("GCS_SC_JSON", json.dumps({"project_id": "p"}))
    # create shared fake client and make Client() return it
    shared_client = FakeClient()
    monkeypatch.setattr("google.cloud.storage.Client", lambda *args, **kw: shared_client)
    monkeypatch.setattr("google.oauth2.service_account.Credentials.from_service_account_info", lambda info: None)

    # prepare blob with sample csv on shared client
    bucket = shared_client.get_bucket("bucketx")
    bucket.blobs["feature-csv/foo.csv"] = DummyBlob(
        "feature-csv/foo.csv", b"symbol,date,open\nAAPL,2020-01-01,1.0\n"
    )

    from featureSQL.dump_bin import DumpDataAll
    dumper = DumpDataAll(
        data_path="feature-csv",
        dump_dir="bucketx",
        store_type="gcs",
        max_workers=1,
        exclude_fields="symbol,date",
    )
    dumper.dump()
    # verify calendar and instruments created
    assert "calendars/day.txt" in bucket.blobs
    assert bucket.blobs["calendars/day.txt"].download_as_bytes().startswith(b"2020-01-01")
    assert "instruments/all.txt" in bucket.blobs
    # debug: list all blob keys
    # also verify that the query service can read from this store
    from featureSQL.duck import DuckQueryService, LRUCache
    from featureSQL.storage import get_storage
    store = get_storage("gcs", "bucketx")
    # debug: inspect glob results
    svc = DuckQueryService(root="", cache=LRUCache(max_symbols=2), store=store)
    # bucket path includes features prefix since root is empty
    df = svc.execute("SELECT open FROM FOO")
    assert not df.empty
    assert df.iloc[0]["open"] == 1.0


def test_dumpdataall_cleans_bad_paths(monkeypatch):
    """Ensure df_files list filters out empty or wrong suffix entries."""
    class FakeClient:
        def __init__(self):
            self.buckets = {}
        def get_bucket(self, name):
            if name not in self.buckets:
                self.buckets[name] = DummyBucket()
            return self.buckets[name]
    monkeypatch.setenv("GCS_SC_JSON", json.dumps({"project_id": "p"}))
    monkeypatch.setattr("google.cloud.storage.Client", lambda *args, **kw: FakeClient())
    monkeypatch.setattr("google.oauth2.service_account.Credentials.from_service_account_info", lambda info: None)

    # fake a glob that returns an empty string and a valid csv path
    # patch the class itself so new store instances are affected
    from featureSQL.storage import GCSStore
    def fake_glob(self, path, pattern):
        return ["", "foo.csv"]
    monkeypatch.setattr(GCSStore, "glob", fake_glob)
    from featureSQL.dump_bin import DumpDataAll
    # call init and ensure df_files is corrected
    dumper = DumpDataAll(data_path="some", dump_dir="bucketx", store_type="gcs")
    assert all(pathlib_suffix(f) == ".csv" for f in dumper.df_files)
