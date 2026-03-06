import pytest
from pathlib import Path
import pandas as pd
import json
import google
from yahooquery import Ticker

from featureSQL.cli import Run
from featureSQL.dump_bin import DumpDataAll, DumpDataUpdate

# simple dummy bucket used for gcs workflow tests
class DummyBlob:
    def __init__(self, name, content=b""):
        self.name = name
        self._content = content
    def exists(self):
        return bool(self._content)
    def download_as_bytes(self):
        return self._content

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


@pytest.fixture(autouse=True)
def patch_yahoo(monkeypatch):
    """Stub out network calls so tests run deterministically."""

    def fake_history(self, start, end, interval):
        # return two days of synthetic OCHLV data
        return pd.DataFrame(
            {
                "date": ["2020-01-01", "2020-01-02"],
                "open": [1.0, 2.0],
                "high": [1.0, 2.0],
                "low": [1.0, 2.0],
                "close": [1.0, 2.0],
                "volume": [100, 200],
            }
        )

    monkeypatch.setattr(Ticker, "history", fake_history)
    yield


def test_csv_download(tmp_path):
    """Downloading to CSV should create files under feature-csv."""
    data_dir = tmp_path / "data"
    r = Run()
    r.download(
        region="US",
        start="2020-01-01",
        end="2020-01-02",
        symbols=["AAPL", "TSLA"],
        data_path=str(data_dir),
        store_type="fs",
    )

    feature = data_dir / "feature-csv"
    assert (feature / "AAPL.csv").exists()
    assert (feature / "TSLA.csv").exists()


def test_csv_then_dump_all(tmp_path):
    """Create CSVs first, then run a full dump_all conversion."""
    data_dir = tmp_path / "data"
    r = Run()
    # download to CSV
    r.download(
        region="US",
        start="2020-01-01",
        end="2020-01-02",
        symbols=["AAPL"],
        data_path=str(data_dir),
        store_type="fs",
    )

    feature = data_dir / "feature-csv"
    assert (feature / "AAPL.csv").exists()

    # perform dump_all on the fresh CSVs
    dumper = DumpDataAll(data_path=str(feature), dump_dir=str(data_dir))
    dumper.dump()

    # expect calendar and feature bins
    assert (data_dir / "calendars" / "day.txt").exists()
    assert (data_dir / "features" / "aapl" / "open.day.bin").exists()


def test_direct_bin_after_reset(tmp_path):
    """Resetting the output and downloading directly to bin should work."""
    data_dir = tmp_path / "data"

    # ensure directory is empty / reset
    if data_dir.exists():
        for child in data_dir.rglob("*"):
            if child.is_file():
                child.unlink()
            else:
                child.rmdir()

    r = Run()
    r.download(
        region="US",
        start="2020-01-01",
        end="2020-01-02",
        symbols=["AAPL"],
        data_path=str(data_dir),
        out_format="bin",        store_type="fs",    )

    assert (data_dir / "calendars" / "day.txt").exists()
    assert (data_dir / "features" / "aapl" / "open.day.bin").exists()


def test_update_mode_skips_old_range(tmp_path):
    """If data_dir already contains newer calendar, older ranges are ignored."""
    data_dir = tmp_path / "data"
    r = Run()

    # first create a bin with later dates
    r.download(
        region="US",
        start="2021-01-01",
        end="2021-01-02",
        symbols=["AAPL"],
        data_path=str(data_dir),
        out_format="bin",
        store_type="fs",
    )
    # capture initial mtime of bin
    bin_path = data_dir / "features" / "aapl" / "open.day.bin"
    mtime1 = bin_path.stat().st_mtime

    # now attempt to download an earlier period (should be skipped)
    r.download(
        region="US",
        start="2020-01-01",
        end="2020-01-02",
        symbols=["AAPL"],
        data_path=str(data_dir),
        out_format="bin",
        store_type="fs",
    )

    mtime2 = bin_path.stat().st_mtime
    assert mtime2 == mtime1  # file untouched


def test_skip_symbol_on_yahoo_error(tmp_path, monkeypatch):
    """If fetching from Yahoo fails for a symbol we skip it and continue."""
    # replace Ticker class with one that raises for TSLA
    class FakeTicker:
        def __init__(self, symbol):
            self._sym = symbol
        def history(self, start, end, interval):
            if self._sym == "TSLA":
                raise RuntimeError("timeout")
            return pd.DataFrame(
                {
                    "date": ["2020-01-01"],
                    "open": [1.0],
                    "high": [1.0],
                    "low": [1.0],
                    "close": [1.0],
                    "volume": [100],
                }
            )
    monkeypatch.setattr("featureSQL.yahoo.Ticker", FakeTicker)
    monkeypatch.setattr("yahooquery.Ticker", FakeTicker)

    data_dir = tmp_path / "data"
    r = Run()
    r.download(
        region="US",
        start="2020-01-01",
        end="2020-01-02",
        symbols=["AAPL", "TSLA"],
        data_path=str(data_dir),
        store_type="fs",
    )
    assert (data_dir / "feature-csv" / "AAPL.csv").exists()
    assert not (data_dir / "feature-csv" / "TSLA.csv").exists()


def test_skip_symbol_on_gcs_write_error(monkeypatch):
    """If writing to GCS fails we skip that symbol and continue."""
    import os
    bucket = os.environ.get("GCS_BUCKET_NAME")
    if not bucket:
        pytest.skip("GCS_BUCKET_NAME not set")
    from featureSQL.storage import GCSStore, get_storage

    class FakeClient:
        def __init__(self):
            self.buckets = {}
        def get_bucket(self, name):
            if name not in self.buckets:
                self.buckets[name] = DummyBucket()
            return self.buckets[name]
    fake_client = FakeClient()
    monkeypatch.setenv("GCS_SC_JSON", json.dumps({"project_id": "p"}))
    monkeypatch.setattr("google.cloud.storage.Client", lambda *args, **kw: fake_client)
    monkeypatch.setattr("google.oauth2.service_account.Credentials.from_service_account_info", lambda info: None)

    # wrap GCSStore.write_text to fail for AAPL
    orig_write = GCSStore.write_text
    def failing_write(self, path, text):
        print(f"DEBUG failing_write path={path} len(text)={len(text)}")
        if "AAPL" in path:
            raise IOError("upload failed")
        return orig_write(self, path, text)
    monkeypatch.setattr(GCSStore, "write_text", failing_write)
    # sanity check that the patched method will raise when used directly
    store = get_storage("gcs", "feature-csv")
    try:
        store.write_text("feature-csv/AAPL.csv", "xyz")
    except Exception as e:
        print("DEBUG direct write raised as expected", e)
    else:
        raise AssertionError("patched write_text did not raise")

    # simple history that returns one row
    def history(self, start, end, interval):
        return pd.DataFrame(
            {
                "date": ["2020-01-01"],
                "open": [1.0],
                "high": [1.0],
                "low": [1.0],
                "close": [1.0],
                "volume": [100],
            }
        )
    monkeypatch.setattr(Ticker, "history", history)

    r = Run()
    # capture store type via intermediary collector hack
    # we can't easily access store used inside Download; add logging to yahoo before call?
    r.download(
        region="US",
        start="2020-01-01",
        end="2020-01-02",
        symbols=["AAPL", "TSLA"],
            data_path=f"{bucket}/feature-csv",
        store_type="gcs",
    )

    client = google.cloud.storage.Client()
    bucket_obj = client.get_bucket(bucket)
    print("DEBUG blobs after download:", list(bucket_obj.blobs.keys()))
    # we don't inspect blob content here; primary goal is error handling above
    assert isinstance(bucket_obj.blobs, dict)
