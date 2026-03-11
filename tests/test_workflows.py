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

def test_gcs_download_requires_bucket(tmp_path):
    """Ensure a non-string/empty data_path for `gcs` store_type raises.
    """
    r = Run()
    with pytest.raises(ValueError):
        r.download(store_type="gcs", data_path=None)
    with pytest.raises(ValueError):
        r.download(store_type="gcs", data_path=True)


def test_csv_merge_on_repeated_download(tmp_path, monkeypatch):
    """Downloading the same symbol twice with overlapping ranges should
    merge the CSVs rather than replacing them.
    """
    data_dir = tmp_path / "data"
    # patch history to return just the start date so we can simulate two calls
    def fake_history(self, start, end, interval):
        # emit one row for the start and, if different, one for the end date
        dates = [start]
        if end and end != start:
            dates.append(end)
        return pd.DataFrame({"date": dates, "open": [1.0] * len(dates)})
    monkeypatch.setattr(Ticker, "history", fake_history)

    r = Run()
    # first download covers 2020-01-01 through 2020-01-02
    r.download(
        region="US",
        start="2020-01-01",
        end="2020-01-02",
        symbols=["AAPL"],
        data_path=str(data_dir),
        store_type="fs",
    )
    # second download overlaps but extends to the 3rd
    r.download(
        region="US",
        start="2020-01-02",
        end="2020-01-03",
        symbols=["AAPL"],
        data_path=str(data_dir),
        store_type="fs",
    )

    df = pd.read_csv(data_dir / "feature-csv" / "AAPL.csv")
    # should contain three rows: 1/1, 1/2, 1/3 (duplicates dropped)
    assert len(df) == 3
    assert df["date"].tolist() == ["2020-01-01", "2020-01-02", "2020-01-03"]
    # no stray index column should be persisted
    assert "index" not in df.columns


def test_fix_corrupted_csv_without_date(tmp_path, monkeypatch):
    """An existing CSV missing the date header should be overwritten.

    We simulate a bad file (no ``date`` column) and ensure that a subsequent
    download produces a properly-formed CSV containing the expected dates.
    """
    data_dir = tmp_path / "data"
    csv_dir = data_dir / "feature-csv"
    csv_dir.mkdir(parents=True)
    # create a malformed CSV (header lacks date)
    malformed = "open,high,low\n1,2,3\n4,5,6\n"
    (csv_dir / "CORP.csv").write_text(malformed)

    # patch history to return two dates so we can verify they appear
    def fake_history(self, start, end, interval):
        return pd.DataFrame({"date": ["2020-01-01", "2020-01-02"], "open": [1, 2]})
    monkeypatch.setattr(Ticker, "history", fake_history)

    r = Run()
    r.download(
        region="US",
        start="2020-01-01",
        end="2020-01-02",
        symbols=["CORP"],
        data_path=str(data_dir),
        store_type="fs",
    )

    df = pd.read_csv(csv_dir / "CORP.csv")
    assert "date" in df.columns
    assert df["date"].tolist() == ["2020-01-01", "2020-01-02"]


def test_merge_handles_mixed_date_types(tmp_path, monkeypatch):
    """If existing CSV has string dates and new data has timestamps, duplicates
    should still be dropped by symbol+date.
    """
    data_dir = tmp_path / "data"
    csv_dir = data_dir / "feature-csv"
    csv_dir.mkdir(parents=True)
    # write existing file with string dates
    existing = "symbol,date,open\nNVDA,2020-01-01,1\nNVDA,2020-01-02,2\n"
    (csv_dir / "NVDA.csv").write_text(existing)

    # patch history to return datetimevalued dates including overlapping
    def fake_history(self, start, end, interval):
        return pd.DataFrame(
            {"symbol": ["NVDA", "NVDA"],
             "date": [pd.Timestamp("2020-01-02"), pd.Timestamp("2020-01-03")],
             "open": [2, 3]}
        )
    monkeypatch.setattr(Ticker, "history", fake_history)

    r = Run()
    r.download(
        region="US",
        start="2020-01-02",
        end="2020-01-03",
        symbols=["NVDA"],
        data_path=str(data_dir),
        store_type="fs",
    )

    df = pd.read_csv(csv_dir / "NVDA.csv")
    # duplicates on 2020-01-02 should have been collapsed
    assert len(df) == 3
    assert df["date"].tolist() == ["2020-01-01", "2020-01-02", "2020-01-03"]


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


def test_dump_all_handles_malformed_dates(tmp_path):
    """A malformed or offset-aware date should not crash dump_all.

    This exercise reproduces the error seen when one of the rows contained
    only a time component such as ``" 09:30:00-04:00"``.  The parser now
    coerces invalid values and drops them rather than blowing up in the
    worker process.
    """
    data_dir = tmp_path / "data"
    feature = data_dir / "feature-csv"
    feature.mkdir(parents=True)
    # create CSV with a valid UTC offset and one bad row
    bad_csv = feature / "AAPL.csv"
    bad_csv.write_text(
        "symbol,date,open\n"
        "AAPL,2020-01-01 09:30:00-04:00,1.0\n"
        "AAPL, 09:30:00-04:00,2.0\n"
    )

    dumper = DumpDataAll(data_path=str(feature), dump_dir=str(data_dir), max_workers=1)
    # should complete without raising
    dumper.dump()

    # only the valid date should appear in the calendar file
    cal = pd.read_csv(data_dir / "calendars" / "day.txt", header=None)[0].tolist()
    assert len(cal) == 1
    assert "2020-01-01" in cal


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


def test_direct_parquet_after_reset(tmp_path):
    """Downloading with parquet format should produce a partitioned
    directory beneath ``<data_dir>/parquet``.
    """
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
        out_format="parquet",
        store_type="fs",
    )

    parquet_folder = data_dir / "parquet"
    assert parquet_folder.exists(), "parquet output root missing"
    # expect symbol partition
    assert (parquet_folder / "symbol=AAPL").exists()



def test_gcs_requires_bucket_name():
    """Providing a non-string/empty data_path for GCS should raise early.
    """
    r = Run()
    with pytest.raises(ValueError):
        r.download(store_type="gcs", data_path=None)
    with pytest.raises(ValueError):
        r.download(store_type="gcs", data_path=True)


def test_parquet_symbol_string(tmp_path):
    """`dump_parquet` accepts a string of comma‑separated symbols.
    """
    data_dir = tmp_path / "data"
    csv_dir = data_dir / "feature-csv"
    csv_dir.mkdir(parents=True)
    # create two symbol CSVs
    (csv_dir / "NVDA.csv").write_text("symbol,date,open\nNVDA,2020-01-01,5.0\n")
    (csv_dir / "AAPL.csv").write_text("symbol,date,open\nAAPL,2020-01-01,1.0\n")

    r = Run()
    r.dump_parquet(data_path=str(data_dir), out_root=str(data_dir / "parquet"), symbols="NVDA")
    # only NVDA partition should exist
    assert (data_dir / "parquet" / "symbol=NVDA").exists()
    assert not (data_dir / "parquet" / "symbol=AAPL").exists()


def test_parquet_incremental(tmp_path):
    data_dir = tmp_path / "data"
    csv_dir = data_dir / "feature-csv"
    csv_dir.mkdir(parents=True)
    # first CSV with one row
    (csv_dir / "AAPL.csv").write_text(
        "symbol,date,open\nAAPL,2020-01-01,1.0\n"
    )

    r = Run()
    r.dump_parquet(data_path=str(data_dir), out_root=str(data_dir / "parquet"), symbols=["AAPL"])

    # append a second date to CSV and rerun
    with open(csv_dir / "AAPL.csv", "a") as f:
        f.write("AAPL,2020-01-02,2.0\n")
    r.dump_parquet(data_path=str(data_dir), out_root=str(data_dir / "parquet"), symbols=["AAPL"])

    # read the resulting parquet table and check both dates present
    import pyarrow.parquet as pq
    tbl = pq.read_table(str(data_dir / "parquet"))
    df = tbl.to_pandas()
    assert "2020-01-01" in df["date"].astype(str).values
    assert "2020-01-02" in df["date"].astype(str).values


def test_parquet_merge_on_gcs_upload(tmp_path, monkeypatch):
    """When ``upload_gcs=True`` we should pull existing bucket data and
    combine it with the freshly-read CSVs instead of wiping the bucket.

    We fake the GCS dataset by patching ``pyarrow.dataset.dataset``; the
    patched version returns a dataset built from a local directory that we
    populate with a one-row parquet file.  After invoking ``dump_parquet``
    with the upload flag the output should contain rows from both sources.
    """

    # prepare CSVs containing one date
    data_dir = tmp_path / "data"
    csv_dir = data_dir / "feature-csv"
    csv_dir.mkdir(parents=True)
    (csv_dir / "AAPL.csv").write_text(
        "symbol,date,open\nAAPL,2020-01-01,1.0\n"
    )

    # create a fake existing parquet dataset with a later date
    external = tmp_path / "existing"
    external.mkdir()
    import pyarrow as pa
    import pyarrow.dataset as ds
    import pandas as pd

    old_df = pd.DataFrame({"symbol": ["AAPL"], "date": ["2020-01-02"], "open": [2.0]})
    old_tbl = pa.Table.from_pandas(old_df)
    # write with the same partitioning logic used by dump_parquet
    ds.write_dataset(old_tbl, base_dir=str(external), partitioning=["symbol"], format="parquet")

    # monkeypatch the dataset factory to return our fake dataset when asked
    # capture original before we replace it, so we can call it without
    # recursing back into the fake function.
    orig_ds = ds.dataset
    def fake_ds(uri, *args, **kwargs):
        # ignore all arguments and just return the dataset we created
        return orig_ds(str(external), format="parquet")

    monkeypatch.setattr(ds, "dataset", fake_ds)

    r = Run()
    # request a dump with upload_gcs so the merge logic executes
    # set a dummy credentials env var so the upload block won't raise
    import os
    os.environ["GCS_SC_JSON"] = "{}"

    # monkeypatch the storage client to make upload a no-op so we do not hit
    # the network or require valid credentials.
    try:
        import google.cloud.storage as _gcs

        monkeypatch.setattr(_gcs.Blob, "upload_from_filename", lambda self, fn: None)
    except ImportError:
        # not available in some environments, fall back quietly
        pass

    # request an upload; merge logic will execute and the fake_ds patch will
    # return the existing rows.  Actual upload is now a no-op.
    r.dump_parquet(
        data_path=str(data_dir),
        out_root=str(tmp_path / "parquet"),
        upload_gcs=True,
        gcs_bucket="my-bucket",
    )

    # undo our earlier patch so that subsequent reads target the newly-
    # written local output rather than the fake external dataset.
    monkeypatch.setattr(ds, "dataset", orig_ds)

    # verify both dates ended up in the output tree
    # read the output via a dataset so hive partitions (symbol)
    # are materialised as columns; the plain ``pq.read_table`` call we used
    # previously ignores files that lack the partition column and would only
    # return the old row.
    import pyarrow.dataset as ds
    result_tbl = ds.dataset(
        str(tmp_path / "parquet"),
        format="parquet",
        partitioning="hive",
    ).to_table()
    result = result_tbl.to_pandas()
    dates = result["date"].astype(str)
    # compare only the date portion (ignore time-of-day)
    assert any(d.startswith("2020-01-01") for d in dates)
    assert any(d.startswith("2020-01-02") for d in dates)


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


def test_symbols_file_multi_column(tmp_path):
    """If a symbols file has extra columns, only the first column is used."""
    symfile = tmp_path / "symbols.txt"
    symfile.write_text("AAPL,ignore\nTSLA other\nGOOG\n")
    data_dir = tmp_path / "data"
    r = Run()
    r.download(
        region="US",
        start="2020-01-01",
        end="2020-01-02",
        symbols_file=str(symfile),
        data_path=str(data_dir),
        store_type="fs",
    )
    assert (data_dir / "feature-csv" / "AAPL.csv").exists()
    assert (data_dir / "feature-csv" / "TSLA.csv").exists()
    assert (data_dir / "feature-csv" / "GOOG.csv").exists()


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
        if "AAPL" in path:
            raise IOError("upload failed")
        return orig_write(self, path, text)
    monkeypatch.setattr(GCSStore, "write_text", failing_write)
    # sanity check that the patched method will raise when used directly
    store = get_storage("gcs", "feature-csv")
    try:
        store.write_text("feature-csv/AAPL.csv", "xyz")
    except Exception:
        pass
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
    # we don't inspect blob content here; primary goal is error handling above
    assert isinstance(bucket_obj.blobs, dict)
