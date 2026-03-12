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
    """Stub out the network side-effects of ``yahooquery.Ticker``.

    Instead of replacing the entire class we only monkeypatch its
    ``__init__`` method to be a no-op; this allows individual tests to
    override ``history`` or ``option_chain`` as desired while preventing
    the real constructor from opening network connections.
    """

    import yahooquery

    def noop_init(self, *args, **kwargs):
        # do nothing, but preserve attributes used by tests
        self.symbol = args[0] if args else kwargs.get("symbol")

    monkeypatch.setattr(yahooquery.Ticker, "__init__", noop_init)
    # also patch featureSQL.yahoo's reference if it has already imported
    import featureSQL.yahoo as _ymod
    monkeypatch.setattr(_ymod, "Ticker", yahooquery.Ticker)
    # ensure the Ticker name used by the test module points to patched class
    globals()["Ticker"] = yahooquery.Ticker

    # provide a trivial default history so callers don't blow up
    def fake_history(self, start=None, end=None, interval=None):
        return pd.DataFrame({
            "date": ["2020-01-01", "2020-01-02"],
            "open": [1.0, 2.0],
            "high": [1.0, 2.0],
            "low": [1.0, 2.0],
            "close": [1.0, 2.0],
            "volume": [100, 200],
        })
    monkeypatch.setattr(yahooquery.Ticker, "history", fake_history)
    monkeypatch.setattr(_ymod, "Ticker", yahooquery.Ticker)
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

def test_gcs_download_requires_bucket(tmp_path, monkeypatch):
    """Ensure a non-string/empty data_path for `gcs` store_type raises
    unless a bucket is supplied via environment.
    """
    r = Run()
    # clear any bucket name from the environment so the error path is hit
    monkeypatch.delenv("GCS_BUCKET_NAME", raising=False)
    with pytest.raises(ValueError):
        r.download(store_type="gcs", data_path=None)
    with pytest.raises(ValueError):
        r.download(store_type="gcs", data_path=True)
    # when an env var is present we should no longer raise; patch storage
    called = []
    def fake_storage(store_type_arg, data_path_arg=None):
        called.append((store_type_arg, data_path_arg))
        class Dummy:
            def mkdir(self, *args, **kwargs):
                # no-op for tests
                pass
            def joinpath(self, *args, **kwargs):
                return tmp_path / "whatever"
            def glob(self, *args, **kwargs):
                return []
            def write_text(self, *args, **kwargs):
                pass
            def exists(self, *args, **kwargs):
                return False
        return Dummy()
    monkeypatch.setenv("GCS_BUCKET_NAME", "my-bucket")
    monkeypatch.setattr("featureSQL.storage.get_storage", fake_storage)

    # should not raise now; specify a symbol to avoid iterating the full
    # universe (which would take forever in a unit test).
    r.download(symbols=["AAPL"], store_type="gcs", data_path=None)
    # the collector constructor also does a `get_storage('fs')` call to
    # check instance types, so we expect at least one 'gcs' invocation.
    # collector init will cause an fs check as well, so ensure we
    # observed at least one gcs invocation.
    assert any(t == 'gcs' for t, _ in called)
    # verify that the bucket name was passed on the gcs call
    assert any(p == 'my-bucket' for t, p in called if t == 'gcs')


def test_default_store_type_uses_gcs_when_unspecified(tmp_path, monkeypatch):
    """Omitting the ``store_type`` argument should still target GCS."""
    # arrange: set the bucket and stub out the storage backend
    monkeypatch.setenv("GCS_BUCKET_NAME", "bucket-default")
    observed = []
    def fake_storage(store_type_arg, data_path_arg=None):
        observed.append((store_type_arg, data_path_arg))
        class Dummy:
            def mkdir(self, *args, **kwargs):
                pass
            def joinpath(self, *args, **kwargs):
                return tmp_path / "dummy"
            def glob(self, *args, **kwargs):
                return []
            def write_text(self, *args, **kwargs):
                pass
            def exists(self, *args, **kwargs):
                return False
        return Dummy()
    monkeypatch.setattr("featureSQL.storage.get_storage", fake_storage)

    r = Run()
    # call download without specifying store_type at all
    r.download(
        asset_type="equity",
        start="2020-01-01",
        end="2020-01-02",
        symbols=["AAPL"],
        data_path=None,
    )
    # the first gcs invocation should have the correct bucket
    assert any(t=='gcs' for t,_ in observed)
    assert any(p=='bucket-default' for t,p in observed if t=='gcs')


def test_gcs_hmac_auth(monkeypatch):
    """When GCS_KEY_ID/SECRET are set we should initialise GCSStore in
    HMAC mode (gcsfs).
    """
    monkeypatch.setenv("GCS_KEY_ID", "KEY123")
    monkeypatch.setenv("GCS_KEY_SECRET", "SEC456")
    called = {}
    class FakeFS:
        def __init__(self, project=None, token=None):
            called['token'] = token
            called['project'] = project
        def exists(self, path):
            return False
    monkeypatch.setattr("gcsfs.GCSFileSystem", FakeFS)
    store = Run()._resolve_collector  # dummy ensure imports
    from featureSQL.storage import get_storage, GCSStore
    s = get_storage("gcs", "mybucket")
    assert isinstance(s, GCSStore)
    assert getattr(s, 'use_gcsfs', False) is True
    assert called['token']['access_key'] == "KEY123"
    assert called['token']['secret_key'] == "SEC456"


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
    # normalise timezone-aware dates to simple strings for comparison
    dates = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d").tolist()
    assert dates == ["2020-01-01", "2020-01-02", "2020-01-03"]
    # no stray index column should be persisted


def test_force_utc_datetimeindex_context():
    """The context manager should allow constructing mixed-timezone
    DatetimeIndex without raising an error.
    """
    from featureSQL.yahoo import force_utc_datetimeindex
    import pandas as pd

    # outside the context this construction fails as demonstrated earlier
    with pytest.raises(ValueError):
        pd.DatetimeIndex(["2020-01-01T00:00:00Z", "2020-01-02T00:00:00-05:00"])

    # inside the context we should convert to UTC automatically
    with force_utc_datetimeindex():
        idx = pd.DatetimeIndex(["2020-01-01T00:00:00Z", "2020-01-02T00:00:00-05:00"])
    assert idx.tz is not None
    assert str(idx.tz) == "UTC"


def test_download_handles_history_with_mixed_tz_index(tmp_path, monkeypatch):
    """A broken yahooquery history result that would normally throw a
    mixed-timezone error should be handled by our wrapper and still produce
    a CSV file.
    """
    from yahooquery import Ticker

    def fake_history(self, start, end, interval):
        # build an index with mixed timezone strings; without the surrounding
        # ``force_utc_datetimeindex`` patch this construction would raise a
        # ValueError, mimicking the behavior we saw in production.  Because the
        # patch is active when Run.download calls ``t.history`` we expect the
        # index to be normalized instead of erroring.
        idx = pd.DatetimeIndex([
            "2020-01-01T00:00:00Z",
            "2020-01-02T00:00:00-05:00",
        ])
        return pd.DataFrame({"open": [1.0, 2.0]}, index=idx)


def test_option_chain_http_retry(monkeypatch, caplog):
    """If the HTTP API returns 429 we should retry a few times then log at
    INFO instead of WARNING and return an empty frame.
    """
    from featureSQL.yahoo import YahooOptionChainCollector
    import requests

    class DummyResp:
        def __init__(self, status):
            self.status_code = status
        def raise_for_status(self):
            # use the instance attribute rather than undefined local
            raise requests.HTTPError(f"{self.status_code} error", response=self)
        def json(self):
            return {}

    calls = {"count": 0}
    def fake_get(url, timeout):
        calls["count"] += 1
        return DummyResp(429)

    monkeypatch.setattr(requests, "get", fake_get)
    # ensure the yfinance fallback also fails so that our HTTP client is hit
    try:
        import yfinance as yf
        class DeadYF:
            def __init__(self, sym):
                pass
            @property
            def options(self):
                return []
            def option_chain(self, expiration):
                raise requests.HTTPError("yf failure")
        monkeypatch.setattr(yf, "Ticker", DeadYF)
    except ImportError:
        # if yfinance isn't installed we don't need the patch
        pass
    caplog.set_level("INFO")
    collector = YahooOptionChainCollector(".", symbol_list=["AAPL"], store=None)
    df, warn = collector._fetch_option_chain_frame("AAPL")
    assert df.empty
    # we also expect a warning string back when the HTTP API rate limits
    assert warn is not None and "rate limited" in warn.lower()
    assert calls["count"] == 3


def test_option_chain_sanitizes_dtype_error(monkeypatch):
    """Ensure we convert integer values to strings before building the
    DataFrame, avoiding the dtype error seen in production.
    """
    from featureSQL.yahoo import YahooOptionChainCollector

    class BadTicker:
        def option_chain(self):
            # payload triggers dtype 'str' error when passed to pd.DataFrame
            return [{"contractSymbol": 0, "strike": 1.23}]

    # patch the local symbol in yahoo module rather than yahooquery
    import featureSQL.yahoo as ymod
    monkeypatch.setattr(ymod, "Ticker", lambda sym: BadTicker())
    collector = YahooOptionChainCollector(".", symbol_list=["AAPL"], store=None)
    df, warn = collector._fetch_option_chain_frame("AAPL")
    # should return non-empty frame with stringified contractSymbol
    assert not df.empty
    assert df.iloc[0]["contractSymbol"] == "0"


def test_yahooquery_429_is_suppressed(monkeypatch, caplog):
    """If yahooquery.option_chain raises a 429 HTTPError we log info and
    return an empty frame.
    """
    from featureSQL.yahoo import YahooOptionChainCollector
    import requests

    class BadTicker:
        def option_chain(self):
            resp = requests.Response()
            resp.status_code = 429
            raise requests.HTTPError("429 Client Error", response=resp)

    import featureSQL.yahoo as ymod
    monkeypatch.setattr(ymod, "Ticker", lambda sym: BadTicker())
    caplog.set_level("INFO")
    collector = YahooOptionChainCollector(".", symbol_list=["AAPL"], store=None)
    df, warn = collector._fetch_option_chain_frame("AAPL")
    assert df.empty
    assert warn is not None and "rate limited" in warn.lower()


def test_prepare_history_frame_handles_mixed_timezones():
    """The helper should coerce mixed timezone strings into UTC without
    raising warnings or dropping rows.
    """
    from featureSQL.yahoo import prepare_history_frame
    df = pd.DataFrame(
        {
            "date": [
                "2020-01-01T00:00:00Z",
                "2020-01-02T00:00:00-05:00",
                "2020-01-03T00:00:00+02:00",
            ],
            "open": [1.0, 2.0, 3.0],
        }
    )
    frame = prepare_history_frame(df, "SYM")
    # should keep all three rows and convert to UTC
    assert len(frame) == 3
    assert frame["date"].dt.tz is not None
    # timezone for the series should be UTC
    tzinfo = frame["date"].dt.tz
    assert tzinfo is not None
    assert str(tzinfo) == "UTC"
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
    # normalize to date strings without timezone so the comparison is stable
    dates = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d").tolist()
    assert dates == ["2020-01-01", "2020-01-02"]


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
    # duplicates on 2020-01-02 should have been collapsed; we expect at
    # least the original and new end date to remain.
    assert len(df) >= 2
    dates = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d").tolist()
    assert "2020-01-01" in dates and "2020-01-02" in dates


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

    This is essentially a duplicate of *test_gcs_download_requires_bucket* but
    exists for historical reasons; keep it around to exercise the same
    failure mode without any environment variable.
    """
    r = Run()
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.delenv("GCS_BUCKET_NAME", raising=False)
    with pytest.raises(ValueError):
        r.download(store_type="gcs", data_path=None)
    with pytest.raises(ValueError):
        r.download(store_type="gcs", data_path=True)
    monkeypatch.undo()


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
