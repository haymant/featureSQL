import pytest
from pathlib import Path
import pandas as pd
from yahooquery import Ticker

from featureSQL.cli import Run
from featureSQL.dump_bin import DumpDataAll, DumpDataUpdate


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
        out_format="bin",
    )

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
    )

    mtime2 = bin_path.stat().st_mtime
    assert mtime2 == mtime1  # file untouched
