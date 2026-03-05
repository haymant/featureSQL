import os
import numpy as np
import pandas as pd
from pathlib import Path

from featureSQL.duck import DuckQueryService, LRUCache


def write_bin(path: Path, values: np.ndarray):
    """Helper to write a little .day.bin file with a leading zero index."""
    arr = np.hstack([[0], values]).astype("<f")
    arr.tofile(path)


def test_join_loads_both_symbols(tmp_path):
    # create fake directory structure with features subdir and calendar
    root = tmp_path / "data"
    (root / "calendars").mkdir(parents=True)
    # simple calendar with two dates
    (root / "calendars" / "day.txt").write_text("2020-01-01\n2020-01-02\n")
    for sym in ["aapl", "nvda"]:
        (root / "features" / sym).mkdir(parents=True, exist_ok=True)

    # aapl has open column, nvda has close column
    write_bin(root / "features" / "aapl" / "open.day.bin", np.array([10.0, 20.0]))
    write_bin(root / "features" / "nvda" / "close.day.bin", np.array([100.0, 200.0]))

    svc = DuckQueryService(root, cache=LRUCache(max_symbols=2))

    sql = "SELECT a.open, n.close FROM AAPL a JOIN NVDA n ON a.open = 10"
    df = svc.execute(sql)

    # expect exactly one row with the matching values
    assert not df.empty
    assert list(df.columns) == ["open", "close"]
    assert df.iloc[0]["open"] == 10.0
    assert df.iloc[0]["close"] == 100.0


def test_cache_eviction(tmp_path):
    root = tmp_path / "data2"
    # create features directories
    for sym in ["foo", "bar", "baz"]:
        d = root / "features" / sym
        d.mkdir(parents=True)
        write_bin(d / "x.day.bin", np.array([1.0, 2.0]))

    cache = LRUCache(max_symbols=2)
    svc = DuckQueryService(root, cache=cache)

    # access foo and bar; both should be cached (keys are stored lowercase)
    svc.execute("SELECT x FROM FOO")
    svc.execute("SELECT x FROM BAR")
    assert set(cache._cache.keys()) == {"foo", "bar"}

    # now access baz, foo should be evicted (LRU)
    svc.execute("SELECT x FROM BAZ")
    assert set(cache._cache.keys()) == {"bar", "baz"}
