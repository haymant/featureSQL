"""Yahoo-specific helpers and collectors.

This module contains everything related to interacting with Yahoo data: calendar
retrieval, symbol lists, and the collector/normalizer classes used by the CLI.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Iterable, List, Union
import abc

import pandas as pd
import requests
from loguru import logger
from yahooquery import Ticker

from .utils import deco_retry


# symbol retrieval helpers follow the same approach as the original
# the akshare fallback; we simply copy the earlier functions verbatim.
_US_SYMBOLS: List[str] = None
_HS_SYMBOLS: List[str] = None
_IN_SYMBOLS: List[str] = None
_BR_SYMBOLS: List[str] = None

MINIMUM_SYMBOLS_NUM = 3900


# ----------------------- utility helpers -----------------------

def get_calendar_list(bench_code: str = "US_ALL") -> List[pd.Timestamp]:
    """Retrieve a trading calendar for the given benchmark.

    This is a minimal re‑implementation of a trading calendar helper.  Only a
    handful of bench codes are supported; the function uses yahooquery
    (which itself calls the public Yahoo API) when the code starts with
    ``US_``/``IN_``/``BR_``.  For Chinese indexes it falls back to
    a hard‑coded start/end range or the Sina API.
    """
    if bench_code.startswith("US_") or bench_code.startswith("IN_") or bench_code.startswith(
        "BR_"
    ):
        # use yahooquery history; the `ticker` field is the bench symbol
        t = Ticker(bench_code.replace("_", ""))
        df = t.history(interval="1d", period="max")
        # index is a MultiIndex (symbol,date)
        dates = df.index.get_level_values("date").unique()
        return sorted(pd.to_datetime(dates))
    elif bench_code.upper() == "ALL":
        # fallback: simple calendar from 2000 to today
        start = pd.Timestamp("2000-01-01")
        end = pd.Timestamp.today().normalize()
        return pd.date_range(start, end, freq="B").tolist()
    else:
        # other Chinese benchmarks; try the EastMoney HTTP API
        url = (
            "http://push2his.eastmoney.com/api/qt/stock/kline/get?secid=1.000300&" \
            "fields1=f1,f2,f3,f4,f5&fields2=f51,f52,f53,f54,f55,f56,f57,f58&" \
            "klt=101&fqt=0&beg=19900101&end=20991231"
        )
        data = requests.get(url, timeout=None).json()["data"]["klines"]
        return sorted(map(lambda x: pd.Timestamp(x.split(",")[0]), data))


@deco_retry()
def _get_nasdaq():
    res = []
    for name in ["otherlisted", "nasdaqtraded"]:
        url = f"ftp://ftp.nasdaqtrader.com/SymbolDirectory/{name}.txt"
        df = pd.read_csv(url, sep="|")
        df = df.rename(columns={"ACT Symbol": "Symbol"})
        syms = df["Symbol"].dropna()
        syms = syms.str.replace("$", "-P", regex=False)
        syms = syms.str.replace(".W", "-WT", regex=False)
        syms = syms.str.replace(".U", "-UN", regex=False)
        syms = syms.str.replace(".R", "-RI", regex=False)
        syms = syms.str.replace(".", "-", regex=False)
        res += syms.unique().tolist()
    return res


@deco_retry()
def _get_nyse():
    url = "https://www.nyse.com/api/quotes/filter"
    params = {
        "instrumentType": "EQUITY",
        "pageNumber": 1,
        "sortColumn": "NORMALIZED_TICKER",
        "sortOrder": "ASC",
        "maxResultsPerPage": 10000,
        "filterToken": "",
    }
    resp = requests.post(url, json=params, timeout=None)
    resp.raise_for_status()
    try:
        return [_v["symbolTicker"].replace("-", "-P") for _v in resp.json()]
    except Exception:
        return []


def get_us_stock_symbols(reload: bool = False, data_path: Union[str, Path] = None) -> List[str]:
    """Return a list of US equity tickers.

    The result is cached in ``us_symbols_cache.pkl`` alongside this script so
    that repeated invocations don’t hammer the NASDAQ/NYSE servers.  Supply
    ``reload=True`` to re‑fetch and overwrite the cache.
    """
    global _US_SYMBOLS

    # memory cache short‑circuit
    if _US_SYMBOLS is not None and not reload:
        return _US_SYMBOLS

    # build cache path using data_path if supplied
    if data_path is None:
        cache_dir = Path(__file__).parent
    else:
        cache_dir = Path(data_path).expanduser()
        cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir.joinpath("source/instruments/us_symbols.txt")

    # disk cache (plain text)
    if not reload and cache_file.exists():
        try:
            with cache_file.open() as fp:
                _US_SYMBOLS = [line.strip() for line in fp if line.strip()]
            return _US_SYMBOLS
        except Exception:
            logger.warning("failed to load symbol cache, refreshing")

    # always fetch when cache empty or reloading requested
    all_syms = _get_nasdaq() + _get_nyse()
    # canonical formatting
    def fmt(s):
        s = s.replace(".", "-")
        s = s.strip("$*")
        return s
    _US_SYMBOLS = sorted(set(map(fmt, filter(lambda x: len(x) < 8 and not x.endswith("WS"), all_syms))))
    # write cache file (plain text)
    try:
        with cache_file.open("w") as fp:
            fp.write("\n".join(_US_SYMBOLS))
    except Exception:
        logger.warning("failed to write us symbol cache")
    return _US_SYMBOLS


def get_hs_stock_symbols(reload: bool = False, data_path: Union[str, Path] = None) -> List[str]:
    """Get HS tickers with optional caching.

    See ``get_us_stock_symbols`` for cache semantics.
    """
    global _HS_SYMBOLS

    # short‑circuit if loaded
    if _HS_SYMBOLS is not None and not reload:
        return _HS_SYMBOLS

    # choose cache directory
    if data_path is None:
        cache_dir = Path(__file__).parent
    else:
        cache_dir = Path(data_path).expanduser()
        cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir.joinpath(_HS_CACHE_NAME)

    # load from disk cache
    if not reload and cache_file.exists():
        try:
            with cache_file.open() as fp:
                _HS_SYMBOLS = [line.strip() for line in fp if line.strip()]
            return _HS_SYMBOLS
        except Exception:
            logger.warning("failed to load HS symbol cache, refreshing")

    @deco_retry()
    def _fetch():
        base = "http://99.push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": 1,
            "pz": 100,
            "po": 1,
            "np": 1,
            "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048",
            "fields": "f12",
        }
        syms = []
        while True:
            resp = requests.get(base, params=params, timeout=None)
            resp.raise_for_status()
            data = resp.json().get("data", {}).get("diff", [])
            if not data:
                break
            page_syms = [_v["f12"] for _v in data]
            syms.extend(page_syms)
            params["pn"] += 1
            time.sleep(0.5)
        if len(syms) < MINIMUM_SYMBOLS_NUM:
            raise ValueError("incomplete hs list")
        syms = [s + ".ss" if s.startswith("6") else s + ".sz" for s in syms]
        return syms

    # always fetch when cache missing or reload requested
    _HS_SYMBOLS = sorted(set(_fetch()))
    try:
        with cache_file.open("w") as fp:
            fp.write("\n".join(_HS_SYMBOLS))
    except Exception:
        logger.warning("failed to write HS symbol cache")
    return _HS_SYMBOLS


# ------------------- collector / normalizer classes -------------------

class BaseCollector(abc.ABC):
    def __init__(
        self,
        source_dir: Union[str, Path],
        symbol_list: Iterable[str] = None,
    ):
        self.source_dir = Path(source_dir).expanduser()
        self.source_dir.mkdir(parents=True, exist_ok=True)
        # preserve an *explicit* empty list/set rather than normalising it to
        # ``None``.  the distinction matters because ``None`` means “no
        # filtering” and an empty iterable should mean “filter to nothing”.
        if symbol_list is None:
            self.symbol_list = None
        else:
            self.symbol_list = set(symbol_list)

    @abc.abstractmethod
    def get_instrument_list(self) -> List[str]:
        raise NotImplementedError

    @abc.abstractmethod
    def normalize_symbol(self, symbol: str) -> str:
        raise NotImplementedError

    def download_data(
        self,
        start: str,
        end: str,
        delay: float = 0.5,
    ):
        # build instrument list respecting any explicit symbol_list
        if self.symbol_list is not None:
            # user provided an explicit set; if it's empty we treat it as nothing to
            # download (avoid falling back on the full universe which was the
            # original bug).  This also sidesteps any normalization/fuzzy filtering
            # problems by simply iterating over the requested symbols directly.
            if isinstance(self.symbol_list, (list, tuple, set)):
                instruments = list(self.symbol_list)
            else:
                instruments = str(self.symbol_list).split(",")
            instruments = [s.strip() for s in instruments if s and s.strip()]
            if not instruments:
                logger.info("symbol_list provided is empty; nothing to download")
                return
        else:
            instruments = self.get_instrument_list()

        for symbol in instruments:
            fname = self.normalize_symbol(symbol)
            path = self.source_dir.joinpath(f"{fname}.csv")
            # skip existing file
            if path.exists():
                continue
            # use yahooquery to fetch data
            t = Ticker(symbol)
            df = t.history(start=start, end=end, interval="1d")
            if not df.empty:
                df.reset_index(inplace=True)
                df.to_csv(path, index=False)
            time.sleep(delay)


class YahooCollectorUS(BaseCollector):
    def get_instrument_list(self):
        return get_us_stock_symbols()

    def normalize_symbol(self, symbol):
        return symbol.replace("^", "_").upper()


class YahooNormalize:
    COLUMNS = ["open", "close", "high", "low", "volume"]

    @staticmethod
    def normalize_yahoo(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
        df["change"] = df["close"].pct_change()
        return df
