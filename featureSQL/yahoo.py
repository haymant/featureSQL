"""Yahoo-specific helpers and collectors.

This module contains everything related to interacting with Yahoo data: calendar
retrieval, symbol lists, and the collector/normalizer classes used by the CLI.
"""

from __future__ import annotations

from datetime import datetime, timezone
import time
import os
from pathlib import Path
from typing import Iterable, List, Union
import abc

import pandas as pd
import requests
from loguru import logger
from yahooquery import Ticker

from .utils import deco_retry
from .storage import get_storage, FileSystemStore
from contextlib import contextmanager


@contextmanager

def force_utc_datetimeindex():
    """Temporarily patch ``pandas.DatetimeIndex.__new__`` to handle mixed
    timezone inputs.

    Attempting to build a ``DatetimeIndex`` from a sequence containing both
    UTC and non-UTC timestamps normally raises a ``ValueError``.  This
    context manager intercepts that error and retries the construction with
    the data coerced to UTC, allowing callers (like our downloader) to
    proceed without crashing.  Since we patch the ``__new__`` method in-place
    the fix applies regardless of how the class was imported by external
    libraries.
    """
    import pandas as _pd
    orig_new = _pd.DatetimeIndex.__new__

    def new(cls, *args, **kwargs):
        try:
            return orig_new(cls, *args, **kwargs)
        except ValueError as e:
            if "Mixed timezones detected" in str(e):
                # coerce the data (first positional arg or 'data' kwarg)
                if args:
                    data, *rest = args
                else:
                    data = kwargs.pop("data", kwargs.pop("values", None))
                    rest = []
                try:
                    conv = _pd.to_datetime(data, utc=True)
                    return orig_new(cls, conv, *rest, **kwargs)
                except Exception:
                    # fallthrough to re-raise original error
                    pass
            raise

    _pd.DatetimeIndex.__new__ = new
    try:
        yield
    finally:
        _pd.DatetimeIndex.__new__ = orig_new


# symbol retrieval helpers follow the same approach as the original
# the akshare fallback; we simply copy the earlier functions verbatim.
_US_SYMBOLS: List[str] = None
_HS_SYMBOLS: List[str] = None
_IN_SYMBOLS: List[str] = None
_BR_SYMBOLS: List[str] = None

MINIMUM_SYMBOLS_NUM = 3900

DEFAULT_YAHOO_SYMBOLS = {
    "equity": ["AAPL", "MSFT", "NVDA"],
    "fx": ["EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X"],
    "ir": ["^IRX", "^FVX", "^TNX", "^TYX"],
    "vol": ["^VIX", "^VXN", "^RVX"],
    "correlation": ["SPY:QQQ", "TLT:IEF"],
    "option": ["AAPL", "MSFT", "SPY"],
}


# ----------------------- utility helpers -----------------------

def get_default_symbols(asset_type: str) -> List[str]:
    asset_key = str(asset_type or "equity").strip().lower()
    if asset_key not in DEFAULT_YAHOO_SYMBOLS:
        raise ValueError(f"asset_type not supported: {asset_type}")
    return list(DEFAULT_YAHOO_SYMBOLS[asset_key])


def normalize_yahoo_symbol(symbol: str) -> str:
    return (
        str(symbol)
        .replace("^", "_")
        .replace("=", "_")
        .replace("/", "_")
        .replace(":", "__")
        .upper()
    )


def prepare_history_frame(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    frame = df.copy()
    if "date" not in frame.columns:
        frame = frame.reset_index()
    if "date" not in frame.columns:
        return pd.DataFrame()
    if "symbol" not in frame.columns:
        frame["symbol"] = symbol
    # ensure all timestamps are converted to UTC to avoid mixed-
    # timezone warnings that occur when Yahoo returns inconsistent tz info.
    # The DuckDB views created later expect naive/UTC dates.
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce", utc=True)
    frame = frame[frame["date"].notna()].copy()
    if frame.empty:
        return frame
    return frame.sort_values("date")


def parse_correlation_pair(symbol: str) -> tuple[str, str]:
    text = str(symbol).strip()
    for delimiter in (":", "|"):
        if delimiter in text:
            left, right = text.split(delimiter, 1)
            return left.strip().upper(), right.strip().upper()
    raise ValueError(
        f"invalid correlation pair '{symbol}', expected LEFT:RIGHT"
    )

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


def get_us_stock_symbols(reload: bool = False, data_path: Union[str, Path] = None, store=None) -> List[str]:
    """Return a list of US equity tickers.

    The result is cached in ``us_symbols_cache.pkl`` alongside this script so
    that repeated invocations don’t hammer the NASDAQ/NYSE servers.  Supply
    ``reload=True`` to re‑fetch and overwrite the cache.
    """
    global _US_SYMBOLS
    from .storage import get_storage

    # memory cache short‑circuit
    if _US_SYMBOLS is not None and not reload:
        return _US_SYMBOLS

    if store is None:
        store = get_storage("fs")

    # build cache path using data_path if supplied
    if data_path is None:
        cache_dir = store.joinpath(str(Path(__file__).parent), "source", "instruments")
    else:
        cache_dir = store.joinpath(str(data_path), "source", "instruments")
        
    store.mkdir(cache_dir, parents=True, exist_ok=True)
    cache_file = store.joinpath(cache_dir, "us_symbols.txt")

    # disk cache (plain text)
    if not reload and store.exists(cache_file):
        try:
            _US_SYMBOLS = [line.strip() for line in store.read_text(cache_file).splitlines() if line.strip()]
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
        store.write_text(cache_file, "\n".join(_US_SYMBOLS))
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
        store=None
    ):
        from .storage import get_storage
        self.store = store if store else get_storage("fs")
        self.source_dir = str(source_dir)
        if isinstance(store, type(get_storage("fs"))):
            self.source_dir = str(Path(source_dir).expanduser())
        self.store.mkdir(self.source_dir, parents=True, exist_ok=True)
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
        mode: str = "history",
    ) -> list[str]:
        mode_key = str(mode or "history").strip().lower()
        if mode_key not in {"history", "spot", "both"}:
            raise ValueError(f"unsupported mode: {mode}")
        warnings: list[str] = []
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
                return warnings
        else:
            instruments = self.get_instrument_list()

        for symbol in instruments:
            fname = self.normalize_symbol(symbol)
            path = self.store.joinpath(self.source_dir, f"{fname}.csv")

            existing_df = None
            # attempt to read existing CSV regardless of what exists() reports;
            # a false negative should not cause data loss.  Missing-file errors
            # are caught and ignored.
            try:
                if isinstance(self.store, FileSystemStore):
                    if os.path.exists(path):
                        existing_df = pd.read_csv(path)
                else:
                    # read_bytes will raise if blob does not exist
                    import io as _io
                    existing_df = pd.read_csv(_io.BytesIO(self.store.read_bytes(path)))
            except Exception:
                # most likely the file wasn't present; ignore this quietly
                existing_df = None

            # drop any leftover index column from previous runs
            if existing_df is not None and "index" in existing_df.columns:
                existing_df = existing_df.drop(columns=["index"])
            # if the existing CSV mysteriously has no date column we consider it
            # corrupt and overwrite it with fresh data.  This replicates the
            # behaviour the user expects when rerunning downloads repeatedly.
            if existing_df is not None and "date" not in existing_df.columns:
                msg = f"existing CSV for {symbol} missing date column, will be replaced"
                logger.warning(msg)
                warnings.append(msg)
                existing_df = None
            # convert existing dates to datetime to avoid type mismatches when
            # concatting with new data (strings vs Timestamp cause duplicates
            # to slip through drop_duplicates).
            if existing_df is not None and not existing_df.empty:
                try:
                    # convert existing dates to UTC as well; this call previously
                    # emitted "Mixed timezones detected" warnings when the CSV
                    # contained zulu and offset-aware strings.  forcing utc=True
                    # normalizes everything and avoids data loss.
                    existing_df["date"] = pd.to_datetime(existing_df["date"], errors="coerce", utc=True)
                except Exception:
                    pass
                # if CSV lacks symbol column (e.g. old runs), inject one for
                # de-duplication; every file is per-symbol so this is safe.
                if "symbol" not in existing_df.columns:
                    existing_df["symbol"] = symbol

            # use yahooquery to fetch data for the requested range
            try:
                t = Ticker(symbol)
                # protect against mixed-timezone errors inside yahooquery by
                # forcing any DatetimeIndex constructions to use utc=True.
                with force_utc_datetimeindex():
                    raw = t.history(start=start, end=end, interval="1d")
                new_df = prepare_history_frame(raw, symbol)
                if mode_key == "spot":
                    new_df = new_df.tail(1)
            except Exception as e:  # includes network timeouts, yahooquery errors
                logger.warning(f"failed to fetch data for {symbol}: {e}, skipping")
                time.sleep(delay)
                continue

            if new_df.empty:
                # nothing to write; leave existing file intact
                time.sleep(delay)
                continue

            if "date" not in new_df.columns:
                msg = f"ticker {symbol} history returned no date column; skipping"
                logger.warning(msg)
                warnings.append(msg)
                time.sleep(delay)
                continue
            # if we have an existing dataframe, merge and dedupe
            if existing_df is not None and not existing_df.empty:
                try:
                    # ensure the incoming data has a symbol column as well;
                    # new_df reset_index() above should already have one from
                    # yahooquery, but normalise just in case.
                    if "symbol" not in new_df.columns:
                        new_df["symbol"] = symbol

                    combined = pd.concat([existing_df, new_df], ignore_index=True)
                    # dedupe on symbol+date when possible, otherwise fall back to
                    # date-only (single-symbol file)
                    subset = ["date"]
                    if "symbol" in combined.columns:
                        subset = ["symbol", "date"]
                    combined.drop_duplicates(subset=subset, inplace=True)
                    df_to_write = combined
                except Exception as e:
                    logger.warning(f"failed to merge CSVs for {symbol}: {e}")
                    df_to_write = new_df
            else:
                df_to_write = new_df

            # drop any spontaneous index column before persisting
            if "index" in df_to_write.columns:
                df_to_write = df_to_write.drop(columns=["index"])

            # normalize date column to plain strings (YYYY-MM-DD); this is
            # applied on every write, whether it's a fresh file or a merge.
            if "date" in df_to_write.columns:
                try:
                    df_to_write["date"] = pd.to_datetime(
                        df_to_write["date"], errors="coerce", utc=True
                    ).dt.strftime("%Y-%m-%d")
                except Exception:
                    pass

            try:
                # `FileSystemStore` is imported at module level; no need to import here
                if isinstance(self.store, FileSystemStore):
                    df_to_write.to_csv(path, index=False)
                else:
                    import io as _io
                    csv_buffer = _io.StringIO()
                    df_to_write.to_csv(csv_buffer, index=False)
                    try:
                        self.store.write_text(path, csv_buffer.getvalue())
                    except Exception as e:
                        logger.warning(f"download_data write_text exception for {symbol} path={path}: {e}")
                        raise
            except Exception as e:
                msg = f"failed to write data for {symbol} ({path}): {e}, skipping"
                logger.warning(msg)
                warnings.append(msg)
                # do not re-raise; move on to next symbol
            time.sleep(delay)
        # completed downloading every symbol; return any accumulated warnings
        return warnings


class YahooCollectorUS(BaseCollector):
    def get_instrument_list(self):
        return get_us_stock_symbols()

    def normalize_symbol(self, symbol):
        return normalize_yahoo_symbol(symbol)


class YahooCollectorGeneric(BaseCollector):
    asset_type = "equity"

    def get_instrument_list(self):
        return get_default_symbols(self.asset_type)

    def normalize_symbol(self, symbol):
        return normalize_yahoo_symbol(symbol)


class YahooCollectorFX(YahooCollectorGeneric):
    asset_type = "fx"


class YahooCollectorIR(YahooCollectorGeneric):
    asset_type = "ir"


class YahooCollectorVol(YahooCollectorGeneric):
    asset_type = "vol"


class YahooCorrelationCollector(BaseCollector):
    def get_instrument_list(self):
        return get_default_symbols("correlation")

    def normalize_symbol(self, symbol):
        left, right = parse_correlation_pair(symbol)
        return normalize_yahoo_symbol(f"{left}:{right}")

    def _fetch_history(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        ticker = Ticker(symbol)
        frame = prepare_history_frame(
            ticker.history(start=start, end=end, interval="1d"),
            symbol,
        )
        if "close" not in frame.columns:
            raise ValueError(f"history for {symbol} does not include close")
        return frame[["date", "close"]].rename(columns={"close": symbol})

    def download_data(
        self,
        start: str,
        end: str,
        delay: float = 0.5,
        mode: str = "history",
        window: int = 20,
    ) -> list[str]:
        mode_key = str(mode or "history").strip().lower()
        if mode_key not in {"history", "spot", "both"}:
            raise ValueError(f"unsupported mode: {mode}")
        instruments = list(self.symbol_list) if self.symbol_list is not None else self.get_instrument_list()
        warnings: list[str] = []
        for pair in instruments:
            try:
                left, right = parse_correlation_pair(pair)
                left_df = self._fetch_history(left, start, end)
                right_df = self._fetch_history(right, start, end)
                merged = left_df.merge(right_df, on="date", how="inner").sort_values("date")
                if merged.empty:
                    time.sleep(delay)
                    continue
                merged["left_return"] = merged[left].pct_change()
                merged["right_return"] = merged[right].pct_change()
                merged["correlation"] = merged["left_return"].rolling(window).corr(merged["right_return"])
                merged["pair"] = f"{left}:{right}"
                merged["window"] = window
                if mode_key == "spot":
                    merged = merged.tail(1)
                # normalize date column for CSV output as well
                if "date" in merged.columns:
                    try:
                        merged["date"] = pd.to_datetime(
                            merged["date"], errors="coerce", utc=True
                        ).dt.strftime("%Y-%m-%d")
                    except Exception:
                        pass
                path = self.store.joinpath(self.source_dir, f"{self.normalize_symbol(pair)}.csv")
                merged.to_csv(path, index=False) if isinstance(self.store, FileSystemStore) else self.store.write_text(path, merged.to_csv(index=False))
            except Exception as e:
                logger.warning(f"failed to compute correlation for {pair}: {e}, skipping")
                warnings.append(str(e))
            time.sleep(delay)
        return warnings


class YahooOptionChainCollector(BaseCollector):
    def get_instrument_list(self):
        return get_default_symbols("option")

    def normalize_symbol(self, symbol):
        return normalize_yahoo_symbol(symbol)

    def _sanitize_chain(self, chain: Any) -> Any:
        """Recursively convert integer values to strings in the provided object.

        Yahooquery sometimes returns mixed types that pandas interprets as
        ``dtype='str'`` expecting strings only; an integer in the payload
        raises the ``Invalid value '0' for dtype 'str'`` error we observed.
        This helper walks lists/dicts and stringifies ints so that
        ``pd.DataFrame`` happily constructs.
        """
        if isinstance(chain, dict):
            return {k: self._sanitize_chain(v) for k, v in chain.items()}
        if isinstance(chain, list):
            return [self._sanitize_chain(v) for v in chain]
        if isinstance(chain, int) and not isinstance(chain, bool):
            return str(chain)
        return chain

    def _fetch_option_chain_frame(self, symbol: str) -> tuple[pd.DataFrame, str | None]:
        warning: str | None = None
        try:
            chain = Ticker(symbol).option_chain()
            if isinstance(chain, pd.DataFrame):
                df = chain.copy()
            else:
                # always sanitize the raw payload so integers become strings;
                # this avoids subtle dtype errors and matches what the tests
                # expect even when pandas happily coerces an integer.
                chain = self._sanitize_chain(chain)
                try:
                    df = pd.DataFrame(chain)
                except Exception as inner_exc:
                    # attempt to sanitize and retry on dtype errors just in case
                    if "dtype 'str'" in str(inner_exc):
                        chain = self._sanitize_chain(chain)
                        df = pd.DataFrame(chain)
                    else:
                        raise
            if not df.empty:
                return df, None
        except requests.HTTPError as exc:
            # if yahooquery itself hit a 429, treat as empty and warn
            if exc.response is not None and getattr(exc.response, "status_code", None) == 429:
                warning = f"yahooquery rate limited for {symbol}, skipping"
                logger.info(warning)
                return pd.DataFrame(), warning
            logger.debug(f"yahooquery option_chain failed for {symbol}: {exc}; falling back to HTTP API")
        except Exception as exc:
            logger.debug(f"yahooquery option_chain failed for {symbol}: {exc}; falling back to HTTP API")

        try:
            import yfinance as yf

            ticker = yf.Ticker(symbol)
            expirations = list(ticker.options or [])
            if expirations:
                expiration = expirations[0]
                chain = ticker.option_chain(expiration)
                frames = []
                if getattr(chain, "calls", None) is not None and not chain.calls.empty:
                    calls = chain.calls.copy()
                    calls["optionType"] = "call"
                    calls["expiration"] = expiration
                    frames.append(calls)
                if getattr(chain, "puts", None) is not None and not chain.puts.empty:
                    puts = chain.puts.copy()
                    puts["optionType"] = "put"
                    puts["expiration"] = expiration
                    frames.append(puts)
                if frames:
                    df = pd.concat(frames, ignore_index=True)
                    price = None
                    try:
                        price = ticker.fast_info.get("lastPrice")
                    except Exception:
                        price = None
                    if price is not None:
                        df["regularMarketPrice"] = price
                    return df, None
        except Exception as exc:
            logger.debug(f"yfinance option_chain failed for {symbol}: {exc}; falling back to HTTP API")

        # finally fall back to the HTTP API with simple retry logic on 429
        url = f"https://query1.finance.yahoo.com/v7/finance/options/{symbol}"
        for attempt in range(3):
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                break
            except requests.HTTPError as exc:
                if response.status_code == 429:
                    # rate limit; retry unless this is the last attempt
                    if attempt < 2:
                        time.sleep(1 + attempt)  # simple backoff
                        continue
                    warning = f"option chain API rate limited for {symbol}, skipping"
                    logger.info(warning)
                    return pd.DataFrame(), warning
                # any other HTTP error should propagate
                raise
        else:
            # should never get here, but be safe
            warning = f"option chain API rate limited for {symbol}, skipping"
            logger.info(warning)
            return pd.DataFrame(), warning

        payload = response.json().get("optionChain", {}).get("result", [])
        if not payload:
            return pd.DataFrame(), None
        result = payload[0]
        quote = result.get("quote", {})
        options = result.get("options", [{}])[0]
        frames = []
        for option_type, field in (("call", "calls"), ("put", "puts")):
            rows = options.get(field, [])
            if not rows:
                continue
            frame = pd.DataFrame(rows)
            frame["optionType"] = option_type
            frames.append(frame)
        if not frames:
            return pd.DataFrame(), None
        df = pd.concat(frames, ignore_index=True)
        if "expiration" in df.columns:
            df["expiration"] = pd.to_datetime(df["expiration"], unit="s", errors="coerce")
        if "expirationDate" in df.columns and "expiration" not in df.columns:
            df["expiration"] = pd.to_datetime(df["expirationDate"], unit="s", errors="coerce")
        if quote.get("regularMarketPrice") is not None:
            df["regularMarketPrice"] = quote.get("regularMarketPrice")
        return df

    def download_data(
        self,
        start: str = None,
        end: str = None,
        delay: float = 0.5,
        mode: str = "history",
    ) -> list[str]:
        """Download option chains and return warnings encountered.

        A warning is generated when a symbol is skipped (rate limit, HTTP
        error, empty frame, write failure, etc.).  The caller (e.g. the CLI
        or duck-server) can show these messages to the user.
        """
        instruments = list(self.symbol_list) if self.symbol_list is not None else self.get_instrument_list()
        snapshot_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        warnings: list[str] = []
        for symbol in instruments:
            path = self.store.joinpath(self.source_dir, f"{self.normalize_symbol(symbol)}.csv")
            try:
                df, warn = self._fetch_option_chain_frame(symbol)
                if warn:
                    warnings.append(warn)
                if df.empty:
                    time.sleep(delay)
                    continue
                if "expiration" not in df.columns and "expirationDate" in df.columns:
                    df["expiration"] = df["expirationDate"]
                df["symbol"] = symbol
                df["snapshot_at"] = snapshot_at
                if self.store.exists(path):
                    existing = pd.read_csv(path) if isinstance(self.store, FileSystemStore) else pd.read_csv(__import__("io").BytesIO(self.store.read_bytes(path)))
                    df = pd.concat([existing, df], ignore_index=True)
                    key_cols = [col for col in ["symbol", "snapshot_at", "contractSymbol"] if col in df.columns]
                    if key_cols:
                        df.drop_duplicates(subset=key_cols, inplace=True)
                if isinstance(self.store, FileSystemStore):
                    df.to_csv(path, index=False)
                else:
                    self.store.write_text(path, df.to_csv(index=False))
            except Exception as e:
                msg = f"option chain download error for {symbol}: {e}, skipping"
                logger.debug(msg)
                warnings.append(msg)
            time.sleep(delay)
        return warnings


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
