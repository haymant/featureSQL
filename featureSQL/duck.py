"""Simple SQL service using DuckDB with lazy-loaded symbol data."""

from __future__ import annotations

from collections import OrderedDict
from pathlib import Path
import re
from typing import Callable, Dict, Optional

import duckdb
import numpy as np
import pandas as pd


class LRUCache:
    """Very lightweight LRU cache keyed by symbol name.

    The cache keeps a mapping from symbol->(df, memory) and evicts the least
    recently used entries whenever a configured threshold is exceeded.  Two
    limits are supported:

    * ``max_symbols``: maximum number of distinct symbols to retain
    * ``max_memory``: approximate total bytes of DataFrame memory to keep
    """

    def __init__(
        self,
        max_symbols: Optional[int] = None,
        max_memory: Optional[int] = None,
    ):
        self.max_symbols = max_symbols
        self.max_memory = max_memory
        self._cache: "OrderedDict[str, Dict[str, object]]" = OrderedDict()
        self._total_memory = 0

    def get(self, key: str, loader: Callable[[str], pd.DataFrame]) -> pd.DataFrame:
        # on hit, bump to end and return
        if key in self._cache:
            self._cache.move_to_end(key)
            return self._cache[key]["df"]

        # load new data
        df = loader(key)
        mem = df.memory_usage(deep=True).sum()
        self._cache[key] = {"df": df, "mem": mem}
        self._total_memory += mem
        self._evict_if_needed()
        return df

    def _evict_if_needed(self) -> None:
        # remove oldest entries until under both thresholds
        while self._cache and (
            (self.max_symbols and len(self._cache) > self.max_symbols)
            or (self.max_memory and self._total_memory > self.max_memory)
        ):
            oldest_key, oldest_val = self._cache.popitem(last=False)
            self._total_memory -= oldest_val["mem"]


class DuckQueryService:
    """SQL service using :mod:`duckdb` over symbol bin files.

    Each symbol corresponds to a subdirectory under ``root``; that directory
    contains one ``<field>.day.bin`` file per numeric column.  When a query
    references a symbol we lazily read its bins into a DataFrame and register
    it with DuckDB.  The cache above keeps only a limited number of symbols
    in memory.
    """

    # match `FROM foo` or `JOIN foo` so that symbols used in joins are
    # loaded as well.  we deliberately avoid more complex SQL parsing;
    # a simple regex is sufficient for the limited syntax we expect.
    SYMBOL_RE = re.compile(r"\b(?:from|join)\s+([A-Za-z0-9_]+)\b", re.IGNORECASE)

    def __init__(self, root: str, cache: Optional[LRUCache] = None, store=None):
        from .storage import get_storage
        self.store = store if store else get_storage("fs")
        self.root = str(root)
        self.cache = cache or LRUCache()
        self._conn = duckdb.connect()
        # try to read calendar file (optional)
        cal_path = self.store.joinpath(self.root, "calendars", "day.txt")
        self._calendar: Optional[list] = None
        if self.store.exists(cal_path):
            self._calendar = [line.strip() for line in self.store.read_text(cal_path).splitlines() if line.strip()]

    def _load_symbol_df(self, symbol: str) -> pd.DataFrame:
        """Read all bin files for ``symbol`` and return a DataFrame."""
        symbol_dir = self.store.joinpath(self.root, "features", symbol.lower())
        if not self.store.exists(symbol_dir) and getattr(self.store, 'store_type', 'fs') == 'fs':
            # Note: For GCS, exists on a directory might be false if no object exactly matches the dir name
            # So we just proceed to glob
            pass

        # normalize for glob: remove any leading slash which confuses
        # object-storage backends.  file system paths should be left alone.
        if isinstance(self.store, __import__('featureSQL.storage', fromlist=['FileSystemStore']).FileSystemStore):
            glob_path = symbol_dir
        else:
            glob_path = symbol_dir.lstrip("/") if isinstance(symbol_dir, str) else symbol_dir

        cols: Dict[str, np.ndarray] = {}
        for binfile in self.store.glob(glob_path, "*.day.bin"):
            field = str(binfile).split("/")[-1].replace(".day.bin", "")
            
            if isinstance(self.store, __import__('featureSQL.storage', fromlist=['FileSystemStore']).FileSystemStore):
                arr = np.fromfile(binfile, dtype="<f")
            else:
                arr = np.frombuffer(self.store.read_bytes(binfile), dtype="<f")
                
            if arr.size == 0:
                continue
            # first element is date index; convert to calendar date if available
            data = arr[1:]
            if self._calendar is not None:
                # produce a date column if not already
                cols.setdefault("date", pd.Series([self._calendar[int(arr[0]) + i] for i in range(len(data))]))
            cols[field] = data
            
        if not cols:
            raise FileNotFoundError(f"symbol directory/files not found or empty: {symbol_dir}")
            
        return pd.DataFrame(cols)

    def _ensure_symbols(self, sql: str) -> None:
        symbols = set(self.SYMBOL_RE.findall(sql))
        for sym in symbols:
            key = sym.lower()
            # load using lowercase key to keep cache consistent
            df = self.cache.get(key, self._load_symbol_df)
            # register table name using original capitalization from query
            self._conn.register(sym, df)

    def execute(self, sql: str) -> pd.DataFrame:
        """Run ``sql`` after loading any referenced symbols.

        The returned DataFrame comes from DuckDB's result set.
        """
        self._ensure_symbols(sql)
        return self._conn.execute(sql).df()
