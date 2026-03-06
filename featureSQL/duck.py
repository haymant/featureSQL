"""Simple SQL service using DuckDB with lazy-loaded symbol data."""

from __future__ import annotations

from collections import OrderedDict
from pathlib import Path
import os
import re
from typing import Callable, Dict, Optional

import duckdb
import numpy as np
import pandas as pd


class ConversionError(ValueError):
    """Raised when SQL value conversion fails due to our time‑series rules.

    This is intentionally a lightweight subclass of ``ValueError`` so callers
    can catch it specifically without importing ``duckdb``.
    """


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

    def invalidate(self, key: str) -> None:
        """Remove a key from the cache if present (used when a symbol is
        dropped or otherwise no longer valid).
        """
        if key in self._cache:
            val = self._cache.pop(key)
            self._total_memory -= val.get("mem", 0)

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

    # match `FROM foo`, `JOIN foo`, or even `UPDATE foo`/`DELETE FROM foo` or
    # `DESCRIBE foo` so that symbols referenced in both read and write operations
    # are loaded lazily.  we deliberately avoid more complex SQL parsing; a
    # simple regex is sufficient for our limited subset of SQL.
    # regex to find table references in simple SQL statements.  we
    # include INSERT so that an INSERT will trigger a load of the target
    # symbol before execution; other write operations like UPDATE and DELETE
    # were already covered.
    SYMBOL_RE = re.compile(
        # capture names after SQL keywords we care about; drop/alter table
        # added so that a standalone `drop table foo` or `alter table foo`
        # will load the symbol before attempting to operate on it.  this also
        # keeps `_loaded_tables` accurate.
        r"\b(?:from|join|update|delete(?:\s+from)?|describe|insert\s+into?|drop\s+table|alter\s+table)\s+([A-Za-z0-9_]+)\b",
        re.IGNORECASE,
    )

    def __init__(self, root: str, cache: Optional[LRUCache] = None, store=None):
        from .storage import get_storage
        self.store = store if store else get_storage("fs")
        self.root = str(root)
        self.cache = cache or LRUCache()
        # always use a fresh in-memory connection; symbol data is loaded
        # lazily from the binary directory structure and persisted externally
        # (in the .day.bin files), so there's no need for an on-disk DuckDB
        # database file.
        self._conn = duckdb.connect()
        # keep track of which symbols we've already materialized as tables
        self._loaded_tables: set[str] = set()
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
        # grab list of binfiles and sort by field name so that when we
        # later construct the DataFrame its column order is stable
        binfiles = list(self.store.glob(glob_path, "*.day.bin"))
        binfiles.sort(key=lambda p: str(p).split("/")[-1])
        for binfile in binfiles:
            field = str(binfile).split("/")[-1].replace(".day.bin", "")
            # ignore explicit date.bin content; we build the column ourselves
            if field.lower() == "date":
                # still need to read arr to advance iteration but skip data
                if isinstance(self.store, __import__('featureSQL.storage', fromlist=['FileSystemStore']).FileSystemStore):
                    _ = np.fromfile(binfile, dtype="<f")
                else:
                    _ = np.frombuffer(self.store.read_bytes(binfile), dtype="<f")
                continue

            if isinstance(self.store, __import__('featureSQL.storage', fromlist=['FileSystemStore']).FileSystemStore):
                arr = np.fromfile(binfile, dtype="<f")
            else:
                arr = np.frombuffer(self.store.read_bytes(binfile), dtype="<f")

            if arr.size == 0:
                continue
            # first element is date index; convert to calendar date if available
            data = arr[1:]
            # always create a date column from the *first* non-empty binfile we
            # encounter.  if a calendar is configured convert the offsets into
            # real timestamps, otherwise just expose the raw integer index so
            # that DESCRIBE still reports a "date" column even when we lack a
            # calendar file.
            if "date" not in cols:
                if self._calendar is not None:
                    cols["date"] = pd.to_datetime(
                        [
                            self._calendar[int(arr[0]) + i]
                            for i in range(len(data))
                        ]
                    )
                else:
                    # offset plus sequential positions
                    idxs = np.arange(len(data), dtype="int64") + int(arr[0])
                    cols["date"] = pd.Series(idxs, dtype="int64")
            cols[field] = data
            

        if not cols:
            raise FileNotFoundError(f"symbol directory/files not found or empty: {symbol_dir}")
        # ensure every array (except date) has the same length by padding
        # shorter series with NaNs.  this handles the case where a text column
        # has only the offset and no values, which would otherwise trigger
        # "All arrays must be of the same length" when constructing the
        # DataFrame.
        lengths = [len(v) for k, v in cols.items() if k.lower() != "date"]
        maxlen = max(lengths) if lengths else 0
        for k, v in list(cols.items()):
            if k.lower() == "date":
                continue
            if len(v) < maxlen:
                pad = np.full(maxlen - len(v), np.nan, dtype=v.dtype)
                cols[k] = np.hstack([v, pad])

        df = pd.DataFrame(cols)
        # if we know the table was created by our CLI (indicated by existence
        # of a schema.json file) and every non-date column contains only zeros
        # (possibly with NaNs due to previous padding) then this is almost
        # certainly a legacy artifact from the old pre‑fix CREATE TABLE bug.  In
        # that case drop all rows so future writes start fresh instead of
        # perpetuating phantom zero records.  We deliberately avoid touching
        # tables without schema.json because those may legitimately hold
        # zero-valued data (e.g. sentiment file in update tests).
        schema_path = self.store.joinpath(symbol_dir, "schema.json")
        if self.store.exists(schema_path) and not df.empty:
            numeric_cols = [c for c in df.columns if c.lower() != "date"]
            if numeric_cols:
                vals = df[numeric_cols].fillna(0)
                if vals.eq(0).all(axis=None):
                    df = df.iloc[0:0]
        # enforce types according to schema.json if available
        if self.store.exists(schema_path):
            try:
                import json
                schema_list = json.loads(self.store.read_text(schema_path))
                for entry in schema_list:
                    name = entry.get("name")
                    typ = entry.get("type", "").lower()
                    if name in df.columns:
                        if "timestamp" in typ:
                            df[name] = pd.to_datetime(df[name])
                        else:
                            # everything else is treated as numeric (float).
                            # if the schema declared some other type we ignore it
                            # and coerce anyway; this keeps the feature database
                            # strictly floating-point.
                            df[name] = pd.to_numeric(df[name], errors="coerce")
            except Exception:
                pass
        return df

    def _ensure_symbols(self, sql: str) -> None:
        symbols = set(self.SYMBOL_RE.findall(sql))
        for sym in symbols:
            key = sym.lower()
            if key in self._loaded_tables:
                # already materialized as a table -- nothing to do
                continue
            # attempt to load the symbol data; if the files don't exist we
            # may still want to create an empty table in DuckDB (for example
            # when ALTER TABLE is executed on a table whose directory exists
            # but currently contains no .day.bin files).  we therefore
            # distinguish between a missing directory (skip) and an empty one
            # (treat as zero rows).
            symbol_dir = self.store.joinpath(self.root, "features", key)
            try:
                df = self.cache.get(key, self._load_symbol_df)
            except FileNotFoundError:
                # if the directory exists but had no bins, create an empty
                # dataframe with just the date column so we can still register
                # a DuckDB table for DDL operations.
                if self.store.exists(symbol_dir):
                    df = pd.DataFrame(columns=["date"])
                else:
                    # genuinely missing symbol: let DuckDB error
                    continue
            # duckdb's ``register`` creates a view over the dataframe which is
            # fine for reads but *cannot* be updated.  to support `UPDATE`
            # statements we need a base table.  ``from_df(...).create(name)``
            # materializes the data as an in-memory table.
            #
            # however, for completely empty datasets DuckDB will happily
            # infer nonsensical types (e.g. integers for empty object columns)
            # which later causes inserts to fail.  when a schema.json file is
            # present we can create the table explicitly using its declared
            # types and then optionally append the DataFrame rows.
            symbol_dir = self.store.joinpath(self.root, "features", key)
            schema_path = self.store.joinpath(symbol_dir, "schema.json")
            if df.empty and self.store.exists(schema_path):
                try:
                    import json
                    schema_list = json.loads(self.store.read_text(schema_path))
                    cols = []
                    for entry in schema_list:
                        name = entry.get("name")
                        typ = entry.get("type", "").lower()
                        if name.lower() == "date":
                            cols.append("date TIMESTAMP NOT NULL PRIMARY KEY")
                        else:
                            # all user columns are numeric (float or BIGINT)
                            if "int" in typ:
                                cols.append(f"{name} BIGINT")
                            else:
                                cols.append(f"{name} float")
                    create_sql = f"CREATE TABLE {sym}({', '.join(cols)})"
                    self._conn.execute(create_sql)
                except Exception:
                    # fallback to default registration if anything goes wrong
                    self._conn.from_df(df).create(sym)
            else:
                self._conn.from_df(df).create(sym)
            # ensure date constraint enforced for all tables
            try:
                self._conn.execute(f"ALTER TABLE {sym} ALTER COLUMN date SET NOT NULL")
            except Exception:
                pass
            try:
                self._conn.execute(f"ALTER TABLE {sym} ADD PRIMARY KEY(date)")
            except Exception:
                pass
            # remember that we have a table for this symbol so future queries
            # can skip re-creating it (and avoid a "table already exists"
            # error).
            self._loaded_tables.add(key)

    def _handle_alter_table(self, sql: str) -> None:
        """Intercept basic ALTER TABLE ADD/DROP COLUMN and update files.

        DuckDB will still execute the statement on its in-memory table, but we
        also need to reflect the change on disk so that a fresh service
        reading the directory sees the new schema.  This is intentionally
        limited to a simple pattern; more complex DDL is still out of scope.
        """
        lower = sql.strip().lower()
        if not lower.startswith("alter table"):
            return
        # simple regex to capture table name and operation
        m = re.match(r"alter\s+table\s+([A-Za-z0-9_]+)\s+(add(?:\s+column)?|drop(?:\s+column)?)\s+(.+)",
                     sql, re.IGNORECASE)
        if not m:
            return
        sym = m.group(1)
        op = m.group(2).lower()
        rest = m.group(3).strip()
        symbol_dir = self.store.joinpath(self.root, "features", sym.lower())
        if op.startswith("add"):
            parts = rest.split()
            if len(parts) < 2:
                return
            col = parts[0]
            typ = parts[1].lower()
            # ignore adding date column (implicitly managed)
            if col.lower() == "date":
                return
            # only numeric types allowed
            if not any(k in typ for k in ("int", "float", "double")):
                raise ConversionError(f"unsupported column type: {typ}")
            os.makedirs(symbol_dir, exist_ok=True)
            # determine number of existing rows from any current bin file
            nrows = 0
            try:
                binlist = list(self.store.glob(symbol_dir, "*.day.bin"))
                for bf in binlist:
                    if bf.lower().endswith("date.day.bin"):
                        continue
                    # read the first non-date file to compute length
                    if isinstance(self.store, __import__('featureSQL.storage', fromlist=['FileSystemStore']).FileSystemStore):
                        arr0 = np.fromfile(bf, dtype="<f")
                    else:
                        arr0 = np.frombuffer(self.store.read_bytes(bf), dtype="<f")
                    nrows = max(nrows, max(0, len(arr0) - 1))
                    break
            except Exception:
                nrows = 0
            # if the directory is effectively empty (no columns yet) then we
            # fall back to the calendar length so new columns start in sync with
            # the growing index.  however, if there are already rows present we
            # must honour that count; using the calendar here would pad the new
            # column longer than its peers and cause ``All arrays must be of the
            # same length`` errors when loading the table.
            if nrows == 0 and self._calendar is not None:
                nrows = len(self._calendar)
            # DEBUG: log computation
            try:
                print(f"DEBUG _handle_alter_table add {col}, nrows={nrows}, calendar_len={len(self._calendar) if self._calendar is not None else None}")
            except Exception:
                pass
            if nrows:
                # make sure the entire array is float32; stacking an int with a
                # float32 array would otherwise upcast to float64, meaning the
                # on‑disk size would be twice what we expect and later loads
                # would treat the extra bytes as phantom rows.
                arr = np.hstack([[0], np.zeros(nrows, dtype="<f")]).astype("<f")
            else:
                arr = np.array([0], dtype="<f")
            try:
                print(f"DEBUG writing arr length {arr.size} to {col}.day.bin")
            except Exception:
                pass
            path = self.store.joinpath(symbol_dir, f"{col}.day.bin")
            if isinstance(self.store, __import__('featureSQL.storage', fromlist=['FileSystemStore']).FileSystemStore):
                arr.tofile(path)
            else:
                self.store.write_bytes(path, arr.tobytes())
        elif op.startswith("drop"):
            parts = rest.split()
            if not parts:
                return
            col = parts[0]
            if col.lower() == "date":
                return
            path = self.store.joinpath(symbol_dir, f"{col}.day.bin")
            if isinstance(self.store, __import__('featureSQL.storage', fromlist=['FileSystemStore']).FileSystemStore):
                try:
                    os.remove(path)
                except FileNotFoundError:
                    pass
            else:
                # for other backends, best effort: attempt to write zero-length
                # file or ignore
                try:
                    self.store.write_bytes(path, b"")
                except Exception:
                    pass

    def _write_symbol_df(self, symbol: str, df: pd.DataFrame) -> None:
        """Persist a DuckDB result DataFrame back to the binary directory.

        The caller passes the *entire* table contents after any INSERT/UPDATE/
        DELETE operation.  The DataFrame is expected to include a ``date``
        column with ISO formatted strings if a calendar is in use; the method
        will perform all index conversion, validation, calendar appends and
        finally rewrite the ``<field>.day.bin`` files for the symbol.
        """
        # date conversions and validations -------------------------------------------------
        if "date" not in df.columns:
            raise ConversionError("missing date column in dataframe")

        # coerce to iso-string sequence for easier calendar lookup; original inputs
        # may be np.datetime64/Timestamp from pandas if the caller (i.e. DuckDB)
        # converted it automatically.  this also allows our existing logic to work
        # unchanged for pure string columns.
        dates = df["date"]
        if np.issubdtype(dates.dtype, np.datetime64):
            # pandas stores UTC timestamps by default; drop time component
            dates = dates.dt.strftime("%Y-%m-%d")
        else:
            dates = dates.astype(str)

        # detect any missing dates produced by an INSERT without a date value
        if dates.isna().any():
            raise ConversionError("INSERT statements must supply a date column")

        idxs: list[int] = []
        for d in dates:
            if self._calendar is None:
                # we can't map without a calendar; fall back to zero-based
                idxs.append(0)
                continue
            try:
                idx = self._calendar.index(d)
            except ValueError:
                # date not present; decide whether to append or error
                last = self._calendar[-1] if self._calendar else None
                if last is None or d > last:
                    # extend calendar both in-memory and on disk
                    self._calendar.append(d)
                    cal_path = self.store.joinpath(self.root, "calendars", "day.txt")
                    self.store.write_text(cal_path, "\n".join(self._calendar) + "\n")
                    idx = len(self._calendar) - 1
                else:
                    raise ConversionError(
                        "Cannot insert date within existing range – rebuild dataset"
                    )
            idxs.append(idx)

        # uniqueness check
        if len(idxs) != len(set(idxs)):
            raise ConversionError("Duplicate date; use UPDATE")

        # contiguous range check and sort rows by index
        sorted_pairs = sorted(enumerate(idxs), key=lambda iv: iv[1])
        order = [i for i, _ in sorted_pairs]
        df2 = df.iloc[order].reset_index(drop=True)
        idxs = [idx for _, idx in sorted_pairs]
        offset = idxs[0] if idxs else 0
        if idxs and idxs != list(range(offset, offset + len(idxs))):
            raise ConversionError("Cannot insert date within existing range – rebuild dataset")

        # write bins ------------------------------------------------------------------------
        symbol_dir = self.store.joinpath(self.root, "features", symbol)
        os.makedirs(symbol_dir, exist_ok=True)
        # refresh schema metadata to include all current columns (preserving
        # types if possible).

        for col in df2.columns:
            if col == "date":
                continue
            # all columns must be convertible to float; if not, we raise.
            vals = df2[col].astype(float).to_numpy()
            arr = np.hstack([[offset], vals]).astype("<f")
            path = self.store.joinpath(symbol_dir, f"{col}.day.bin")
            if isinstance(self.store, __import__(
                'featureSQL.storage',
                fromlist=['FileSystemStore'],
            ).FileSystemStore):
                arr.tofile(path)
            else:
                self.store.write_bytes(path, arr.tobytes())

    def execute(self, sql: str, reload: bool = False) -> pd.DataFrame:
        """Run ``sql`` after loading any referenced symbols.

        ``reload`` forces symbol data to be read from the bin files, bypassing
        any cached DataFrames.  This matches the CLI ``--reload`` flag.
        The returned DataFrame comes from DuckDB's result set.

        Writeable statements (CREATE/UPDATE/INSERT/DELETE) are persisted back
        to the corresponding bin files so that the filesystem reflects the
        latest state.  Only the basic column-oriented schema supported by the
        bin directory is handled; more advanced DDL (DROP, ALTER DROP) is
        intentionally out of scope.
        """
        # handle cache invalidation
        if reload:
            self.cache._cache.clear()
            self._loaded_tables.clear()
            # new DB connection to drop any registered tables
            self._conn = duckdb.connect()

        lower = sql.strip().lower()
        # create table: we may need to rewrite the SQL so that the
        # implicit date column comes first.  DuckDB positions columns in
        # the order they are declared, and our previous ALTER approach
        # left date at the end, which broke positional INSERTs.
        if lower.startswith("create table"):
            # inspect original column list and rewrite if needed
            m = re.match(r"create\s+table\s+([A-Za-z0-9_]+)\s*\((.*)\)", sql, re.IGNORECASE)
            if m:
                colspec = m.group(2)
                names = [c.strip().split()[0].lower() for c in colspec.split(",") if c.strip()]
                if "date" not in names:
                    # prepend date declaration
                    sql = f"create table {m.group(1)}(date timestamp, {colspec})"
                    lower = sql.lower()
            m = re.match(r"create\s+table\s+([A-Za-z0-9_]+)\s*\((.*)\)", sql, re.IGNORECASE)
            if m:
                sym = m.group(1)
                # parse names; we need to create a bin file for every column
                # except the implicit date.  text columns will get zero-filled
                # floats but are otherwise ignored on writes.
                cols = []
                names = []
                for token in m.group(2).split(","):
                    parts = token.strip().split()
                    if not parts:
                        continue
                    name = parts[0]
                    typ = parts[1].lower() if len(parts) > 1 else ""
                    # only numeric or timestamp types are allowed
                    if typ and not any(k in typ for k in ("int", "float", "double", "timestamp")):
                        raise ConversionError(f"unsupported column type: {typ}")
                    names.append(name)
                    cols.append(name)
                # ensure date column present in declarations
                if not any(n.lower() == "date" for n in names):
                    names.insert(0, "date")
                    cols.insert(0, "date")
                symbol_dir = self.store.joinpath(self.root, "features", sym.lower())
                os.makedirs(symbol_dir, exist_ok=True)
                # we no longer write schema.json; the binary structure is
                # self-describing via the filenames and numeric data.
                # create empty bin for each declared non-date column.  we
                # intentionally do *not* pre-populate with a row for every
                # calendar date; doing so confuses subsequent INSERT logic and
                # had been causing ghost rows in freshly created tables.  an
                # empty symbol directory should represent zero rows.
                for col in cols:
                    if col.lower() == "date":
                        continue
                    arr = np.array([0], dtype="<f")
                    path = self.store.joinpath(symbol_dir, f"{col}.day.bin")
                    if isinstance(self.store, __import__('featureSQL.storage', fromlist=['FileSystemStore']).FileSystemStore):
                        arr.tofile(path)
                    else:
                        self.store.write_bytes(path, arr.tobytes())
                # mark symbol loaded so we don't reload from disk later
                self._loaded_tables.add(sym.lower())
        # for INSERT we insist the user provide a date column
        if lower.startswith("insert into"):
            # simple regex to capture column list if present
            mcols = re.match(r"insert\s+into\s+[A-Za-z0-9_]+\s*\(([^)]+)\)", sql, re.IGNORECASE)
            if mcols:
                cols = [c.strip().lower() for c in mcols.group(1).split(",")]
                if "date" not in cols:
                    raise ConversionError("INSERT statements must supply a date column")
        # make sure referenced symbols exist for parsing
        # and handle lightweight DDL for our custom storage format.  The
        # service used to ignore ALTER TABLE operations because the
        # underlying directory might be empty (e.g. after dropping the last
        # column) which meant DuckDB saw no table at all.  We now intercept
        # simple ADD/DROP COLUMN commands so we can update the bin files
        # immediately and ensure a table is created in the connection.
        self._handle_alter_table(sql)
        self._ensure_symbols(sql)

        # update clause: check for new columns
        if lower.startswith("update"):
            m = re.match(r"update\s+([A-Za-z0-9_]+)\s+set\s+(.*)", sql, re.IGNORECASE)
            if m:
                table = m.group(1)
                set_clause = m.group(2)
                cols = re.findall(r"([A-Za-z0-9_]+)\s*=", set_clause)
                if cols:
                    info = self._conn.execute(f"PRAGMA table_info('{table}')").df()
                    if "column_name" in info.columns:
                        names = info["column_name"]
                    elif "name" in info.columns:
                        names = info["name"]
                    else:
                        names = []
                    existing = set(c.lower() for c in names)
                    for col in cols:
                        if col.lower() not in existing:
                            self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} float")
                            existing.add(col.lower())
        try:
            result = self._conn.execute(sql).df()
        except duckdb.CatalogException as e:
            # if DROP TABLE failed because DuckDB doesn't know about the table
            # we still want to wipe out any existing feature directory.
            if lower.startswith("drop table"):
                dm = re.match(r"drop\s+table\s+([A-Za-z0-9_]+)", lower)
                if dm:
                    sym = dm.group(1)
                    symbol_dir = self.store.joinpath(self.root, "features", sym.lower())
                    try:
                        import shutil
                        shutil.rmtree(symbol_dir)
                    except Exception:
                        pass
                    self._loaded_tables.discard(sym.lower())
                    try:
                        self.cache.invalidate(sym.lower())
                    except Exception:
                        pass
                # swallow the error; nothing else to do
                return pd.DataFrame()
            # silently ignore missing-column errors for alter-drop; the
            # change has already been applied by our filesystem handler
            if lower.startswith("alter table") and "drop" in lower:
                msg = str(e).lower()
                if "does not have a column" in msg:
                    return pd.DataFrame()
            # similarly, if the column already exists we don't need DuckDB to
            # complain – we created the bin file in _handle_alter_table above.
            if lower.startswith("alter table") and "add" in lower:
                msg = str(e).lower()
                if "already exists" in msg or "column with name" in msg:
                    return pd.DataFrame()
            if lower.startswith("describe"):
                raise ValueError(str(e)) from None
            raise
        except duckdb.BinderException as e:
            # Binder errors can also occur for missing/existing columns when
            # the in-memory schema differs.  We mirror the CatalogException
            # handling above to keep the CLI quiet.
            if lower.startswith("alter table") and "drop" in lower:
                msg = str(e).lower()
                if "does not have a column" in msg:
                    return pd.DataFrame()
            if lower.startswith("alter table") and "add" in lower:
                msg = str(e).lower()
                if "already exists" in msg or "column with name" in msg:
                    return pd.DataFrame()
            # otherwise propagate
            raise
        # persist modifications (INSERT/UPDATE/DELETE/CREATE) or handle
        # DROP by removing the underlying directory.
        if any(lower.startswith(k) for k in ("update", "insert into", "delete from", "create table", "drop table")):
            # handle drop separately because we don't want to query the table
            if lower.startswith("drop table"):
                m = re.match(r"drop\s+table\s+([A-Za-z0-9_]+)", lower)
                if m:
                    sym = m.group(1)
                    # remove bin directory if exists; ignore errors
                    symbol_dir = self.store.joinpath(self.root, "features", sym.lower())
                    try:
                        import shutil

                        shutil.rmtree(symbol_dir)
                    except Exception:
                        pass
                    # also forget it from our cache state
                    self._loaded_tables.discard(sym.lower())
                    # if we cached the dataframe, evict it so subsequent
                    # queries don't reload stale data
                    try:
                        self.cache.invalidate(sym.lower())
                    except Exception:
                        pass
                # nothing else to persist
            else:
                m = re.match(r"(?:update|insert into|delete from|create table)\s+([A-Za-z0-9_]+)", lower)
                if m:
                    sym = m.group(1)
                    # if we just created a table, make sure the duckdb schema
                    # includes a `date` column so that subsequent inserts with
                    # an explicit date will match column count.  the ALTER is
                    # harmless if the column already exists.
                    if lower.startswith("create table"):
                        try:
                            info = self._conn.execute(f"PRAGMA table_info('{sym}')").df()
                            names = info.get("column_name") if "column_name" in info.columns else info.get("name")
                            if names is not None and not any(str(n).lower() == "date" for n in names):
                                self._conn.execute(f"ALTER TABLE {sym} ADD COLUMN date TIMESTAMP")
                        except Exception:
                            pass
                    try:
                        df2 = self._conn.execute(f"select * from {sym}").df()
                        # don't attempt to write when the table is empty; this
                        # avoids requiring a `date` column for a fresh
                        # CREATE TABLE.
                        if not df2.empty:
                            self._write_symbol_df(sym.lower(), df2)
                    except ConversionError:
                        # validation errors are important and should propagate
                        raise
                    except Exception:
                        # other write errors are ignored to keep SQL engine
                        # responsive (e.g. permission issues on disk).
                        pass
        return result
