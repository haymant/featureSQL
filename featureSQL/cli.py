"""CLI entrypoint for the package.

This module delegates Yahoo logic to ``featureSQL.yahoo`` and binary dumping to
``featureSQL.dump_bin``.  Only the user-facing ``Run`` class and the Fire
`main` helper remain here.
"""

from pathlib import Path
import os

import numpy as np
import pandas as pd
from loguru import logger
import fire

# import the yahoo helpers/collectors
from .yahoo import YahooCollectorUS, YahooNormalize, get_us_stock_symbols


# simple CLI using fire

class Run:
    def __init__(self, source_dir="./source"):
        self.source_dir = source_dir

    def download(
        self,
        region: str = "US",
        start: str = None,
        end: str = None,
        symbols: str = None,
        symbols_file: str = None,
        reload_symbols: bool = False,
        data_path: str = None,
        out_format: str = "csv",
        store_type: str = "fs",
    ):
        from .storage import get_storage
        # fire may occasionally treat an empty argument as a boolean flag,
        # resulting in ``data_path`` being True/False instead of a string.
        if store_type == "gcs" and (not data_path or not isinstance(data_path, str)):
            raise ValueError("--data_path must be supplied with a non-empty GCS bucket name when using store_type gcs")
        store = get_storage(store_type, data_path)

        # determine symbol_list either from explicit symbols or file
        sym_list = None
        if symbols_file:
            path = symbols_file
            def parse_lines(text: str):
                out = []
                for ln in text.splitlines():
                    if not ln.strip():
                        continue
                    # take first token separated by comma or whitespace
                    first = __import__("re").split(r"[,\s]+", ln.strip())[0]
                    out.append(first.upper())
                return out

            if store_type == "fs":
                path_obj = Path(symbols_file).expanduser()
                if path_obj.exists():
                    sym_list = parse_lines(path_obj.read_text())
                else:
                    logger.warning(f"symbols_file {path} does not exist")
            else:
                if store.exists(path):
                    sym_list = parse_lines(store.read_text(path))
                else:
                    logger.warning(f"symbols_file {path} does not exist in {store_type}")

            # if the file exists but is empty, we still want to treat that as an
            # intentional (albeit odd) request to download nothing rather than
            # blow up and fetch the full universe.
            if reload_symbols or sym_list is None:
                # fetch fresh and optionally write back (only if explicitly
                # requested via reload_symbols)
                sym_list = get_us_stock_symbols(reload=True, data_path=data_path, store=store)
                try:
                    store.write_text(path, "\n".join(sym_list))
                except Exception:
                    logger.warning(f"could not write symbol file {path}")
        elif symbols:
            # fire may give us a list/tuple, or a comma string
            if isinstance(symbols, (list, tuple)):
                sym_list = [s.strip().upper() for s in symbols if isinstance(s, str) and s.strip()]
            else:
                sym_list = [s.strip().upper() for s in str(symbols).split(",") if s.strip()]

        # if the caller passed a data_path we use that as the base;
        # otherwise fall back to the configured source directory.  the
        # storage backend will interpret the base string appropriately (e.g.
        # a bucket name/prefix for GCS).
        base = data_path if data_path is not None else self.source_dir
        csv_dir = store.joinpath(base, "feature-csv")

        if region.upper() == "US":
            collector = YahooCollectorUS(str(csv_dir), symbol_list=sym_list, store=store)
        else:
            raise ValueError("region not supported")
        collector.download_data(start=start, end=end)

        # optionally produce binary dump if requested
        if out_format.lower() in ("bin", "dump"):
            try:
                # import from the package rather than a top-level module
                from .dump_bin import DumpDataUpdate, DumpDataAll

                dump_dir = data_path if data_path is not None else csv_dir
                # decide whether to do a full initial dump or an update; the
                # former is required if the target directory does not yet
                # contain a calendar file.
                cal_file = store.joinpath(dump_dir, "calendars", "day.txt")
                if store.exists(cal_file):
                    dumper = DumpDataUpdate(
                        data_path=str(csv_dir),
                        dump_dir=dump_dir,
                        exclude_fields="symbol,date",
                        store_type=store_type,
                    )
                else:
                    dumper = DumpDataAll(
                        data_path=str(csv_dir),
                        dump_dir=dump_dir,
                        exclude_fields="symbol,date",
                        store_type=store_type,
                    )
                dumper.dump()
            except Exception as e:
                logger.warning(f"unable to perform binary dump: {e}")

        # parquet output support
        if out_format.lower() == "parquet":
            try:
                # before attempting to dump we verify that the downloader
                # actually produced CSVs for the requested symbols; this
                # prevents confusing "no csv files match requested symbols"
                # errors when the fetch failed or returned nothing.
                sym_filter = None
                if symbols is not None:
                    if isinstance(symbols, (list, tuple)):
                        sym_filter = {s.strip().upper() for s in symbols if isinstance(s, str) and s.strip()}
                    else:
                        sym_filter = {s.strip().upper() for s in str(symbols).split(",") if s.strip()}
                    if sym_filter:
                        # gather existing csv files and check for overlap
                        csv_root = store.joinpath(base, "feature-csv")
                        all_csvs = store.glob(csv_root, "*.csv")
                        matching = []
                        for f in all_csvs:
                            name = os.path.basename(f).split(".")[0].upper()
                            if name in sym_filter:
                                matching.append(f)
                        if not matching:
                            logger.warning(
                                "no csv files were downloaded for requested symbols; skipping parquet dump"
                            )
                            # skip the parquet stage entirely
                            sym_filter = None  # signal to avoid dumping
                # choose a local directory for parquet files; avoid stomping
                # the CSV tree when writing locally, and use a temporary
                # folder when targeting GCS so that upload logic can operate
                # from a known root.
                if store_type == "fs":
                    parquet_root = store.joinpath(base, "parquet")
                else:
                    parquet_root = os.path.join(os.getcwd(), "_parquet_temp")
                    # wipe any existing temporary data to ensure clean write
                    try:
                        import shutil

                        shutil.rmtree(parquet_root)
                    except Exception:
                        pass
                if sym_filter is not None:
                    self.dump_parquet(
                        data_path=base,
                        out_root=parquet_root,
                        upload_gcs=store_type == "gcs",
                        gcs_bucket=os.environ.get("GCS_BUCKET_NAME") if store_type == "gcs" else None,
                        store_type=store_type,
                        symbols=symbols if symbols is not None else None,
                    )
            except Exception as e:
                logger.warning(f"unable to perform parquet dump: {e}")

    def dump_parquet(
        self,
        data_path: str = None,
        out_root: str = "local_parquet_features",
        partition_cols: str = "symbol,year",
        upload_gcs: bool = False,
        gcs_bucket: str = None,
        store_type: str = "fs",
        symbols: list[str] | None = None,
    ):
        """Dump the CSV dataset to a partitioned Parquet collection.

        The CLI already knows how to download and normalize price data into the
        traditional ``feature-csv`` directory.  This helper reads every CSV
        file found under that directory, concatenates them, adds ``year`` and
        ``month`` fields based on the ``date`` column, and then writes the
        resulting table using :mod:`pyarrow.dataset` with Hive-style
        partitioning.  The default partitions are ``instrument`` and
        ``year`` which mirrors the example in ``.testpy/gcs.py``.

        If ``upload_gcs`` is ``True`` or ``gcs_bucket`` is provided the code
        will attempt to upload the generated parquet files to the specified
        Google Cloud Storage bucket.  Credentials are read from the
        ``GCS_SC_JSON`` environment variable and the usual service account
        JSON format is expected.

        Before writing, when targeting GCS the helper will also look for an
        existing parquet dataset at the same bucket/prefix.  Any rows found
        there are merged with the newly-read CSVs (duplicates dropped by
        symbol+date) so that repeated runs augment the collection instead of
        blowing away earlier data.  Hive partition columns such as ``symbol``
        are respected during the merge.
        """
        import os
        from .storage import get_storage

        base = data_path if data_path is not None else self.source_dir
        store = get_storage(store_type, base)

        # gather CSV paths; the downloader stores files under ``feature-csv``
        csv_root = store.joinpath(base, "feature-csv")
        csv_files = store.glob(csv_root, "*.csv")
        if not csv_files:
            print(f"no csv files found under {csv_root}")
            return

        # optionally restrict to a subset of symbols when provided; this
        # makes the routine idempotent and limits work when only a few
        # tickers are being updated (e.g. in the download() helper).
        if symbols is not None:
            # accept comma-separated string or any iterable of strings
            if isinstance(symbols, str):
                symbols = [s for s in symbols.split(",") if s.strip()]
            else:
                symbols = [s for s in symbols]
            symbol_set = {s.strip().upper() for s in symbols if isinstance(s, str)}
            filtered = []
            for f in csv_files:
                name = os.path.basename(f).split(".")[0].upper()
                if name in symbol_set:
                    filtered.append(f)
            csv_files = filtered
            if not csv_files:
                print("no csv files match requested symbols")
                return

        frames = []
        for csv in csv_files:
            try:
                if store_type == "fs":
                    df = pd.read_csv(csv)
                else:
                    import io

                    df = pd.read_csv(io.BytesIO(store.read_bytes(csv)))
            except Exception as e:
                logger.warning(f"failed to read {csv}: {e}")
                continue
            # some sources (e.g. YahooCollectorUS) don't include an explicit
            # "symbol" column, so infer from the file name if necessary.
            if "symbol" not in df.columns:
                inferred = os.path.basename(csv).split(".")[0]
                df["symbol"] = inferred
            frames.append(df)

        if not frames:
            print("no readable csv data")
            return

        df = pd.concat(frames, ignore_index=True)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["year"] = df["date"].dt.year
        df["month"] = df["date"].dt.month

        # if we are targeting GCS, attempt to pull in any existing parquet
        # dataset from the same bucket/prefix and merge it with the newly
        # concatenated CSV data.  This prevents the“overwrite everything”
        # behaviour that was previously observed when the downloader ran
        # against an already-populated bucket.
        if upload_gcs or store_type == "gcs":
            # determine dataset URI using the supplied base path; this will
            # typically look like "gs://bucket" or "gs://bucket/prefix".
            dataset_uri = f"gs://{base}" if base else None
            if dataset_uri:
                try:
                    import pyarrow.fs as fs
                    # ensure dataset module is available for the merge step
                    import pyarrow.dataset as ds

                    gcsfs = fs.GcsFileSystem()
                    existing_ds = ds.dataset(dataset_uri, filesystem=gcsfs, format="parquet", partitioning="hive")
                    existing_tbl = existing_ds.to_table()
                    old_df = existing_tbl.to_pandas()
                    if not old_df.empty:
                        df = pd.concat([old_df, df], ignore_index=True)
                        # dedupe on key columns so later rewrites don't create
                        # duplicate rows (symbol+date is the natural key).
                        df.drop_duplicates(subset=["symbol", "date"], inplace=True)
                except Exception as e:
                    # if the bucket is empty or unreadable just proceed with
                    # the CSV-only dataframe.
                    logger.warning(f"unable to read existing parquet from gcs: {e}")

        # ensure any object/boolean columns are stringified to avoid
        # pyarrow complaints about mixed types (e.g. symbol column sometimes
        # contains bools when read from CSVs).
        for col in df.select_dtypes(include=["object", "bool"]).columns:
            df[col] = df[col].astype(str)

        # prepare partitioning schema for pyarrow
        import pyarrow as pa
        import pyarrow.dataset as ds


        LOCAL_ROOT = Path(out_root)
        LOCAL_ROOT.mkdir(parents=True, exist_ok=True)

        # the partitioning logic is currently fixed to symbol-only; the
        # earlier version computed ``cols`` from ``partition_cols`` but the
        # CLI never exposed any other choice.  Leave the hard-coded schema in
        # place so that downstream processes can rely on a consistent layout.
        print("Partitioning by columns: ['symbol']")
        partitioning = ds.partitioning(pa.schema([("symbol", pa.string())]), flavor="hive")

        table = pa.Table.from_pandas(df)
        # display the table schema/title so user sees what was written
        try:
            print(f"Parquet table schema:\n{table.schema}")
        except Exception:
            # if printing fails for any reason, silently continue
            pass
        ds.write_dataset(
            table,
            base_dir=LOCAL_ROOT,
            partitioning=partitioning,
            format="parquet",
            existing_data_behavior="overwrite_or_ignore",
        )

        print(f"Local Parquet written to: {LOCAL_ROOT}")

        # optional upload to GCS
        if upload_gcs or gcs_bucket:
            bucket = gcs_bucket or os.environ.get("GCS_BUCKET_NAME")
            if not bucket:
                raise ValueError("gcs_bucket must be provided to upload")
            # reuse the example logic from .testpy/gcs.py
            from google.cloud import storage

            gcs_sc_json = os.environ.get("GCS_SC_JSON")
            if not gcs_sc_json:
                raise ValueError("Set env vars: GCS_SC_JSON for GCS upload")
            client = storage.Client.from_service_account_json("gcs.json")
            bkt = client.bucket(bucket)
            for file_path in LOCAL_ROOT.rglob("*.parquet"):
                blob_path = f"{file_path.relative_to(LOCAL_ROOT)}"
                blob = bkt.blob(blob_path)
                blob.upload_from_filename(file_path)
                print(f"Uploaded: {blob_path}")
            print(f"All files uploaded to gs://{bucket}/")

    def normalize(self, source_dir: str = None, store_type: str = "fs"):
        from .storage import get_storage
        store = get_storage(store_type, source_dir or self.source_dir)
        src = source_dir or self.source_dir
        
        # This uses pandas directly heavily, we will adapt it
        import io
        for csv_path in store.glob(src, "*.csv"):
            if store_type == "fs":
                df = pd.read_csv(csv_path)
            else:
                csv_bytes = store.read_bytes(csv_path)
                df = pd.read_csv(io.BytesIO(csv_bytes))
                
            df2 = YahooNormalize.normalize_yahoo(df)
            
            if store_type == "fs":
                df2.to_csv(csv_path, index=False)
            else:
                csv_buffer = io.StringIO()
                df2.to_csv(csv_buffer, index=False)
                store.write_text(csv_path, csv_buffer.getvalue())

    def view(self, bin_file: str, calendar_file: str = None, store_type: str = "fs", data_path: str = None):
        """Print basic information about a binary feature file.

        The bin format starts with a 4-byte date index followed by
        little-endian floats.  The date index is an offset into a calendar
        file (one date per line) that lives in the dataset root under
        ``calendars/day.txt``.  When a `calendar_file` path is supplied (or if
        the code can automatically locate one by traversing upwards from the
        bin file location) this helper will print the corresponding date for
        each value.
        """
        from .storage import get_storage
        # for gcs, bucket is data_path
        store = get_storage(store_type, data_path)

        if not store.exists(bin_file):
            logger.error(f"file not found: {bin_file}")
            return
        try:
            if store_type == "fs":
                arr = np.fromfile(bin_file, dtype="<f")
            else:
                arr = np.frombuffer(store.read_bytes(bin_file), dtype="<f")
        except Exception as e:
            logger.error(f"unable to read file {bin_file}: {e}")
            return
        if arr.size == 0:
            print(f"{bin_file} is empty")
            return
        date_index = int(arr[0])
        values = arr[1:]
        print(f"date index: {date_index}, values shape: {values.shape}")

        dates = None
        # locate calendar if not given
        if calendar_file:
            cal_path = calendar_file
        else:
            if store_type == "fs":
                path = Path(bin_file).expanduser()
                cal_path = path
                for _ in range(5):
                    cal_path = cal_path.parent
                    candidate = cal_path.joinpath("calendars/day.txt")
                    if candidate.exists():
                        cal_path = candidate
                        break
                else:
                    cal_path = None
                if cal_path: cal_path = str(cal_path)
            else:
                # GCS is flat, try removing segments
                parts = bin_file.split("/")
                cal_path = None
                for i in range(len(parts)-1, 0, -1):
                    prefix = "/".join(parts[:i])
                    candidate = f"{prefix}/calendars/day.txt" if prefix else "calendars/day.txt"
                    if store.exists(candidate):
                        cal_path = candidate
                        break
                        
        if cal_path and store.exists(cal_path):
            try:
                dates = [line.strip() for line in store.read_text(cal_path).splitlines() if line.strip()]
            except Exception:
                dates = None
        if dates is not None:
            # print date mapping for each value
            offset = date_index
            for i, val in enumerate(values):
                idx = offset + i
                date_str = dates[idx] if idx < len(dates) else "<out of range>"
                print(f"{date_str}: {val}")
        else:
            print(values)

    def query(
        self,
        sql: str,
        data_path: str = None,
        max_symbols: int = None,
        max_memory: int = None,
        store_type: str = "fs",
        reload: bool = False,
    ):
        """Execute an SQL query over the binary dataset using DuckDB.

        The query engine lazily loads symbol directories as needed and uses an
        LRU cache to limit memory/number of symbols.  ``sql`` should be a
        valid SQL string referencing symbol names as table names.  If
        ``reload`` is True symbol data is re-read from disk instead of using
        the cache.
        """
        from .duck import DuckQueryService, LRUCache
        from .storage import get_storage

        base = data_path if data_path is not None else self.source_dir
        store = get_storage(store_type, base)

        cache = LRUCache(max_symbols=max_symbols, max_memory=max_memory)
        svc = DuckQueryService(base, cache=cache, store=store)

        # split multi‑statement strings so that a failure in one doesn't
        # abort the entire batch.  DuckDB will raise on the first failing
        # statement, so we handle exceptions per-statement here.
        results = []
        for part in sql.split(";"):
            stmt = part.strip()
            if not stmt:
                continue
            try:
                df = svc.execute(stmt, reload=reload)
                results.append(df)
                print(df.to_string(index=False))
            except Exception as e:
                # we want to log the error but continue executing later
                msg = str(e)
                logger.error(msg)
                print(msg)
                # continue without appending a result
        # nothing to return; CLI exists with 0 regardless of errors
        return


def main():
    fire.Fire(Run)


# ensure the CLI runs when the module is executed directly
if __name__ == "__main__":
    main()