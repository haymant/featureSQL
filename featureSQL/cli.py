"""CLI entrypoint for the package.

This module delegates Yahoo logic to ``featureSQL.yahoo`` and binary dumping to
``featureSQL.dump_bin``.  Only the user-facing ``Run`` class and the Fire
`main` helper remain here.
"""

from pathlib import Path

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
    ):
        """Execute an SQL query over the binary dataset using DuckDB.

        The query engine lazily loads symbol directories as needed and uses an
        LRU cache to limit memory/number of symbols.  ``sql`` should be a
        valid SQL string referencing symbol names as table names.
        """
        from .duck import DuckQueryService, LRUCache
        from .storage import get_storage

        base = data_path if data_path is not None else self.source_dir
        store = get_storage(store_type, base)

        cache = LRUCache(max_symbols=max_symbols, max_memory=max_memory)
        svc = DuckQueryService(base, cache=cache, store=store)
        df = svc.execute(sql)
        # pretty print result
        print(df.to_string(index=False))


def main():
    fire.Fire(Run)


# ensure the CLI runs when the module is executed directly
if __name__ == "__main__":
    main()