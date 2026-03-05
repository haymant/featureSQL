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
    ):
        # determine symbol_list either from explicit symbols or file
        sym_list = None
        if symbols_file:
            path = Path(symbols_file).expanduser()
            if path.exists():
                sym_list = [s.strip().upper() for s in path.read_text().splitlines() if s.strip()]
            else:
                logger.warning(f"symbols_file {path} does not exist")
            # if the file exists but is empty, we still want to treat that as an
            # intentional (albeit odd) request to download nothing rather than
            # blow up and fetch the full universe.
            if reload_symbols or sym_list is None:
                # fetch fresh and optionally write back (only if explicitly
                # requested via reload_symbols)
                sym_list = get_us_stock_symbols(reload=True, data_path=data_path)
                try:
                    path.write_text("\n".join(sym_list))
                except Exception:
                    logger.warning(f"could not write symbol file {path}")
        elif symbols:
            # fire may give us a list/tuple, or a comma string
            if isinstance(symbols, (list, tuple)):
                sym_list = [s.strip().upper() for s in symbols if isinstance(s, str) and s.strip()]
            else:
                sym_list = [s.strip().upper() for s in str(symbols).split(",") if s.strip()]

        # if the caller passed a data_path we create a subdirectory for
        # feature CSVs; otherwise we also use a feature-csv subdir under the
        # configured source directory.  this keeps the top‑level dump/output
        # directory clean and mirrors what some external tooling expects.
        if data_path is not None:
            base = Path(data_path).expanduser()
        else:
            base = Path(self.source_dir).expanduser()
        csv_dir = base.joinpath("feature-csv")

        if region.upper() == "US":
            collector = YahooCollectorUS(str(csv_dir), symbol_list=sym_list)
        else:
            raise ValueError("region not supported")
        collector.download_data(start=start, end=end)

        # optionally produce binary dump if requested
        if out_format.lower() in ("bin", "dump"):
            try:
                # import from the package rather than a top-level module
                from .dump_bin import DumpDataUpdate, DumpDataAll

                dump_dir = data_path if data_path is not None else str(csv_dir)
                dump_path = Path(dump_dir)
                # decide whether to do a full initial dump or an update; the
                # former is required if the target directory does not yet
                # contain a calendar file.
                cal_file = dump_path.joinpath("calendars", "day.txt")
                if cal_file.exists():
                    dumper = DumpDataUpdate(
                        data_path=str(csv_dir),
                        dump_dir=dump_dir,
                        exclude_fields="symbol,date",
                    )
                else:
                    # ensure parent dirs exist
                    dump_path.mkdir(parents=True, exist_ok=True)
                    dumper = DumpDataAll(
                        data_path=str(csv_dir),
                        dump_dir=dump_dir,
                        exclude_fields="symbol,date",
                    )
                dumper.dump()
            except Exception as e:
                logger.warning(f"unable to perform binary dump: {e}")

    def normalize(self, source_dir: str = None):
        src = Path(source_dir or self.source_dir)
        for csv in src.glob("*.csv"):
            df = pd.read_csv(csv)
            df2 = YahooNormalize.normalize_yahoo(df)
            df2.to_csv(csv, index=False)

    def view(self, bin_file: str, calendar_file: str = None):
        """Print basic information about a binary feature file.

        The bin format starts with a 4-byte date index followed by
        little-endian floats.  The date index is an offset into a calendar
        file (one date per line) that lives in the dataset root under
        ``calendars/day.txt``.  When a `calendar_file` path is supplied (or if
        the code can automatically locate one by traversing upwards from the
        bin file location) this helper will print the corresponding date for
        each value.
        """
        path = Path(bin_file).expanduser()
        if not path.exists():
            logger.error(f"file not found: {path}")
            return
        try:
            arr = np.fromfile(path, dtype="<f")
        except Exception as e:
            logger.error(f"unable to read file {path}: {e}")
            return
        if arr.size == 0:
            print(f"{path} is empty")
            return
        date_index = int(arr[0])
        values = arr[1:]
        print(f"date index: {date_index}, values shape: {values.shape}")

        dates = None
        # locate calendar if not given
        if calendar_file:
            cal_path = Path(calendar_file).expanduser()
        else:
            cal_path = path
            for _ in range(5):
                cal_path = cal_path.parent
                candidate = cal_path.joinpath("calendars/day.txt")
                if candidate.exists():
                    cal_path = candidate
                    break
            else:
                cal_path = None
        if cal_path and cal_path.exists():
            try:
                dates = [line.strip() for line in cal_path.read_text().splitlines() if line.strip()]
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
    ):
        """Execute an SQL query over the binary dataset using DuckDB.

        The query engine lazily loads symbol directories as needed and uses an
        LRU cache to limit memory/number of symbols.  ``sql`` should be a
        valid SQL string referencing symbol names as table names.
        """
        from .duck import DuckQueryService, LRUCache

        base = Path(data_path) if data_path is not None else Path(self.source_dir)
        cache = LRUCache(max_symbols=max_symbols, max_memory=max_memory)
        svc = DuckQueryService(base, cache=cache)
        df = svc.execute(sql)
        # pretty print result
        print(df.to_string(index=False))


def main():
    fire.Fire(Run)


# ensure the CLI runs when the module is executed directly
if __name__ == "__main__":
    main()