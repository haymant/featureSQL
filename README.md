This repository is now packaged as the **featureSQL** Python module.  You can
install it in one of two ways:

1. **From PyPI:**

   ```bash
   pip install featureSQL
   ```

   This makes the `featureSQL` console script available on your PATH and lets
   you `import featureSQL` from any Python program.

2. **From source (development mode):**

   ```bash
   cd /path/to/featureSQL
   pip install -e .
   ```

   This installs the package in editable mode, so local edits are reflected
   immediately without reinstalling.  Useful when working on the project.

After installation you can still use the original CLI helpers directly by
importing:

```python
from featureSQL.cli import Run
```

and invoking `Run().download(...)` or `featureSQL` at the shell.  The remainder
of this document is a how‑to reference that you can include in your own
projects; it covers the three common workflows you asked about.

---

## 1. Download a list of symbols (OCHLVF) to CSV

## 2. Dump downloaded CSVs to partitioned Parquet

The new `dump_parquet` subcommand converts the raw CSV files produced by
`download` into a Hive‑style partitioned Parquet dataset.  It mirrors the
example shown in `.testpy/gcs.py` and is useful when you want to process the
collection with engines such as DuckDB or Spark that understand Parquet.

```bash
uv run -m featureSQL.cli dump_parquet \
    --data_path ./source          # location of the feature-csv directory
    --out_root /tmp/parquet_output # directory where files will be written
```

By default the output is partitioned on `symbol` (ticker) and
year, with a `month` column added to every row for convenience.  The
command will create the target directory if it does not already exist.

Note that the parquet stage runs **after** the CSV download; if no CSVs are
produced for the requested symbols (e.g. network failures or invalid
tickers), the CLI will log a warning and skip the parquet dump instead of
raising a cryptic error.  Always confirm that CSV files exist under
`feature-csv/` (locally or in your bucket) before assuming the parquet
portion succeeded.

If you’d like the files pushed directly to GCS you can supply a bucket name
(or set the `GCS_BUCKET_NAME` environment variable) and pass
`--upload_gcs`.  When writing to a bucket the command will first read any
existing parquet dataset in the target prefix and merge it with the newly
concatenated CSVs, so subsequent runs **augment** the collection instead of
wiping it out:

```bash
export GCS_SC_JSON='...'
export GCS_BUCKET_NAME='my-edge-bucket'
uv run -m featureSQL.cli dump_parquet \
    --data_path ./source \
    --out_root /tmp/parquet_output \
    --upload_gcs
```

(Internally the code uses `google.cloud.storage.Client` in the same manner
as the standalone example.)

The remaining sections of this document describe other workflows.

## GCS end‑to‑end workflow

The following steps walk through preparing a clean bucket prefix, downloading
OCHLVF data and pushing it straight to Google Cloud Storage, then keeping the
collection up‑to‑date.  Adjust the bucket name (or set `GCS_BUCKET_NAME`) and
`--store_type gcs` flag as shown.

1. **Clean existing data path** (local or bucket).

   Clear the target directory on your filesystem or remove the prefix in the
   bucket.  For example, to remove everything under `gs://my-bucket/feature-csv`
   you can use the `gsutil` CLI or the web console:

   ```bash
   gsutil -m rm -r gs://my-bucket/feature-csv/**
   gsutil -m rm -r gs://my-bucket/calendars/**
   gsutil -m rm -r gs://my-bucket/features/**
   ```

   Similarly, remove any local data under `./source` if you previously used the
   same path.

2. **Initial download (all symbols or a subset) from 1990‑01‑01 to today**

   Here we demonstrate with a symbol list file; omit `--symbols_file` to grab
   the entire US universe.  The downloader writes CSVs into the bucket using
   the `gcs` store type.

   ```bash
   # ensure credentials are exported
   export GCS_SC_JSON='{"type":"service_account", …}'
   export GCS_BUCKET_NAME='my-bucket'

   uv run -m featureSQL.cli download \
       --region US \
       --start 2026-03-01 \
       --end   $(date +%F) \
       --symbols_file ./symbols.txt \
       --data_path $GCS_BUCKET_NAME \
       --store_type gcs \
       --out_format parquet
   ```

   Once successful the bucket will contain a `feature-csv/` prefix with one CSV
   per symbol.

3. **Binary dump and maintenance**

   After downloading, you can immediately run the dumper (or include
   `--out_format bin` on the same command as above).  The bin files are also
   written to GCS when using `--store_type gcs`.

   ```bash
   uv run -m featureSQL.cli download \
       --region US --start 1990-01-01 --end $(date +%F) \
       --symbols AAPL,MSFT \
       --data_path $GCS_BUCKET_NAME \
       --store_type gcs \
       --out_format bin
   ```

   To update the bucket with new data on subsequent days simply rerun the
   same command with an expanded date range or new symbols.  The downloader and
   dumper are idempotent; they will skip files that already exist and the
   `DumpDataUpdate` class appends to existing bins.

You can combine the upload-to-GCS logic with `dump_parquet` as described in
section 2 if you prefer Parquet output instead of the binary format.

## 3. Download symbols and produce the binary dataset in one go

```bash
# 1. prepare a text file listing tickers, one per line
cat > .testsymbols.txt <<'EOF'
AAPL
AMZN
GOOG
TSLA
EOF

# 2. run the collector, writing CSVs into a directory (they’ll land in
#    the `feature-csv` subfolder of whatever you pass to --data_path)
uv run -m featureSQL.cli download \
    --region US \
    --start 2026-01-01 \
    --end   2026-02-28 \
    --symbols_file ./.testsymbols.txt \
    --data_path ./source \
    --store_type fs   # Output to local fs (default for explicit flag).
                   # When no --store_type is given the CLI now defaults to
                   # 'gcs' and will use the bucket named by the
                   # GCS_BUCKET_NAME environment variable.
```

> **GCS setup:** when using `--store_type gcs` you must configure a
> target bucket name and some form of credentials before running any
> commands or tests.  One of the following authentication methods is
> supported:
>
> 1. **Service account JSON** – store the contents of a service account
>    file in the `GCS_SC_JSON` environment variable.  The JSON should look
>    like the file you normally download from Google Cloud Console.  This
>    is the legacy mechanism and continues to work unchanged.
>
> 2. **HMAC key pair** – set `GCS_KEY_ID` and `GCS_KEY_SECRET` to a Google
>    Cloud HMAC key and secret.  This is convenient for lightweight
>    environments (e.g. CI runners) where a full service account file is
>    inconvenient.  The helper will automatically switch to the HMAC
>    backend when both values are present.
>
> In either case supply the bucket via `GCS_BUCKET_NAME`.  The unit tests
> will skip network-dependent scenarios if the bucket name is unset, and
> they mock the client in other cases.  However, any manual invocation
> against a real bucket requires both variables to be correctly configured.
>
> The unit tests will skip network‑dependent scenarios if `GCS_BUCKET_NAME`
> is unset, and they mock the client in other cases.  However, any manual
> invocation against a real bucket requires both variables to be set.
>
> **Publishing:** to push a new release to PyPI, follow these steps:
>
> 1. build the package:
>    ```bash
>    uv run -m build
>    ```
> 2. optionally test the release on TestPyPI:
>    ```bash
>    uv run -m twine upload --repository testpypi dist/*
>    uv pip install --index-url https://test.pypi.org/simple/ --no-deps featureSQL
>    ```
> 3. when ready for production, upload to PyPI:
>    ```bash
>    uv run -m twine upload dist/*
>    ```

```bash
uv run -m featureSQL.dump_bin dump_all \
    --data_path ./source/feature-csv \
    --dump_dir   ./source/ \
    --exclude_fields symbol,date \
    --store_type fs         # don’t try to treat metadata as floats.
    # when invoking without any store_type the default is now 'gcs'
# (./source/ is /path/to/output/dir/)

> ⛑️ **Note:** recent releases automatically coerce a broad range of
> timestamp strings (including ISO‑8601 offsets) when building the
> calendar.  Invalid or malformed dates are dropped rather than causing a
> crash.  Additionally, the dumper now skips any non‑numeric columns
> (e.g. `symbol` or `date`) instead of trying to encode them, so you
> usually don’t need to provide `--exclude_fields` unless you want to
> explicitly whittle down the feature set.
```

After the command finishes you’ll have a structure like:

```
/path/to/output/dir/
  calendars/day.txt          # trading dates
  instruments/all.txt        # ticker start/end dates
  features/<ticker>/<field>.day.bin …
```

This directory can be consumed by any tool that understands the same layout – it’s a full **initialised** dataset.

> 💡 run `dump_all` only once per collection.  To add new data later, use `dump_update` (see next section).

---

## 3. Download symbols and produce the binary dataset in one go

The built‑in CLI helper (exposed via the `featureSQL` script or as
`python -m featureSQL.cli`) can drive both phases and even run ad‑hoc
queries against a binary dataset:

```bash
uv run -m featureSQL.cli download \
    --region US \
    --start 2026-01-01 \
    --end   2026-02-28 \
    --symbols AMZN,GOOG,TSLA \
    --data_path source  \
    --out_format bin \
    --store_type fs
```

This will

1. fetch OCHLVF data from Yahoo and save raw CSVs under
   `--{data_path}/feature-csv` (or `./source` if you didn’t pass
   `--data_path`),
2. run the binary dumper. On the **first** invocation the helper uses
   `DumpDataAll` to initialise the dataset; subsequent runs use
   `DumpDataUpdate` to append new days.  The target directory is the same as
   `--data_path` and you can still provide `exclude_fields="symbol,date"`.

You may also specify `--out_format parquet` instead of `bin`.  In that case
only the symbols you request (via `--symbols` or `--symbols_file`) are
converted; the utility consolidates CSVs, adds ``symbol``/``year``
columns if needed, and writes a Hive‑style partitioned Parquet tree under
`<data_path>/parquet` (or uploads directly to GCS when using
`--store_type gcs`).  Running the same command again will rebuild the
parquet files for the chosen symbols from the CSVs, effectively replacing the
existing files with a dataset that includes any newly downloaded dates.

The first run must be preceded by an empty or non‑existent output directory
(and/or you can manually run `dump_all` as shown above); thereafter the
same command will **append** new days to the existing bins.

## 4. Query the binary dataset using SQL

Once you have an initialised dataset you can issue SQL against it.  The
``query`` subcommand lazily loads only the symbols mentioned in the query
and keeps a small LRU cache in memory.

> **Note:** every table implicitly includes a `date` column of type
> `TIMESTAMP`.  This column is considered the primary key and is always
> `NOT NULL`—you must provide a date value on `INSERT`, and attempts to
> insert duplicate dates will fail with a constraint error.
>
> **DDL support:** the engine understands basic `CREATE`, `DROP`, `ALTER
> TABLE … ADD/DROP COLUMN` and simple `UPDATE`/`INSERT`/`DELETE`
> statements.  Adding or removing a column automatically creates or
> deletes the corresponding `.day.bin` file and keeps its length in sync
> with existing data, so you can evolve your schema in-place.

```bash
uv run -m featureSQL.cli query \
    --data_path source \
    --max_symbols 100 \
    --max_memory 2000000000 \
    --store_type fs \
# (or leave off the flag to default to your configured GCS bucket)
    "select date, open, close, high, low, volume, adjclose from AAPL where volume > 1000000"
```

Joins work transparently as long as both tables have been dumped:

```bash
uv run -m featureSQL.cli query --data_path source --store_type fs \
    "select a.open, n.close from AAPL a join NVDA n on a.date = n.date"
```

(The cache flags are optional; omit them to use unlimited resources.)

---

### 4.1 Example: run a sequence of SQL commands

The CLI can also be used interactively or in a script to exercise the
entire life‑cycle of a DuckDB table.  One convenient property of the
command is that it continues executing subsequent statements even if one
of the earlier ones fails (for instance, attempting to `DROP` or
`DESCRIBE` a table that doesn’t exist).  In such cases it prints a friendly
message but exits with a zero status, making it safe to run whole
scripts without worrying about short‑circuiting.

Below is a step‑by‑step sequence that matches the unit test we added earlier; it’s useful when you want to
verify that basic DDL/DML operations are behaving as expected.

```bash
# start a DuckDB session using the same Python runtime as the package
uv run -m featureSQL.cli query --data_path source --store_type fs \
    "\
    drop table foo;                     -- not present; CLI prints a message but continues\
    describe foo;                      -- should report missing table\
    create table foo (id int, price float, amount float);\
    describe foo;                      -- now shows three columns (date plus two numeric)
    insert into foo values ('2026-03-01',1,100.0),('2026-03-02',2,200.0);\
    select * from foo order by id;      -- two rows present\
    update foo set id = 10 where price=1;\
    select id from foo where price=1; -- id==10\
    alter table foo add disc float;     -- add a new numeric column\
    select * from foo;                 -- new column shows as NULL\
    alter table foo drop disc;         -- remove the column again\
    drop table foo;                     -- remove table\
    describe foo;                      -- failure again
    "
```

You can run the same commands one‑by‑one in an interactive Python REPL if
preferred; they simply exercise the underlying DuckDB engine that our
`DuckQueryService` uses.  This sequence is especially handy for sanity
checks when changing the SQL layer or adding new features.
---

### Notes & tips

- **Symbols list:** `--symbols` accepts a comma string, Python list/tuple, or
  `--symbols_file` path.  If you provide a file that exists but contains no
  tickers the downloader will now do nothing (previous behaviour fell through
  to downloading the entire US universe).
- **Reloading tickers:** use `--reload_symbols` to refresh the cached symbol list.
- **Alternate flows:** to skip the binning step, omit `--out_format` or set it
  to `yahoo` (the default).

#### Maintaining binary dataset integrity

Because the dumper appends new days based on the existing calendar, you
must take care when fetching overlapping or out‑of‑order date ranges:

1. **Always use a single start date that is at or before any previously
   downloaded data.**  For a continuous collection you can simply run:  
   ```bash
   uv run -m featureSQL.cli download \
       --region US \
       --start 2025-01-01 \
       --end   $(date +%Y-%m-%d) \
       --symbols TSLA \
       --data_path source --out_format bin
   ```
   The collector will skip existing CSVs and the dumper will append only the
   new days, keeping the existing bins and calendar consistent.

2. **If you need to back‑fill a gap or change the start date earlier:**
   delete the old bin files (or the entire `source/features` tree) and run
   with `--out_format bin` again (or manually use
   `uv run -m featureSQL.dump_bin dump_all`) so that `DumpDataAll` recomputes the
   calendar from scratch.  The update mode never rewrites the date index,
   so appending older data without rebuilding will cause the printed dates to
   be incorrect.

3. **Automate detection if desired.**
   You can extend the downloader to inspect the last date in existing CSVs
   and request only missing days, and/or modify the dumper to warn when the
   incoming data’s maximum date does not exceed the current calendar end.

Following these practices prevents “wrong” date offsets from appearing when
you use `uv run -m featureSQL.cli view …` (or simply `featureSQL view …`), and ensures the binary dataset remains a
faithful time series.

Feel free to copy‑paste these examples into your own docs or scripts!

---

## Viewing the contents of a bin file

A new CLI subcommand makes this easy without writing Python.  Once you have
an initialised dataset you can inspect any field file with:

```bash
uv run -m featureSQL.cli view /path/to/output/dir/features/aapl/open.day.bin --store_type fs
```

By default the command prints the starting date index and the shape of the
array, followed by each raw float value.  If the dataset contains a calendar
file (``calendars/day.txt``) the subcommand will automatically find it and
show the corresponding date for each value.  You can also supply an explicit
calendar path:

```bash
uv run -m featureSQL.cli view path/to/bin/file --calendar_file path/to/calendars/day.txt --store_type fs
```

Internally the helper still uses `numpy.fromfile` so the Python snippet
below remains available if you prefer to inspect the file manually.

```python
import numpy as np
# if you have a helper for converting codes to filenames, import it here
from featureSQL.dump_bin import code_to_fname

# point to a particular field file (e.g. open.day.bin) for one symbol
bin_path = "/path/to/output/dir/features/aapl/open.day.bin"
arr = np.fromfile(bin_path, dtype="<f")

# first value is the date offset, the rest are data values
date_index = int(arr[0])
values = arr[1:]
print(date_index, values.shape)
```

Alternatively, many tools provide helpers once a dataset is loaded; you can
query the field of a symbol and receive a NumPy array.  Use whatever API
your application or library supplies – the underlying files remain the same.

```pythonfeatureSQL
# pseudo-code using a generic loader
data = my_loader.load("/path/to/output/dir")
print(data['AAPL']['open'])
```

If you just want to look at the calendar, it’s a text file under
`calendars/day.txt` with one date per line.
