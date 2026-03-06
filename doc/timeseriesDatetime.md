# Time‑Series / Date Handling Design

The feature store is built around a *date primary key*, stored as an
integer offset into a shared calendar file.  The design below documents
how the SQL layer and binary layout interact and what invariants must be
maintained when performing CRUD operations.

## Binary layout recap
1. Each symbol has a directory `features/<symbol>`.  Every numeric column is
   a little-endian float file named `<field>.day.bin`.
2. The first element of each file is an integer offset (4‑byte float encoded)
   pointing into `calendars/day.txt` which lists every trading date one-per-line.
3. The remaining floats are field values for consecutive dates starting at the
   indicated offset.  The date column is not materialised on disk – it is
   implicitly derived from the calendar.

## Schema view via DESCRIBE
- When you `DESCRIBE foo`, the engine reads the bin files for `foo` and
  reports one column per file.
- The *date column* is special: although stored on disk as an integer index,
  the in-memory table uses a pandas `datetime64` series so that DuckDB
  reports the column type as `TIMESTAMP` (users previously saw `FLOAT`).
  In other words, describe now returns the date column as a proper
  datetime type instead of a string.
- Example output:
  ```text
  column_name column_type null key default extra
  date        TIMESTAMP  YES  ...

## INSERT behaviour
1. **Date required.** Every `INSERT` statement **must supply a `date` value**
   in `YYYY-MM-DD` format.  Without a date we cannot map into the calendar; a
   missing date now raises a ``ConversionError``.
2. **Convert to index.** When the SQL has executed the resulting table is
   passed to `DuckQueryService._write_symbol_df`, which walks the DataFrame's
   ``date`` column and converts each string into its integer offset by
   searching `calendars/day.txt`.
3. **Uniqueness check.** The numeric index list is checked for duplicates.
   If the new row uses an index already present the operation fails with the
   message ``Duplicate date; use UPDATE``.
4. **Calendar extension.** If the supplied date is later than the last
   entry in `day.txt` we append it to the calendar before writing the new row.
   (Note that the date must be strictly increasing; see next point.)
5. **No blobs in range.** Retaining the contiguous-offset property is crucial.
   After conversion the sequence of indices must form a single contiguous
   range.  Attempting to insert a date that falls *between* two existing
   calendar entries (creating a hole) triggers a ``Cannot insert date within
   existing range – rebuild dataset`` error.
6. After validation we write the bin files by sorting the rows by date index,
   computing the starting offset and concatenating the float values.  All of
   this logic lives in `_write_symbol_df`, which rewrites `<field>.day.bin`
   files via the configured storage backend.

The separate "implementation plan" section below describes the exact
algorithm used.

## UPDATE behaviour
- `UPDATE` statements work exactly as before; the only addition is that any
  `WHERE` clause referencing the `date` column will automatically translate the
  supplied `'YYYY-MM-DD'` string into the corresponding index.  The underlying
  float bin is then updated in place for matching rows.
- `ALTER TABLE ... ADD COLUMN` also creates a new `<field>.day.bin` filled with
  zeros for existing dates.

## DELETE behaviour
- Deleting a row identifies dates via the same string‑to‑index conversion and
  removes the corresponding entry from every bin (shifting later values one step
  earlier).  This makes deletes potentially expensive; they may trigger a
  full rewrite of the symbol’s bins.

## Validation and error messages
- Attempting to insert without a `date` column, or with a malformed date,
  raises `ConversionError` (a lightweight subclass of ``ValueError`` defined in
  :mod:`featureSQL.duck`).
- If the date already exists the engine emits: `Duplicate date; use UPDATE`
  (mirrors SQL primary key violation semantics).
- If an insert would create a hole in the calendar the error reads: `Cannot
  insert date within existing range – rebuild dataset`.

## Implementation plan

The SQL layer must translate user-provided YYYY‑MM‑DD literals into the
integer offsets actually stored in the binary files and enforce the
constraints described above.  The work will be done in two places within
``DuckQueryService``:

1. **SQL execution wrapper (`execute`)**
   * Detect ``INSERT`` statements and ensure the column list explicitly
     includes ``date``.  Raise ``ConversionError`` if not.
   * For writeable statements we continue to call ``_write_symbol_df`` with the
     post‑operation DataFrame returned by DuckDB.  That helper will perform
     all remaining validation and conversion.

2. **Dataframe‑to‑bin writer (`_write_symbol_df`)**
   * Expect a ``date`` column containing ISO strings.  Reject any ``NaN``
     values (missing dates).
   * Map each string to its calendar index by searching ``self._calendar``.
     * If a string is not found and is strictly greater than the last entry,
       append it to both the in‑memory list and the on‑disk ``day.txt``.
     * If a string is not found but falls between existing dates, raise the
       ``Cannot insert date within existing range`` error.  This catches the
       “hole” scenario.
   * After mapping we have a list of integer indices.  Validate:
     * No duplicate indices (otherwise ``Duplicate date`` error).
     * The set of indices must form a contiguous range.  If rows have been
       deleted the start offset may be >0; that is fine, but gaps are not.
   * Reorder the DataFrame by ascending index and compute ``offset = first
     index``.  The binary array to write is ``[offset] + values`` for each
     non‑date column.
   * Write or overwrite ``<field>.day.bin`` files using the existing
     storage backend (supporting both filesystem and cloud).

With this plan in place we can implement the necessary logic and then add
unit tests covering the corner cases described below.

## Test cases to add

The existing test suite already exercises basic updates and reloads; the
following new tests are required to verify the date‑handling behaviour:

1. **Insert must include date** – executing ``INSERT INTO foo(id) VALUES (1)``
   should raise a ``ConversionError``.
2. **Successful insert with explicit date** – inserting a row with a date
   later than the current calendar should append both the calendar file and
   the corresponding value in every bin.
3. **Duplicate date error** – attempts to insert a date already present in
   the table must fail with the ``Duplicate date; use UPDATE`` message.
4. **Hole detection** – inserting a date that falls between existing
   calendar entries must raise the ``Cannot insert date within existing range``
   error.
5. **Calendar extension on insert** – verify the calendar file is updated and
   that repeated inserts continue to append correctly.
6. **CLI smoke checks** – exercise the new behaviour through the CLI so the
   error messages are propagated to users.

The newly added tests will live alongside the existing ones in
``tests/test_duck.py`` and ``tests/test_cli_query.py``.

## CLI flag
- The `Run.query()` method now accepts `--reload` (or `reload=True` from
  Python) to drop cached DataFrames.  This ensures conversions use the latest
  calendar and bins when they are modified outside the current session.

---

This design ensures that the date index remains the single source of truth,
and that the SQL layer behaves predictably for users familiar with standard
relational semantics but backed by our lightweight binary store.
