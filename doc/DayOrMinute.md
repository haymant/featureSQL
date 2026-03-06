# Design: Supporting `--interval` (day vs. minute)

This document describes how the system would be extended to let the user
specify a time‑series granularity when downloading data.  The new
`--interval` argument is optional and defaults to `day`; when set to
`minute` the download, calendar, and binary naming adapt accordingly.

The important thing to realise up front is that **the underlying binary
format doesn’t fundamentally change** – every field is still a little‑endian
float file whose first element is an index into a calendar.  What varies
is how that calendar is constructed and which values it contains.

---

## 1. download behaviour

* `Run.download(interval: str = "day", …)` accepts `day` or `minute`.
* the collector (`YahooCollectorUS`) propagates the same value to
  `Ticker.history` via its `interval` parameter.  ``'1d'`` for day,
  ``'1m'`` for minute.
* data fetched for a minute interval contains per‑minute rows and a
  datetime index with timestamps down to the minute.  day downloads use
  a date‑only index.
* the CLI help text is updated accordingly.

In pseudo‑code:

```python
class Run:
    def download(self, ..., interval: str = "day", …):
        ...
        collector = YahooCollectorUS(..., interval=interval)
        collector.download_data(...)
```

and the collector uses it for the ticker call:

```python
df = t.history(start=start, end=end, interval="1d" if interval=="day" else "1m")
```


## 2. binary filenames

To keep the names self‑documenting the interval is appended to the
extension:

* day files: `open.day.bin`, `close.day.bin`, … (existing behaviour)
* minute files: `open.min.bin`, `close.min.bin`, …

The code that globs and writes files already uses `*.day.bin` patterns – it
will be extended to substitute the current interval string.  The
`DumpData*` classes would accept an `interval` parameter and pass it to
`_write_symbol_df` or similar when creating/writing bins.

Existing datasets with `.day.bin` remain untouched; minute datasets use
`.min.bin` so the two can coexist in the same root.


## 3. timestamp handling / calendar

The calendar is still a single file under `calendars/day.txt` regardless of
interval.  Its contents depend on the interval:

* **day**: one ISO date per line (the current behaviour).
* **minute**: we generate a minute‑level index from the daily calendar
  when the first minute file is written.  The generation uses the helper
  that appears in the Qlib snippets:

```python
generate_minutes_calendar_from_daily(
    calendars=daily_list,           # lines from day.txt
    freq="1min",
    am_range=("09:30:00","11:29:00"),
    pm_range=("13:00:00","14:59:00"),
)
```

This returns a `pd.Index` of `Timestamp`s (e.g. `2025-11-03 09:30:00`).
Our loader (`_load_symbol_df`) would inspect the file extension and, if
it’s `.min.bin`, convert the integer offsets into this generated minute
calendar instead of treating them as day strings.

When writing new rows via `_write_symbol_df` the date column supplied by
the user must be a full timestamp for minute data and a plain date for
day data.  The conversion logic already normalises to strings; it would be
extended to accept `datetime64` values and, when `interval == "minute"`, to
validate that the timestamp appears in the generated minute calendar.

The date‑uniqueness and hole checks remain the same, just over a finer
sequence of calendar entries.


## 4. testing

Tests would need to exercise both intervals:

1. create a tiny day dataset (as existing tests already do) and verify
   `describe`, `insert`, `update` etc.
2. create a tiny minute dataset by writing a `day.txt` with a couple of
   dates and generating the minute calendar in code.  files have a
   `.min.bin` suffix and tests check that `DESCRIBE` still reports a
   TIMESTAMP column and that inserts respect minute‑level ordering.
3. ensure that the downloader is invoked with the correct `interval`
   string; this can be done by monkeypatching `YahooCollectorUS` or by
   using the fake history helper in `tests/test_workflows.py`.
4. calendar‑append behaviour should append correct minute timestamps when
   inserting beyond the existing minute span (and reject inserts that
   would create holes).

Existing tests can be parameterised over `interval` to avoid duplication.
A simple `@pytest.mark.parametrize("interval", ["day","minute"])`
decorator on the core insert/update tests would cover both cases.


---

This expanded spec provides a roadmap for implementing `--interval`.
No code changes are made here; I'll wait for your go‑ahead before writing
any actual implementation.  Let me know if you want the CLI help text, the
SQL engine or the dump utilities to be sketched next.  The tests above
outline the necessary coverage.  👇
