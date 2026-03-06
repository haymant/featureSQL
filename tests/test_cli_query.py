import os
import numpy as np
from pathlib import Path

import pandas as pd
from featureSQL.cli import Run

# helper to create tiny bin files
import numpy as np
from pathlib import Path

def write_bin(path: Path, values: np.ndarray):
    arr = np.hstack([[0], values]).astype("<f")
    arr.tofile(path)


def test_cli_query(tmp_path, capsys):
    # set up simple dataset containing foo/bar
    # mimic the output directory: symbols are direct children of root
    base = tmp_path / "dataset"
    # create calendar and features directories
    (base / "calendars").mkdir(parents=True)
    (base / "calendars" / "day.txt").write_text("2020-01-01\n2020-01-02\n")
    for sym in ["foo", "bar"]:
        d = base / "features" / sym
        d.mkdir(parents=True)
        write_bin(d / "x.day.bin", np.array([1.0, 2.0]))

    r = Run(source_dir=str(base))
    # run query; expect printed dataframe
    r.query("select x from foo where x=1", data_path=str(base))
    captured = capsys.readouterr()
    assert "x" in captured.out
    assert "1.0" in captured.out


def test_cli_update(tmp_path, capsys):
    # build a minimal dataset with a calendar but no sentiment file initially;
    # the engine should add the column when the update is executed.
    base = tmp_path / "dataset2"
    (base / "calendars").mkdir(parents=True)
    dates = ["2026-01-28", "2026-01-29", "2026-02-10"]
    (base / "calendars" / "day.txt").write_text("\n".join(dates) + "\n")
    d = base / "features" / "aapl"
    d.mkdir(parents=True, exist_ok=True)
    # intentionally omit sentiment.bin to simulate missing column
    write_bin(d / "open.day.bin", np.ones(len(dates)))

    r = Run(source_dir=str(base))
    # run an update followed by a select in a single SQL string so we can
    # inspect the modified table in the same session.
    r.query(
        "update aapl set sentiment = 1 where date between '2026-01-28' and '2026-02-10';"
        "select sentiment from aapl order by date",
        data_path=str(base),
    )
    captured = capsys.readouterr()
    assert "sentiment" in captured.out
    # ensure the values printed reflect the update
    assert "1.0" in captured.out



def test_cli_describe_error(tmp_path, capsys):
    # describe a non-existent table should print an error and not raise
    base = tmp_path / "empty"
    base.mkdir()
    r = Run(source_dir=str(base))
    r.query("describe missing", data_path=str(base))
    captured = capsys.readouterr()
    # error text should appear on stdout as well
    out = captured.out.lower()
    assert "does not exist" in out or "error" in out


def test_cli_describe_date_type(tmp_path, capsys):
    # with a real symbol the CLI should show the date column as TIMESTAMP
    base = tmp_path / "dt"
    (base / "calendars").mkdir(parents=True)
    (base / "calendars" / "day.txt").write_text("2026-01-01\n")
    d = base / "features" / "aapl"
    d.mkdir(parents=True, exist_ok=True)
    write_bin(d / "open.day.bin", np.array([1.0]))
    r = Run(source_dir=str(base))
    r.query("describe aapl", data_path=str(base))
    captured = capsys.readouterr()
    out = captured.out.lower()
    assert "date" in out
    assert "timestamp" in out


def test_cli_describe_no_calendar(tmp_path, capsys):
    # creating a table with no calendar should still result in `date` being
    # listed by DESCRIBE.  we don't care about the type.
    base = tmp_path / "cli_nocal"
    base.mkdir()
    r = Run(source_dir=str(base))
    r.query("create table foo (id int, price float, amount float)", data_path=str(base))
    captured = capsys.readouterr()  # ignore create output
    r.query("describe foo", data_path=str(base))
    captured = capsys.readouterr()
    out = captured.out.lower()
    assert "date" in out
    # the two user columns should be mentioned
    assert "price" in out and "amount" in out


def test_cli_insert_requires_date(tmp_path, capsys):
    base = tmp_path / "cli_insert"
    (base / "calendars").mkdir(parents=True)
    (base / "calendars" / "day.txt").write_text("2026-01-01\n")
    d = base / "features" / "foo"
    d.mkdir(parents=True, exist_ok=True)
    write_bin(d / "x.day.bin", np.array([0]))
    r = Run(source_dir=str(base))
    r.query("insert into foo(x) values (1)", data_path=str(base))
    captured = capsys.readouterr()
    assert "date" in captured.out.lower()


def test_cli_insert_duplicate(tmp_path, capsys):
    base = tmp_path / "cli_dup"
    (base / "calendars").mkdir(parents=True)
    (base / "calendars" / "day.txt").write_text("2026-01-01\n")
    d = base / "features" / "foo"
    d.mkdir(parents=True, exist_ok=True)
    write_bin(d / "x.day.bin", np.array([0]))
    r = Run(source_dir=str(base))
    # insert with an existing date should fail
    r.query("insert into foo(date,x) values ('2026-01-01', 5)", data_path=str(base))
    captured = capsys.readouterr()
    assert "duplicate" in captured.out.lower() or "update" in captured.out.lower()
