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
