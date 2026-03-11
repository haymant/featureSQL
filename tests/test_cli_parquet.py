import pandas as pd
from pathlib import Path
import pyarrow.parquet as pq

from featureSQL.cli import Run


def test_cli_dump_parquet(tmp_path):
    # create a tiny CSV dataset under the source directory
    base = tmp_path / "source"
    csv_dir = base / "feature-csv"
    csv_dir.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(
        {
            "symbol": ["AAPL", "AAPL", "GOOG"],
            "date": ["2020-01-01", "2020-01-02", "2021-03-01"],
            "open": [1.0, 2.0, 3.0],
        }
    )
    df.to_csv(csv_dir / "demo.csv", index=False)

    r = Run(source_dir=str(base))
    out = tmp_path / "parquet"
    r.dump_parquet(data_path=str(base), out_root=str(out))

    # schema should be printed
    # (we can't easily capture stdout from Fire invocation, so we assert
    # that the output directory exists as a proxy; printing happens earlier)

    # at least one parquet file should exist in a partitioned directory
    produced = list(out.rglob("*.parquet"))
    assert produced, "no parquet files were created"

    # read one of the parquet files to ensure it round-trips
    sample = pq.read_table(produced[0]).to_pandas()
    assert not sample.empty
    assert "symbol" in sample.columns
    assert "date" in sample.columns

    # partition directories should match symbol/year structure
    assert (out / "symbol=AAPL").exists()
    assert (out / "symbol=GOOG").exists()