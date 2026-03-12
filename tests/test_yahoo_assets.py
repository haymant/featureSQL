import pandas as pd

from featureSQL.cli import Run


def test_fx_spot_download_uses_asset_subdir(tmp_path, monkeypatch):
    def fake_history(self, start, end, interval):
        return pd.DataFrame(
            {
                "date": ["2020-01-01", "2020-01-02"],
                "open": [1.10, 1.11],
                "high": [1.11, 1.12],
                "low": [1.09, 1.10],
                "close": [1.10, 1.11],
            }
        )

    monkeypatch.setattr("featureSQL.yahoo.Ticker.history", fake_history)

    data_dir = tmp_path / "data"
    Run().download(
        asset_type="fx",
        mode="spot",
        start="2020-01-01",
        end="2020-01-02",
        symbols=["EURUSD=X"],
        data_path=str(data_dir),
        store_type="fs",
    )

    out_file = data_dir / "feature-csv" / "fx" / "EURUSD_X.csv"
    assert out_file.exists()
    df = pd.read_csv(out_file)
    assert len(df) == 1
    assert df.iloc[0]["date"] == "2020-01-02"


def test_correlation_download_creates_pair_csv(tmp_path, monkeypatch):
    closes = {
        "SPY": [100.0, 102.0, 101.0, 104.0],
        "QQQ": [200.0, 203.0, 201.0, 206.0],
    }

    def fake_history(self, start, end, interval):
        symbol = self.symbol if hasattr(self, "symbol") else self._symbols[0]
        return pd.DataFrame(
            {
                "date": ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04"],
                "close": closes[symbol],
            }
        )

    monkeypatch.setattr("featureSQL.yahoo.Ticker.history", fake_history)

    data_dir = tmp_path / "data"
    Run().download(
        asset_type="correlation",
        start="2020-01-01",
        end="2020-01-04",
        symbols="SPY:QQQ",
        data_path=str(data_dir),
        store_type="fs",
        correlation_window=2,
    )

    out_file = data_dir / "feature-csv" / "correlation" / "SPY__QQQ.csv"
    assert out_file.exists()
    df = pd.read_csv(out_file)
    assert "correlation" in df.columns
    assert "pair" in df.columns
    assert df["pair"].iloc[-1] == "SPY:QQQ"


def test_option_chain_download_creates_snapshot_csv(tmp_path, monkeypatch):
    def fake_option_chain(self):
        return pd.DataFrame(
            {
                "contractSymbol": ["AAPL260417C00100000", "AAPL260417P00100000"],
                "expiration": ["2026-04-17", "2026-04-17"],
                "strike": [100.0, 100.0],
                "optionType": ["call", "put"],
                "lastPrice": [6.5, 5.8],
                "regularMarketPrice": [102.0, 102.0],
            }
        )

    monkeypatch.setattr("featureSQL.yahoo.Ticker.option_chain", fake_option_chain)

    data_dir = tmp_path / "data"
    Run().download(
        asset_type="option",
        symbols=["AAPL"],
        data_path=str(data_dir),
        store_type="fs",
    )

    out_file = data_dir / "option-chain" / "AAPL.csv"
    assert out_file.exists()
    df = pd.read_csv(out_file)
    assert set(df["optionType"].str.lower()) == {"call", "put"}
    assert "snapshot_at" in df.columns