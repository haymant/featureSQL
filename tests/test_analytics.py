import math

import pandas as pd

from featureSQL.cli import Run
from featureSQL.volsurface import black_scholes_price


def test_boost_ir_curve_from_downloaded_ir_files(tmp_path, monkeypatch):
    rates = {
        "^IRX": 4.0,
        "^FVX": 4.5,
        "^TNX": 5.0,
    }

    def fake_history(self, start, end, interval):
        symbol = self.symbol if hasattr(self, "symbol") else self._symbols[0]
        return pd.DataFrame(
            {
                "date": ["2020-01-01", "2020-01-02"],
                "close": [rates[symbol], rates[symbol]],
            }
        )

    monkeypatch.setattr("featureSQL.yahoo.Ticker.history", fake_history)

    data_dir = tmp_path / "data"
    out_file = data_dir / "analytics" / "ir_curve.csv"
    runner = Run()
    runner.download(
        asset_type="ir",
        start="2020-01-01",
        end="2020-01-02",
        symbols=["^IRX", "^FVX", "^TNX"],
        data_path=str(data_dir),
        store_type="fs",
    )
    curve = runner.boost_ir_curve(data_path=str(data_dir), output_path=str(out_file))

    assert out_file.exists()
    assert list(curve["maturity_years"]) == [0.25, 5.0, 10.0]
    assert curve["discount_factor"].is_monotonic_decreasing


def test_calibrate_vol_surface_from_option_chain_csv(tmp_path):
    data_dir = tmp_path / "data"
    chain_dir = data_dir / "option-chain"
    chain_dir.mkdir(parents=True)

    spot = 100.0
    rate = 0.01
    sigma = 0.20
    valuation_date = pd.Timestamp("2026-03-12T00:00:00Z")
    expiry = pd.Timestamp("2026-06-12T00:00:00Z")
    expiry_years = (expiry - valuation_date).total_seconds() / (365.25 * 24 * 3600)
    prices = [
        black_scholes_price(spot, strike, expiry_years, rate, sigma, "call")
        for strike in [90.0, 100.0, 110.0]
    ]
    pd.DataFrame(
        {
            "contractSymbol": [
                "AAPL260612C00090000",
                "AAPL260612C00100000",
                "AAPL260612C00110000",
            ],
            "snapshot_at": [valuation_date.isoformat()] * 3,
            "expiration": [expiry.isoformat()] * 3,
            "strike": [90.0, 100.0, 110.0],
            "optionType": ["call", "call", "call"],
            "lastPrice": prices,
            "regularMarketPrice": [spot] * 3,
        }
    ).to_csv(chain_dir / "AAPL.csv", index=False)

    out_file = data_dir / "analytics" / "vol_surface.csv"
    surface = Run().calibrate_vol_surface(
        data_path=str(data_dir),
        rate=rate,
        output_path=str(out_file),
    )

    assert out_file.exists()
    assert not surface.empty
    assert surface["implied_vol"].between(0.19, 0.21).all()
    assert math.isclose(surface["moneyness"].iloc[1], 1.0, rel_tol=1e-6)