"""Interest-rate curve helpers."""

from __future__ import annotations

import math
from typing import Iterable

import pandas as pd


IR_TENOR_MAP = {
    "^IRX": 0.25,
    "^FVX": 5.0,
    "^TNX": 10.0,
    "^TYX": 30.0,
}


def infer_maturity_years(symbol: str) -> float | None:
    return IR_TENOR_MAP.get(str(symbol).upper())


def build_curve_instruments(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    frame = df.copy()
    if "maturity_years" not in frame.columns:
        if "symbol" not in frame.columns:
            raise ValueError("curve instruments require symbol or maturity_years column")
        frame["maturity_years"] = frame["symbol"].map(infer_maturity_years)
    if frame["maturity_years"].isna().any():
        missing = sorted(frame.loc[frame["maturity_years"].isna(), "symbol"].astype(str).unique())
        raise ValueError(f"unable to infer maturity_years for symbols: {missing}")
    if "rate" not in frame.columns:
        if "close" not in frame.columns:
            raise ValueError("curve instruments require rate or close column")
        frame["rate"] = pd.to_numeric(frame["close"], errors="coerce") / 100.0
    frame["maturity_years"] = pd.to_numeric(frame["maturity_years"], errors="coerce")
    frame["rate"] = pd.to_numeric(frame["rate"], errors="coerce")
    frame = frame.dropna(subset=["maturity_years", "rate"]).copy()
    if "instrument_type" not in frame.columns:
        frame["instrument_type"] = "zero"
    return frame.sort_values("maturity_years")


def boost_ir_curve(instruments: pd.DataFrame | Iterable[dict]) -> pd.DataFrame:
    frame = build_curve_instruments(pd.DataFrame(instruments))
    if frame.empty:
        return pd.DataFrame(columns=["maturity_years", "zero_rate", "discount_factor", "instrument_type"])

    results: list[dict] = []
    discounts: dict[float, float] = {}
    for row in frame.itertuples(index=False):
        maturity = float(row.maturity_years)
        rate = float(row.rate)
        instrument_type = str(row.instrument_type).lower()
        if maturity <= 0:
            continue
        if instrument_type == "zero":
            discount = 1.0 / (1.0 + rate * maturity)
        elif instrument_type == "swap":
            coupon = rate
            prior = sorted(term for term in discounts if term < maturity)
            accrual = 0.0
            previous = 0.0
            for tenor in prior:
                accrual += (tenor - previous) * discounts[tenor]
                previous = tenor
            delta = maturity - previous if maturity > previous else 1.0
            denominator = 1.0 + coupon * delta
            if denominator <= 0:
                raise ValueError("invalid swap denominator while bootstrapping curve")
            discount = max((1.0 - coupon * accrual) / denominator, 1e-12)
        else:
            raise ValueError(f"unsupported instrument_type: {instrument_type}")
        zero_rate = max(-math.log(discount) / maturity, 0.0)
        discounts[maturity] = discount
        results.append(
            {
                "maturity_years": maturity,
                "zero_rate": zero_rate,
                "discount_factor": discount,
                "instrument_type": instrument_type,
            }
        )
    return pd.DataFrame(results).sort_values("maturity_years")