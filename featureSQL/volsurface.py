"""Volatility surface helpers built from Yahoo option-chain snapshots."""

from __future__ import annotations

import math

import pandas as pd


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def black_scholes_price(
    spot: float,
    strike: float,
    expiry_years: float,
    rate: float,
    sigma: float,
    option_type: str,
) -> float:
    if expiry_years <= 0 or sigma <= 0 or spot <= 0 or strike <= 0:
        intrinsic = max(spot - strike, 0.0) if option_type == "call" else max(strike - spot, 0.0)
        return intrinsic
    d1 = (math.log(spot / strike) + (rate + 0.5 * sigma * sigma) * expiry_years) / (sigma * math.sqrt(expiry_years))
    d2 = d1 - sigma * math.sqrt(expiry_years)
    if option_type == "put":
        return strike * math.exp(-rate * expiry_years) * _normal_cdf(-d2) - spot * _normal_cdf(-d1)
    return spot * _normal_cdf(d1) - strike * math.exp(-rate * expiry_years) * _normal_cdf(d2)


def implied_volatility(
    price: float,
    spot: float,
    strike: float,
    expiry_years: float,
    rate: float,
    option_type: str,
    low: float = 1e-4,
    high: float = 5.0,
    tolerance: float = 1e-6,
    max_iter: int = 120,
) -> float:
    target = max(float(price), 0.0)
    if target == 0 or expiry_years <= 0:
        return 0.0
    left = low
    right = high
    for _ in range(max_iter):
        mid = 0.5 * (left + right)
        mid_price = black_scholes_price(spot, strike, expiry_years, rate, mid, option_type)
        if abs(mid_price - target) <= tolerance:
            return mid
        if mid_price > target:
            right = mid
        else:
            left = mid
    return 0.5 * (left + right)


def calibrate_vol_surface(
    option_chain: pd.DataFrame,
    spot: float,
    rate: float = 0.0,
    valuation_date: str | pd.Timestamp | None = None,
) -> pd.DataFrame:
    if option_chain.empty:
        return option_chain.copy()
    frame = option_chain.copy()
    valuation_ts = pd.Timestamp(valuation_date) if valuation_date is not None else None
    if valuation_ts is not None:
        valuation_ts = pd.to_datetime(valuation_ts, utc=True)
    if valuation_ts is None:
        if "snapshot_at" in frame.columns:
            valuation_ts = pd.to_datetime(frame["snapshot_at"], errors="coerce", utc=True).dropna().min()
        else:
            valuation_ts = pd.Timestamp.now(tz="UTC")
    if "expiration" not in frame.columns and "expirationDate" in frame.columns:
        frame["expiration"] = frame["expirationDate"]
    frame["expiration"] = pd.to_datetime(frame["expiration"], errors="coerce", utc=True)
    frame["strike"] = pd.to_numeric(frame["strike"], errors="coerce")
    price_col = None
    for candidate in ["mid_price", "lastPrice", "mark", "price"]:
        if candidate in frame.columns:
            price_col = candidate
            break
    if price_col is None and {"bid", "ask"}.issubset(frame.columns):
        frame["mid_price"] = (pd.to_numeric(frame["bid"], errors="coerce") + pd.to_numeric(frame["ask"], errors="coerce")) / 2.0
        price_col = "mid_price"
    if price_col is None:
        raise ValueError("option chain requires price columns such as lastPrice or bid/ask")
    frame[price_col] = pd.to_numeric(frame[price_col], errors="coerce")
    if "optionType" not in frame.columns:
        frame["optionType"] = frame.get("type", "call")
    frame["optionType"] = frame["optionType"].astype(str).str.lower().map(lambda value: "put" if value.startswith("p") else "call")
    frame = frame.dropna(subset=["expiration", "strike", price_col]).copy()
    if frame.empty:
        return frame
    frame["expiry_years"] = (frame["expiration"] - valuation_ts).dt.total_seconds() / (365.25 * 24 * 3600)
    frame = frame[frame["expiry_years"] > 0].copy()
    if frame.empty:
        return frame
    if "impliedVolatility" in frame.columns:
        frame["yahoo_implied_vol"] = pd.to_numeric(frame["impliedVolatility"], errors="coerce")
    frame["implied_vol"] = frame.apply(
        lambda row: implied_volatility(
            price=row[price_col],
            spot=spot,
            strike=row["strike"],
            expiry_years=row["expiry_years"],
            rate=rate,
            option_type=row["optionType"],
        ),
        axis=1,
    )
    frame["moneyness"] = frame["strike"] / float(spot)
    frame["total_variance"] = (frame["implied_vol"] ** 2) * frame["expiry_years"]
    return frame.sort_values(["expiration", "strike", "optionType"]).reset_index(drop=True)