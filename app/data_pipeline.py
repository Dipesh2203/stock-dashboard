from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable

import numpy as np
import pandas as pd
import yfinance as yf


@dataclass(frozen=True)
class PipelineConfig:
    symbols: Iterable[str]
    lookback_days: int = 400


DEFAULT_SYMBOLS = [
    "TCS.NS",
    "INFY.NS",
    "RELIANCE.NS",
    "HDFCBANK.NS",
    "ICICIBANK.NS",
    "SBIN.NS",
    "LT.NS",
    "WIPRO.NS",
    "ITC.NS",
    "HINDUNILVR.NS",
]


def normalize_symbol(symbol: str) -> str:
    if symbol.endswith(".NS"):
        return symbol.replace(".NS", "")
    if symbol.endswith(".BO"):
        return symbol.replace(".BO", "")
    return symbol


def _coerce_numeric_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for col in columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        out = df.copy()
        out.columns = [col[0] if isinstance(col, tuple) else col for col in out.columns]
        return out
    return df


def _prepare_single_symbol(raw_df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if raw_df.empty:
        return raw_df

    df = _flatten_columns(raw_df).reset_index().copy()
    df = _flatten_columns(df)

    if "Date" not in df.columns and "date" in df.columns:
        df = df.rename(columns={"date": "Date"})

    if "Date" not in df.columns:
        return pd.DataFrame()

    keep_cols = ["Date", "Open", "High", "Low", "Close", "Volume"]
    df = df[keep_cols]
    df = df.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = _coerce_numeric_columns(df, ["open", "high", "low", "close", "volume"])

    # Drop malformed rows before metric calculations.
    df = df.dropna(subset=["date", "open", "close"])
    df = df.sort_values("date").reset_index(drop=True)

    df["symbol"] = normalize_symbol(symbol)
    df["daily_return"] = np.where(df["open"] != 0, (df["close"] - df["open"]) / df["open"], np.nan)
    close_returns = df["close"].pct_change()
    df["cumulative_return"] = (1 + close_returns.fillna(0.0)).cumprod() - 1
    df["log_return"] = np.log(df["close"] / df["close"].shift(1))
    df["ma7"] = df["close"].rolling(window=7, min_periods=1).mean()
    df["rolling_52w_high"] = df["close"].rolling(window=252, min_periods=1).max()
    df["rolling_52w_low"] = df["close"].rolling(window=252, min_periods=1).min()

    # Custom metric: annualized historical volatility from 14-day returns.
    df["volatility_14d"] = close_returns.rolling(window=14, min_periods=5).std() * np.sqrt(252)

    return df


def download_and_transform(config: PipelineConfig | None = None) -> pd.DataFrame:
    cfg = config or PipelineConfig(symbols=DEFAULT_SYMBOLS)
    end_date = date.today()
    start_date = end_date - timedelta(days=cfg.lookback_days)

    prepared_frames: list[pd.DataFrame] = []
    for ticker in cfg.symbols:
        raw = yf.download(
            ticker,
            start=start_date.isoformat(),
            end=(end_date + timedelta(days=1)).isoformat(),
            interval="1d",
            auto_adjust=False,
            progress=False,
        )
        prepared = _prepare_single_symbol(raw, ticker)
        if not prepared.empty:
            prepared_frames.append(prepared)

    if not prepared_frames:
        return pd.DataFrame()

    combined = pd.concat(prepared_frames, ignore_index=True)
    combined["date"] = combined["date"].astype(str)

    numeric_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "daily_return",
        "cumulative_return",
        "log_return",
        "ma7",
        "rolling_52w_high",
        "rolling_52w_low",
        "volatility_14d",
    ]
    combined[numeric_cols] = combined[numeric_cols].replace([np.inf, -np.inf], np.nan)

    return combined


def build_mock_data(config: PipelineConfig | None = None) -> pd.DataFrame:
    cfg = config or PipelineConfig(symbols=DEFAULT_SYMBOLS)
    num_days = 300
    dates = pd.date_range(end=pd.Timestamp.today().normalize(), periods=num_days, freq="B")
    records: list[dict] = []

    rng = np.random.default_rng(seed=42)
    for ticker in cfg.symbols:
        symbol = normalize_symbol(ticker)
        price = rng.uniform(200, 2500)
        for dt in dates:
            shock = rng.normal(0, 0.012)
            open_price = price * (1 + rng.normal(0, 0.004))
            close_price = open_price * (1 + shock)
            high_price = max(open_price, close_price) * (1 + abs(rng.normal(0, 0.003)))
            low_price = min(open_price, close_price) * (1 - abs(rng.normal(0, 0.003)))
            volume = float(rng.integers(150_000, 2_000_000))
            records.append(
                {
                    "date": dt.date().isoformat(),
                    "symbol": symbol,
                    "open": float(open_price),
                    "high": float(high_price),
                    "low": float(low_price),
                    "close": float(close_price),
                    "volume": volume,
                }
            )
            price = close_price

    df = pd.DataFrame.from_records(records)
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
    grouped = df.groupby("symbol", group_keys=False)
    df["daily_return"] = grouped.apply(lambda x: (x["close"] - x["open"]) / x["open"]).reset_index(drop=True)
    close_returns = grouped["close"].transform(lambda s: s.pct_change())
    df["cumulative_return"] = (1 + close_returns.fillna(0.0)).groupby(df["symbol"]).cumprod() - 1
    df["log_return"] = grouped["close"].transform(lambda s: np.log(s / s.shift(1)))
    df["ma7"] = grouped["close"].transform(lambda s: s.rolling(7, min_periods=1).mean())
    df["rolling_52w_high"] = grouped["close"].transform(lambda s: s.rolling(252, min_periods=1).max())
    df["rolling_52w_low"] = grouped["close"].transform(lambda s: s.rolling(252, min_periods=1).min())
    df["volatility_14d"] = grouped["close"].transform(lambda s: s.pct_change().rolling(14, min_periods=5).std() * np.sqrt(252))
    return df
