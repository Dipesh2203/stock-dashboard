from __future__ import annotations

from datetime import date

from pydantic import BaseModel


class StockDataPoint(BaseModel):
    date: date
    symbol: str
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: float | None = None
    daily_return: float | None = None
    cumulative_return: float | None = None
    log_return: float | None = None
    ma7: float | None = None
    rolling_52w_high: float | None = None
    rolling_52w_low: float | None = None
    volatility_14d: float | None = None


class SummaryResponse(BaseModel):
    symbol: str
    latest_close: float
    average_close_52w: float
    high_52w: float
    low_52w: float
    average_daily_return_30d: float
    latest_volatility_14d: float | None = None


class CompareResponse(BaseModel):
    symbol1: str
    symbol2: str
    correlation_close_30d: float
    symbol1_return_30d: float
    symbol2_return_30d: float
    winner: str
