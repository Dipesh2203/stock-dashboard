from __future__ import annotations

from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse

from app.data_pipeline import build_mock_data, download_and_transform, normalize_symbol
from app.db import initialize_database, read_query, upsert_stock_data
from app.schemas import CompareResponse, StockDataPoint, SummaryResponse

BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "app" / "static"

app = FastAPI(
    title="Stock Data Intelligence Dashboard API",
    description="Mini financial intelligence platform with stock analytics endpoints.",
    version="1.0.0",
    contact={
        "name": "Dipesh Kumar",
        "email": "dipesh.shs11@gmail.com",
    },
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def refresh_database() -> dict:
    initialize_database()
    dataset = download_and_transform()
    source = "yfinance"
    if dataset.empty:
        dataset = build_mock_data()
        source = "mock"
    rows = upsert_stock_data(dataset)
    return {"rows": rows, "source": source}


@app.on_event("startup")
def startup_event() -> None:
    refresh_database()


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/dashboard")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/dashboard", include_in_schema=False)
def dashboard() -> FileResponse:
    index_file = STATIC_DIR / "index.html"
    if not index_file.exists():
        raise HTTPException(status_code=404, detail="Dashboard file not found")
    return FileResponse(index_file)


@app.post("/refresh")
def refresh_data() -> dict:
    return refresh_database()


@app.get("/companies")
def companies() -> dict:
    query = """
    SELECT DISTINCT symbol
    FROM stock_prices
    ORDER BY symbol
    """
    df = read_query(query)
    return {"companies": df["symbol"].tolist()}


@app.get("/data/{symbol}", response_model=list[StockDataPoint])
def stock_data(symbol: str, days: int = Query(default=30, ge=5, le=365)) -> list[StockDataPoint]:
    cleaned_symbol = normalize_symbol(symbol.upper())
    query = """
    SELECT date, symbol, open, high, low, close, volume,
            daily_return, cumulative_return, log_return,
            ma7, rolling_52w_high, rolling_52w_low, volatility_14d
    FROM stock_prices
    WHERE symbol = ?
    ORDER BY date DESC
    LIMIT ?
    """
    df = read_query(query, (cleaned_symbol, days))
    if df.empty:
        raise HTTPException(status_code=404, detail=f"No data found for symbol {cleaned_symbol}")

    df = df.sort_values("date")
    return [StockDataPoint(**record) for record in df.to_dict(orient="records")]


@app.get("/summary/{symbol}", response_model=SummaryResponse)
def summary(symbol: str) -> SummaryResponse:
    cleaned_symbol = normalize_symbol(symbol.upper())

    latest_query = """
    SELECT close, volatility_14d
    FROM stock_prices
    WHERE symbol = ?
    ORDER BY date DESC
    LIMIT 1
    """
    stats_query = """
    SELECT AVG(close) AS average_close_52w,
           MAX(close) AS high_52w,
           MIN(close) AS low_52w
    FROM (
        SELECT close
        FROM stock_prices
        WHERE symbol = ?
        ORDER BY date DESC
        LIMIT 252
    )
    """
    ret_query = """
    SELECT AVG(daily_return) AS average_daily_return_30d
    FROM (
        SELECT daily_return
        FROM stock_prices
        WHERE symbol = ?
        ORDER BY date DESC
        LIMIT 30
    )
    """

    latest_df = read_query(latest_query, (cleaned_symbol,))
    stats_df = read_query(stats_query, (cleaned_symbol,))
    ret_df = read_query(ret_query, (cleaned_symbol,))

    if latest_df.empty or stats_df.empty:
        raise HTTPException(status_code=404, detail=f"No summary available for symbol {cleaned_symbol}")

    payload = {
        "symbol": cleaned_symbol,
        "latest_close": float(latest_df.iloc[0]["close"]),
        "average_close_52w": float(stats_df.iloc[0]["average_close_52w"] or 0.0),
        "high_52w": float(stats_df.iloc[0]["high_52w"] or 0.0),
        "low_52w": float(stats_df.iloc[0]["low_52w"] or 0.0),
        "average_daily_return_30d": float(ret_df.iloc[0]["average_daily_return_30d"] or 0.0),
        "latest_volatility_14d": (
            float(latest_df.iloc[0]["volatility_14d"])
            if pd.notna(latest_df.iloc[0]["volatility_14d"])
            else None
        ),
    }
    return SummaryResponse(**payload)


@app.get("/compare", response_model=CompareResponse)
def compare(
    symbol1: str = Query(..., min_length=1),
    symbol2: str = Query(..., min_length=1),
) -> CompareResponse:
    s1 = normalize_symbol(symbol1.upper())
    s2 = normalize_symbol(symbol2.upper())

    if s1 == s2:
        raise HTTPException(status_code=400, detail="Please choose two different symbols")

    query = """
    SELECT date, symbol, close
    FROM stock_prices
    WHERE symbol IN (?, ?)
    ORDER BY date DESC
    LIMIT 120
    """
    df = read_query(query, (s1, s2))
    if df.empty:
        raise HTTPException(status_code=404, detail="No data available for comparison")

    pivot = (
        df.assign(date=pd.to_datetime(df["date"]))
        .pivot_table(index="date", columns="symbol", values="close", aggfunc="last")
        .sort_index()
        .tail(30)
    )

    if s1 not in pivot.columns or s2 not in pivot.columns:
        raise HTTPException(status_code=404, detail="One or both symbols are missing recent data")

    pivot = pivot[[s1, s2]].dropna()
    if len(pivot) < 5:
        raise HTTPException(status_code=400, detail="Not enough overlapping data to compare")

    correlation = float(pivot[s1].corr(pivot[s2]))
    s1_return = float((pivot[s1].iloc[-1] - pivot[s1].iloc[0]) / pivot[s1].iloc[0])
    s2_return = float((pivot[s2].iloc[-1] - pivot[s2].iloc[0]) / pivot[s2].iloc[0])

    winner = s1 if s1_return > s2_return else s2

    return CompareResponse(
        symbol1=s1,
        symbol2=s2,
        correlation_close_30d=correlation,
        symbol1_return_30d=s1_return,
        symbol2_return_30d=s2_return,
        winner=winner,
    )


@app.get("/top-movers")
def top_movers(limit: int = Query(default=5, ge=1, le=20)) -> dict:
    query = """
    WITH latest_rows AS (
        SELECT symbol, daily_return,
               ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
        FROM stock_prices
    )
    SELECT symbol, daily_return
    FROM latest_rows
    WHERE rn = 1 AND daily_return IS NOT NULL
    ORDER BY daily_return DESC
    """
    df = read_query(query)
    if df.empty:
        return {"gainers": [], "losers": []}

    gainers = df.head(limit).to_dict(orient="records")
    losers = df.tail(limit).iloc[::-1].to_dict(orient="records")
    return {"gainers": gainers, "losers": losers}
