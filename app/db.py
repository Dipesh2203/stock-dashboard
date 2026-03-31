from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "stock_data.db"


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    daily_return REAL,
    cumulative_return REAL,
    log_return REAL,
    ma7 REAL,
    rolling_52w_high REAL,
    rolling_52w_low REAL,
    volatility_14d REAL,
    UNIQUE(date, symbol)
)
"""


INDEXES_SQL: Iterable[str] = [
    "CREATE INDEX IF NOT EXISTS idx_stock_symbol ON stock_prices(symbol)",
    "CREATE INDEX IF NOT EXISTS idx_stock_date ON stock_prices(date)",
]


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def initialize_database() -> None:
    with get_connection() as connection:
        connection.execute(CREATE_TABLE_SQL)
        for statement in INDEXES_SQL:
            connection.execute(statement)

        # Keep older DB files compatible when new derived metrics are added.
        existing_columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(stock_prices)").fetchall()
        }
        required_columns = {
            "cumulative_return": "REAL",
            "log_return": "REAL",
        }
        for column_name, column_type in required_columns.items():
            if column_name not in existing_columns:
                connection.execute(
                    f"ALTER TABLE stock_prices ADD COLUMN {column_name} {column_type}"
                )

        connection.commit()


def upsert_stock_data(df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    expected_columns = [
        "date",
        "symbol",
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
    records = df[expected_columns].to_dict(orient="records")

    upsert_sql = """
    INSERT INTO stock_prices (
        date, symbol, open, high, low, close, volume,
        daily_return, cumulative_return, log_return, ma7,
        rolling_52w_high, rolling_52w_low, volatility_14d
    ) VALUES (
        :date, :symbol, :open, :high, :low, :close, :volume,
        :daily_return, :cumulative_return, :log_return, :ma7,
        :rolling_52w_high, :rolling_52w_low, :volatility_14d
    )
    ON CONFLICT(date, symbol) DO UPDATE SET
        open = excluded.open,
        high = excluded.high,
        low = excluded.low,
        close = excluded.close,
        volume = excluded.volume,
        daily_return = excluded.daily_return,
        cumulative_return = excluded.cumulative_return,
        log_return = excluded.log_return,
        ma7 = excluded.ma7,
        rolling_52w_high = excluded.rolling_52w_high,
        rolling_52w_low = excluded.rolling_52w_low,
        volatility_14d = excluded.volatility_14d
    """

    with get_connection() as connection:
        connection.executemany(upsert_sql, records)
        connection.commit()

    return len(records)


def read_query(sql: str, params: tuple | None = None) -> pd.DataFrame:
    with get_connection() as connection:
        return pd.read_sql_query(sql, connection, params=params or ())
