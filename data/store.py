"""데이터 저장/로드 (SQLite + CSV 캐시)"""
import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional


class DataStore:
    """거래 기록 및 시장 데이터 저장소"""

    def __init__(self, db_path: str = "logs/trades.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    side TEXT NOT NULL,
                    price REAL NOT NULL,
                    quantity INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    commission REAL NOT NULL,
                    tax REAL NOT NULL,
                    slippage REAL NOT NULL,
                    pnl REAL DEFAULT 0,
                    strategy TEXT,
                    memo TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    total_value REAL NOT NULL,
                    cash REAL NOT NULL,
                    positions_value REAL NOT NULL,
                    daily_pnl REAL NOT NULL,
                    cumulative_pnl REAL NOT NULL,
                    drawdown REAL NOT NULL
                )
            """)

    def save_trade(self, trade: dict):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """INSERT INTO trades
                   (timestamp, ticker, side, price, quantity, amount, commission, tax, slippage, pnl, strategy, memo)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    trade["timestamp"], trade["ticker"], trade["side"],
                    trade["price"], trade["quantity"], trade["amount"],
                    trade["commission"], trade["tax"], trade["slippage"],
                    trade.get("pnl", 0), trade.get("strategy", ""),
                    trade.get("memo", ""),
                ),
            )

    def save_snapshot(self, snapshot: dict):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """INSERT INTO portfolio_snapshots
                   (date, total_value, cash, positions_value, daily_pnl, cumulative_pnl, drawdown)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    snapshot["date"], snapshot["total_value"], snapshot["cash"],
                    snapshot["positions_value"], snapshot["daily_pnl"],
                    snapshot["cumulative_pnl"], snapshot["drawdown"],
                ),
            )

    def get_trades(self, start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
        query = "SELECT * FROM trades"
        params = []
        if start and end:
            query += " WHERE timestamp BETWEEN ? AND ?"
            params = [start, end]
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(query, conn, params=params)

    def get_snapshots(self) -> pd.DataFrame:
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query("SELECT * FROM portfolio_snapshots ORDER BY date", conn)

    def save_ohlcv_cache(self, ticker: str, df: pd.DataFrame, cache_dir: str = "logs/cache"):
        """OHLCV 데이터를 CSV로 캐싱"""
        path = Path(cache_dir)
        path.mkdir(parents=True, exist_ok=True)
        df.to_csv(path / f"{ticker}.csv")

    def load_ohlcv_cache(self, ticker: str, cache_dir: str = "logs/cache") -> Optional[pd.DataFrame]:
        fpath = Path(cache_dir) / f"{ticker}.csv"
        if fpath.exists():
            return pd.read_csv(fpath, index_col=0, parse_dates=True)
        return None
