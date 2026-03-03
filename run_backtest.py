#!/usr/bin/env python3
"""백테스트 실행 스크립트 (DB에서 데이터 로드)"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pathlib import Path
import pandas as pd

from auto.config import SystemConfig
from auto.data.collector import MarketDataCollector
from auto.backtest import Backtester


def main():
    config = SystemConfig()
    logs_dir = Path(__file__).parent / "logs"
    config.db_path = str(logs_dir / "trades_backtest.db")

    print("=== 변경된 config ===")
    print(f"  momentum_lookback: {config.strategy.momentum_lookback}")
    print(f"  momentum_volume_mult: {config.strategy.momentum_volume_mult}")
    print(f"  momentum_atr_mult: {config.strategy.momentum_atr_mult}")
    print(f"  per_stock_loss_pct: {config.risk.per_stock_loss_pct}")
    print(f"  max_position_pct: {config.risk.max_position_pct}")
    print(f"  max_positions: {config.risk.max_positions}")
    print()

    # DB에서 데이터 로드 (이미 수집됨)
    collector = MarketDataCollector(str(logs_dir / "market.db"))
    start, end = "20210303", "20260227"

    print("데이터 로딩 (market.db)...")
    market_df = collector.load_from_db("069500", start, end)
    print(f"  마켓 인덱스: {len(market_df)}일")

    universe = collector.load_all_from_db(start, end)
    print(f"  유니버스: {len(universe)}종목")
    print()

    # 백테스트 실행
    print("=== 백테스트 실행 ===")
    backtester = Backtester(config)
    result = backtester.run(universe, market_df)

    print(result.summary())

    # 월별 수익률
    daily = pd.DataFrame(result.daily_values)
    daily["date"] = pd.to_datetime(daily["date"])
    daily["month"] = daily["date"].dt.to_period("M")

    monthly = daily.groupby("month").agg(
        start_val=("total_value", "first"),
        end_val=("total_value", "last"),
        min_val=("total_value", "min"),
    ).reset_index()
    monthly["return_pct"] = (monthly["end_val"] / monthly["start_val"] - 1) * 100
    monthly["dd_pct"] = (1 - monthly["min_val"] / monthly["start_val"]) * 100

    print("=== 월별 수익률 ===")
    for _, row in monthly.iterrows():
        print(f"  {row['month']} | {row['return_pct']:+6.2f}% | 자산 {row['end_val']:>12,.0f} | DD {row['dd_pct']:>5.2f}%")

    # CSV 저장
    pd.DataFrame(result.daily_values).to_csv(logs_dir / "backtest_daily.csv", index=False)
    pd.DataFrame(result.trades).to_csv(logs_dir / "backtest_trades.csv", index=False)
    print(f"\n결과 저장: backtest_daily.csv, backtest_trades.csv")


if __name__ == "__main__":
    main()
