"""메인 실행 - 백테스트 및 페이퍼 트레이딩"""
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from .config import SystemConfig
from .data.collector import MarketDataCollector
from .data.store import DataStore
from .backtest import Backtester


def run_backtest():
    """5년치 데이터로 백테스트 실행"""
    config = SystemConfig()
    collector = MarketDataCollector()

    # logs 디렉토리를 auto/ 하위로
    logs_dir = Path(__file__).parent / "logs"
    logs_dir.mkdir(exist_ok=True)
    config.db_path = str(logs_dir / "trades.db")

    store = DataStore(config.db_path)
    cache_dir = str(logs_dir / "cache")

    end_date = datetime.now().strftime("%Y%m%d")
    start_date = config.data_start_date.replace("-", "")

    print(f"=== 데이터 수집 시작 ({start_date} ~ {end_date}) ===")

    # 코스피 지수 프록시 (KODEX 200 ETF)
    print("코스피 지수(KODEX 200) 수집 중...")
    market_cached = store.load_ohlcv_cache("MARKET_069500", cache_dir)
    if market_cached is not None and len(market_cached) > 100:
        market_df = market_cached
        print(f"코스피 지수: {len(market_df)}일치 (캐시)")
    else:
        market_df = collector.get_market_index("069500", start_date, end_date)
        store.save_ohlcv_cache("MARKET_069500", market_df, cache_dir)
        print(f"코스피 지수: {len(market_df)}일치 데이터")

    # 코스피200 종목 데이터
    print("종목 데이터 수집 중... (시간이 걸립니다)")
    tickers = collector.get_kospi200_tickers()
    print(f"유니버스: {len(tickers)}종목")

    universe_data = {}
    for i, ticker in enumerate(tickers):
        # 캐시 확인
        cached = store.load_ohlcv_cache(ticker, cache_dir)
        if cached is not None and len(cached) > 100:
            universe_data[ticker] = cached
        else:
            try:
                df = collector.get_ohlcv(ticker, start_date, end_date)
                if not df.empty:
                    universe_data[ticker] = df
                    store.save_ohlcv_cache(ticker, df, cache_dir)
                time.sleep(0.3)  # KRX API 부하 방지
            except Exception as e:
                print(f"  [{ticker}] 실패: {e}")

        if (i + 1) % 10 == 0:
            print(f"  {i + 1}/{len(tickers)} 종목 완료")

    print(f"수집 완료: {len(universe_data)}종목")

    # 백테스트 실행
    print("\n=== 백테스트 실행 ===")
    backtester = Backtester(config)
    result = backtester.run(universe_data, market_df)

    print(result.summary())

    # 결과 저장
    import pandas as pd
    pd.DataFrame(result.daily_values).to_csv(logs_dir / "backtest_daily.csv", index=False)
    pd.DataFrame(result.trades).to_csv(logs_dir / "backtest_trades.csv", index=False)
    print(f"결과 저장: {logs_dir}/backtest_daily.csv, {logs_dir}/backtest_trades.csv")

    return result


if __name__ == "__main__":
    run_backtest()
