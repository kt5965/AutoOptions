"""pykrx 기반 시장 데이터 수집 + 기술적 지표 계산 + SQLite 저장 + 스케줄러"""
import sqlite3
import logging
import time
import numpy as np
import pandas as pd
from pykrx import stock
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from apscheduler.schedulers.background import BackgroundScheduler

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 코스피200 시가총액 1000억+ 종목 (2025.02 기준)
# pykrx ticker_list / market_cap API가 불안정하므로 수동 관리
# 분기마다 업데이트 권장
# ──────────────────────────────────────────────
UNIVERSE = {
    "005930": "삼성전자",
    "000660": "SK하이닉스",
    "373220": "LG에너지솔루션",
    "207940": "삼성바이오로직스",
    "005380": "현대차",
    "006400": "삼성SDI",
    "051910": "LG화학",
    "000270": "기아",
    "035420": "NAVER",
    "055550": "신한지주",
    "105560": "KB금융",
    "035720": "카카오",
    "003670": "포스코홀딩스",
    "068270": "셀트리온",
    "096770": "SK이노베이션",
    "028260": "삼성물산",
    "012330": "현대모비스",
    "066570": "LG전자",
    "032830": "삼성생명",
    "003550": "LG",
    "034730": "SK",
    "015760": "한국전력",
    "086790": "하나금융지주",
    "316140": "우리금융지주",
    "018260": "삼성에스디에스",
    "017670": "SK텔레콤",
    "009150": "삼성전기",
    "033780": "KT&G",
    "010130": "고려아연",
    "030200": "KT",
    "034020": "두산에너빌리티",
    "011200": "HMM",
    "010950": "S-Oil",
    "000810": "삼성화재",
    "024110": "기업은행",
    "036570": "엔씨소프트",
    "259960": "크래프톤",
    "010140": "한화",
    "009540": "한국조선해양",
    "138040": "메리츠금융지주",
    "047050": "포스코인터내셔널",
    "011170": "롯데케미칼",
    "000720": "현대건설",
    "016360": "삼성증권",
    "088350": "한화생명",
    "090430": "아모레퍼시픽",
    "323410": "카카오뱅크",
    "352820": "하이브",
    "003490": "대한항공",
    "009830": "한화솔루션",
    "004020": "현대제철",
    "267260": "HD현대일렉트릭",
    "329180": "HD현대중공업",
    "042670": "HD현대인프라코어",
    "009240": "한샘",
    "010620": "HD현대미포",
    "361610": "SK아이이테크놀로지",
    "402340": "SK스퀘어",
    "003410": "쌍용C&E",
    "000100": "유한양행",
    "006800": "미래에셋증권",
    "097950": "CJ제일제당",
    "271560": "오리온",
    "051900": "LG생활건강",
    "004170": "신세계",
    "005490": "POSCO",
    "028050": "삼성엔지니어링",
    "011070": "LG이노텍",
    "000880": "한화",
    "034220": "LG디스플레이",
    "069500": "KODEX 200",  # 지수 프록시
}


class MarketDataCollector:
    """코스피200 대형주 일봉 수집 + 지표 계산 + SQLite 저장"""

    DB_SCHEMA = """
    CREATE TABLE IF NOT EXISTS ohlcv (
        date      TEXT    NOT NULL,
        ticker    TEXT    NOT NULL,
        name      TEXT,
        open      REAL,
        high      REAL,
        low       REAL,
        close     REAL,
        volume    INTEGER,
        ma5       REAL,
        ma20      REAL,
        ma60      REAL,
        rsi14     REAL,
        bb_upper  REAL,
        bb_mid    REAL,
        bb_lower  REAL,
        atr14     REAL,
        adx14     REAL,
        vol_ratio REAL,
        PRIMARY KEY (date, ticker)
    );
    """

    def __init__(self, db_path: str = "auto/logs/market.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        self._scheduler: Optional[BackgroundScheduler] = None

    # ─── DB ────────────────────────────────────
    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(self.DB_SCHEMA)

    def _get_last_date(self, ticker: str) -> Optional[str]:
        """해당 종목의 DB에 저장된 가장 최근 날짜"""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT MAX(date) FROM ohlcv WHERE ticker = ?", (ticker,)
            ).fetchone()
        return row[0] if row and row[0] else None

    # ─── 데이터 수집 ──────────────────────────────
    def fetch_ohlcv(self, ticker: str, start: str, end: str) -> pd.DataFrame:
        """pykrx로 개별 종목 일봉 OHLCV 가져오기 (YYYYMMDD 형식)"""
        df = stock.get_market_ohlcv_by_date(start, end, ticker)
        if df.empty:
            return df
        col_map = {
            "시가": "open", "고가": "high", "저가": "low",
            "종가": "close", "거래량": "volume",
        }
        df = df.rename(columns=col_map)
        cols = [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
        df = df[cols]
        df.index.name = "date"
        return df

    def fetch_live_prices(self, tickers: list[str]) -> dict:
        """장중 현재가 조회 (pykrx 당일 데이터)

        Returns: {ticker: {"open": .., "high": .., "low": .., "close": .., "volume": ..}}
        빈 dict이면 장 외 시간이거나 조회 실패
        """
        today = datetime.now().strftime("%Y%m%d")
        result = {}
        for ticker in tickers:
            try:
                df = stock.get_market_ohlcv_by_date(today, today, ticker)
                if df.empty:
                    continue
                col_map = {"시가": "open", "고가": "high", "저가": "low",
                           "종가": "close", "거래량": "volume"}
                df = df.rename(columns=col_map)
                row = df.iloc[-1]
                result[ticker] = {
                    "open": row.get("open", 0),
                    "high": row.get("high", 0),
                    "low": row.get("low", 0),
                    "close": row.get("close", 0),
                    "volume": row.get("volume", 0),
                }
            except Exception as e:
                logger.warning(f"[{ticker}] 실시간 조회 실패: {e}")
        return result

    def fetch_market_index(self, start: str, end: str) -> pd.DataFrame:
        """코스피 지수 프록시 (KODEX 200 ETF)"""
        return self.fetch_ohlcv("069500", start, end)

    # ─── 기술적 지표 ─────────────────────────────
    @staticmethod
    def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
        """OHLCV DataFrame에 기술적 지표 컬럼 추가

        MA5, MA20, MA60, RSI(14), 볼린저밴드(20,2σ), ATR(14), ADX(14), 거래량비율
        """
        c = df["close"]
        h = df["high"]
        l = df["low"]
        v = df["volume"].astype(float)

        # 이동평균
        df["ma5"] = c.rolling(5).mean()
        df["ma20"] = c.rolling(20).mean()
        df["ma60"] = c.rolling(60).mean()

        # RSI(14)
        delta = c.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)
        avg_gain = gain.ewm(alpha=1 / 14, min_periods=14).mean()
        avg_loss = loss.ewm(alpha=1 / 14, min_periods=14).mean()
        rs = avg_gain / avg_loss
        df["rsi14"] = 100 - (100 / (1 + rs))

        # 볼린저밴드(20, 2σ)
        df["bb_mid"] = c.rolling(20).mean()
        std20 = c.rolling(20).std()
        df["bb_upper"] = df["bb_mid"] + 2 * std20
        df["bb_lower"] = df["bb_mid"] - 2 * std20

        # ATR(14)
        prev_c = c.shift(1)
        tr = pd.concat([
            h - l,
            (h - prev_c).abs(),
            (l - prev_c).abs(),
        ], axis=1).max(axis=1)
        df["atr14"] = tr.ewm(span=14, adjust=False).mean()

        # ADX(14)
        plus_dm = h.diff()
        minus_dm = -l.diff()
        plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
        minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)
        atr14 = df["atr14"]
        plus_di = 100 * plus_dm.ewm(span=14, adjust=False).mean() / atr14
        minus_di = 100 * minus_dm.ewm(span=14, adjust=False).mean() / atr14
        di_sum = plus_di + minus_di
        dx = 100 * (plus_di - minus_di).abs() / di_sum.replace(0, np.nan)
        df["adx14"] = dx.ewm(span=14, adjust=False).mean()

        # 거래량비율 (당일 거래량 / 20일 평균 거래량)
        vol_ma20 = v.rolling(20).mean()
        df["vol_ratio"] = v / vol_ma20.replace(0, np.nan)

        return df

    # ─── SQLite 저장 (중복 스킵) ─────────────────
    def save_to_db(self, ticker: str, df: pd.DataFrame, name: str = ""):
        """지표가 포함된 DataFrame을 SQLite에 저장. 이미 있는 날짜는 스킵."""
        if df.empty:
            return 0

        indicator_cols = [
            "open", "high", "low", "close", "volume",
            "ma5", "ma20", "ma60", "rsi14",
            "bb_upper", "bb_mid", "bb_lower",
            "atr14", "adx14", "vol_ratio",
        ]

        rows = []
        for date_idx, row in df.iterrows():
            date_str = date_idx.strftime("%Y-%m-%d") if hasattr(date_idx, "strftime") else str(date_idx)
            vals = [date_str, ticker, name]
            for col in indicator_cols:
                v = row.get(col, None)
                vals.append(None if v is not None and (pd.isna(v)) else v)
            rows.append(tuple(vals))

        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """INSERT OR IGNORE INTO ohlcv
                   (date, ticker, name, open, high, low, close, volume,
                    ma5, ma20, ma60, rsi14, bb_upper, bb_mid, bb_lower,
                    atr14, adx14, vol_ratio)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                rows,
            )
        return len(rows)

    # ─── 메인: 전체 유니버스 수집 ─────────────────
    def collect_all(
        self,
        start: str = "20210303",
        end: Optional[str] = None,
        tickers: Optional[dict[str, str]] = None,
    ) -> dict[str, pd.DataFrame]:
        """전체 유니버스 일봉 + 지표 수집 → SQLite 저장

        이미 DB에 있는 날짜 이후부터만 수집 (증분 업데이트).
        Returns: {ticker: DataFrame}
        """
        if end is None:
            end = datetime.now().strftime("%Y%m%d")
        if tickers is None:
            tickers = UNIVERSE

        result = {}
        total = len(tickers)

        for i, (ticker, name) in enumerate(tickers.items()):
            try:
                # 증분 수집: DB에 마지막 날짜가 있으면 그 다음날부터
                last = self._get_last_date(ticker)
                if last:
                    fetch_start = (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y%m%d")
                    if fetch_start > end:
                        # 이미 최신 → DB에서 로드
                        result[ticker] = self.load_from_db(ticker, start, end)
                        continue
                else:
                    fetch_start = start

                # pykrx로 가져오기 (지표 계산에 여유 기간 필요)
                # 60일 이동평균 계산을 위해 80일 정도 앞에서부터 수집
                buffer_start = (datetime.strptime(fetch_start, "%Y%m%d") - timedelta(days=120)).strftime("%Y%m%d")
                raw = self.fetch_ohlcv(ticker, buffer_start, end)

                if raw.empty:
                    logger.warning(f"[{ticker}] {name} - 데이터 없음")
                    continue

                # 지표 계산 (전체 기간에 대해)
                with_indicators = self.compute_indicators(raw.copy())

                # 실제 저장은 fetch_start 이후만
                save_slice = with_indicators.loc[with_indicators.index >= pd.Timestamp(fetch_start)]
                saved = self.save_to_db(ticker, save_slice, name)

                # 리턴은 요청한 전체 범위
                result[ticker] = self.load_from_db(ticker, start, end)

                if saved > 0:
                    logger.info(f"[{i+1}/{total}] {ticker} {name}: {saved}일 저장")

            except Exception as e:
                logger.warning(f"[{ticker}] {name} 수집 실패: {e}")

            # KRX API 부하 방지
            if (i + 1) % 5 == 0:
                time.sleep(1)

            if (i + 1) % 10 == 0:
                print(f"  {i+1}/{total} 종목 완료")

        return result

    # ─── DB에서 로드 ──────────────────────────────
    def load_from_db(
        self,
        ticker: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        """SQLite에서 지표 포함 데이터 로드"""
        query = "SELECT * FROM ohlcv WHERE ticker = ?"
        params: list = [ticker]

        if start:
            s = f"{start[:4]}-{start[4:6]}-{start[6:8]}" if len(start) == 8 else start
            query += " AND date >= ?"
            params.append(s)
        if end:
            e = f"{end[:4]}-{end[4:6]}-{end[6:8]}" if len(end) == 8 else end
            query += " AND date <= ?"
            params.append(e)

        query += " ORDER BY date"

        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query(query, conn, params=params)

        if df.empty:
            return df

        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")
        return df

    def load_all_from_db(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
        tickers: Optional[list[str]] = None,
    ) -> dict[str, pd.DataFrame]:
        """전체 유니버스 DB 로드"""
        if tickers is None:
            tickers = list(UNIVERSE.keys())

        result = {}
        for ticker in tickers:
            df = self.load_from_db(ticker, start, end)
            if not df.empty:
                result[ticker] = df
        return result

    def get_db_stats(self) -> dict:
        """DB 현황 요약"""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT COUNT(*), COUNT(DISTINCT ticker), MIN(date), MAX(date) FROM ohlcv").fetchone()
        return {
            "total_rows": row[0],
            "tickers": row[1],
            "min_date": row[2],
            "max_date": row[3],
        }

    # ─── 스케줄러 ─────────────────────────────────
    def start_scheduler(self):
        """매일 16:00에 당일 데이터 자동 업데이트"""
        if self._scheduler is not None:
            logger.warning("스케줄러가 이미 실행 중입니다")
            return

        self._scheduler = BackgroundScheduler()
        self._scheduler.add_job(
            self._scheduled_update,
            trigger="cron",
            hour=16,
            minute=0,
            day_of_week="mon-fri",
            id="daily_update",
            name="장 마감 후 일봉 업데이트",
        )
        self._scheduler.start()
        logger.info("스케줄러 시작: 매일 16:00 (월-금) 자동 업데이트")

    def stop_scheduler(self):
        if self._scheduler:
            self._scheduler.shutdown()
            self._scheduler = None
            logger.info("스케줄러 중지")

    def _scheduled_update(self):
        """스케줄러가 호출하는 업데이트 함수"""
        today = datetime.now().strftime("%Y%m%d")
        logger.info(f"[스케줄러] {today} 데이터 업데이트 시작")
        try:
            result = self.collect_all(end=today)
            logger.info(f"[스케줄러] 업데이트 완료: {len(result)}종목")
        except Exception as e:
            logger.error(f"[스케줄러] 업데이트 실패: {e}")
