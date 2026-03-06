"""스케줄러 데몬 - 매일 자동으로 데이터 수집 + 리포트 생성 + 트레이딩 스케줄

systemd 서비스로 등록하여 재부팅 시에도 자동 실행.

스케줄:
  08:30  prescreening        - 프리스크리닝 (유니버스 스캔, 매수 후보 선별)
  09:05  start_monitoring    - 장 시작 후 모니터링 (시그널 체크 + 주문)
  */5분  intraday_monitor    - 장중 모니터링 (09:10~15:20, 손절/트레일링 체크)
  15:25  close_positions     - 장 마감 전 포지션 정리
  15:35  daily_settlement    - 일일 정산 (PnL 기록, 리포트, 데이터 수집)
  16:00  daily_update        - 데이터 수집 + 일일 리포트
  16:30  weekly_report       - 주간 리포트 (금요일)
"""
import os
import logging
import signal
import sys
import time
import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler

from .config import SystemConfig
from .data.collector import MarketDataCollector, UNIVERSE
from .data.store import DataStore
from .strategy.momentum import MomentumBreakout
from .execution.paper import PaperTradingEngine
from .execution.base import Order, OrderSide
from .report.generator import ReportGenerator
from .alert.telegram import TelegramAlert
from .alert.bot import TelegramBot

# 로그 설정
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "scheduler.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 런타임 상태 (스케줄러 프로세스 공유)
# ──────────────────────────────────────────────
STATE_FILE = LOG_DIR / "trading_state.json"


def _load_state() -> dict:
    """파일에서 트레이딩 상태 로드"""
    default = {
        "capital": 10_000_000,
        "cash": 10_000_000,
        "positions": {},       # {ticker: {qty, entry_price, entry_date, stop_loss, atr}}
        "screening": [],       # 프리스크리닝 결과 (당일)
        "daily_pnl": 0,
        "daily_trades": [],
    }
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
            # 누락 키 보정
            for k, v in default.items():
                if k not in state:
                    state[k] = v
            return state
        except Exception as e:
            logger.warning(f"상태 파일 로드 실패, 기본값 사용: {e}")
    return default


def _save_state(state: dict):
    """트레이딩 상태를 파일에 저장"""
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"상태 파일 저장 실패: {e}")


def _get_alert() -> TelegramAlert:
    """텔레그램 알림 인스턴스 (.env 또는 환경변수에서 자동 로드)"""
    return TelegramAlert()


# ══════════════════════════════════════════════
# 트레이딩 스케줄 작업 (4개)
# ══════════════════════════════════════════════

def job_prescreening():
    """08:30 프리스크리닝 - 장 시작 전 매수 후보 선별

    1) 어제까지의 데이터를 DB에서 로드
    2) 모멘텀 전략으로 전 종목 시그널 스캔
    3) 매수 후보 리스트를 상태 파일에 저장
    4) 텔레그램으로 스크리닝 결과 전송
    """
    logger.info("=== 08:30 프리스크리닝 시작 ===")
    alert = _get_alert()
    state = _load_state()

    try:
        config = SystemConfig()
        db_path = str(LOG_DIR / "market.db")
        collector = MarketDataCollector(db_path)
        strategy = MomentumBreakout(config.strategy)

        # 기존 포지션 복원
        for ticker, pos_info in state.get("positions", {}).items():
            strategy.on_new_position(
                ticker, pos_info["entry_price"], pos_info["entry_date"],
                pos_info["stop_loss"], pos_info.get("atr", 0),
            )

        # 유니버스 전체 스캔
        candidates = []
        today = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=120)).strftime("%Y%m%d")

        for ticker, name in UNIVERSE.items():
            if ticker == "069500":  # 지수 프록시 제외
                continue
            if ticker in state.get("positions", {}):
                continue  # 이미 보유 중

            df = collector.load_from_db(ticker, start_date, today)
            if df.empty or len(df) < 60:
                continue

            sig = strategy.generate_signal(df, ticker)
            if sig:
                # 손절가는 돌파 레벨 기준으로 재계산
                breakout = sig.breakout_level
                stop_by_pct = breakout * (1 - strategy.STOP_PCT)
                stop_by_atr = breakout - strategy.ATR_STOP_MULT * (df.iloc[-1].get("atr14", 0) or 0)
                stop_at_breakout = max(stop_by_pct, stop_by_atr)

                candidates.append({
                    "ticker": ticker,
                    "name": name,
                    "price": sig.price,
                    "breakout_level": breakout,
                    "stop_loss": stop_at_breakout,
                    "strength": sig.strength,
                    "reason": sig.reason,
                })

        # 강도순 정렬
        candidates.sort(key=lambda x: x["strength"], reverse=True)

        # 상태에 저장
        state["screening"] = candidates
        state["daily_pnl"] = 0        # 일일 PnL 초기화
        state["daily_trades"] = []    # 일일 거래 초기화
        _save_state(state)

        # 텔레그램 알림
        n_pos = len(state.get("positions", {}))
        msg = f"*🔍 08:30 프리스크리닝*\n"
        msg += f"스캔: {len(UNIVERSE) - 1}종목\n"
        msg += f"매수후보: {len(candidates)}종목\n"
        msg += f"보유중: {n_pos}종목\n"

        if candidates:
            msg += "\n*매수 후보 TOP 5:*\n"
            for c in candidates[:5]:
                msg += (f"• {c['name']}({c['ticker']}) "
                        f"{c['price']:,.0f}원 "
                        f"강도={c['strength']:.2f}\n")

        alert.send(msg)
        logger.info(f"프리스크리닝 완료: {len(candidates)}종목 후보")

    except Exception as e:
        logger.error(f"프리스크리닝 실패: {e}", exc_info=True)
        alert.send(f"*❌ 프리스크리닝 오류*\n{e}")


def job_start_monitoring():
    """09:05 장 시작 후 모니터링 - 시그널 기반 주문 실행

    1) 프리스크리닝 후보 확인
    2) 포지션 사이징 계산
    3) 페이퍼 주문 실행
    4) 보유 포지션 청산 조건 체크
    5) 텔레그램으로 주문 결과 전송
    """
    logger.info("=== 09:05 모니터링 시작 ===")
    alert = _get_alert()
    state = _load_state()
    config = SystemConfig()

    try:
        db_path = str(LOG_DIR / "market.db")
        collector = MarketDataCollector(db_path)
        strategy = MomentumBreakout(config.strategy)
        engine = PaperTradingEngine(config.fees)
        store = DataStore(str(LOG_DIR / "trades.db"))

        today = datetime.now().strftime("%Y%m%d")
        today_dash = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=120)).strftime("%Y%m%d")

        capital = state["capital"]
        cash = state["cash"]
        positions = state.get("positions", {})

        # ─── 1) 보유 포지션 청산 체크 (실시간 가격) ───
        held_tickers = list(positions.keys())
        live_prices = collector.fetch_live_prices(held_tickers) if held_tickers else {}
        logger.info(f"실시간 가격 조회: {len(live_prices)}/{len(held_tickers)}종목")

        for ticker in list(positions.keys()):
            pos_info = positions[ticker]

            # 실시간 가격 우선, 없으면 DB fallback
            if ticker in live_prices:
                lp = live_prices[ticker]
                cur_price = lp["close"]
                cur_high = lp["high"]
                logger.info(f"  {ticker}: 실시간 현재가={cur_price:,.0f}, 고가={cur_high:,.0f}")
            else:
                df = collector.load_from_db(ticker, start_date, today)
                if df.empty:
                    continue
                latest = df.iloc[-1]
                cur_price = latest["close"]
                cur_high = latest["high"]
                logger.warning(f"  {ticker}: DB fallback 사용 (실시간 조회 실패)")

            cur_atr = pos_info.get("atr", 0)  # 진입 시 ATR 사용

            # 포지션 복원 → 청산 체크
            strategy.on_new_position(
                ticker, pos_info["entry_price"], pos_info["entry_date"],
                pos_info["stop_loss"], pos_info.get("atr", 0),
            )
            mp = strategy.positions[ticker]
            mp.holding_days = pos_info.get("holding_days", 0)
            mp.highest_since_entry = pos_info.get("highest", pos_info["entry_price"])
            mp.trailing_stop = pos_info.get("trailing_stop", pos_info["stop_loss"])

            exit_reason = strategy.check_position_exit(ticker, cur_price, cur_high, cur_atr)

            if exit_reason:
                # 매도 주문
                qty = pos_info["qty"]
                order = Order(ticker, OrderSide.SELL, cur_price, qty, "momentum", exit_reason)
                engine.set_current_prices({ticker: cur_price})
                result = engine.execute_order(order)

                if result.success:
                    pnl = (result.filled_price - pos_info["entry_price"]) * qty - result.total_cost
                    strategy.on_position_closed(ticker, result.filled_price, pnl)

                    trade = {
                        "timestamp": today_dash,
                        "ticker": ticker,
                        "side": "sell",
                        "price": result.filled_price,
                        "quantity": qty,
                        "amount": result.filled_price * qty,
                        "commission": result.commission,
                        "tax": result.tax,
                        "slippage": result.slippage,
                        "pnl": pnl,
                        "strategy": "momentum",
                        "memo": exit_reason,
                    }
                    store.save_trade(trade)
                    state["daily_trades"].append(trade)

                    cash += result.filled_price * qty - result.total_cost
                    del positions[ticker]

                    name = UNIVERSE.get(ticker, ticker)
                    alert.send_trade_alert({
                        "side": "sell", "ticker": f"{name}({ticker})",
                        "price": result.filled_price, "quantity": qty,
                        "amount": result.filled_price * qty,
                        "strategy": "momentum", "reason": exit_reason,
                    })
                    logger.info(f"[매도] {ticker} {qty}주 @ {result.filled_price:,.0f} PnL={pnl:+,.0f}")
            else:
                # 보유 유지 → 상태 갱신
                positions[ticker]["holding_days"] = mp.holding_days + 1
                positions[ticker]["highest"] = mp.highest_since_entry
                positions[ticker]["trailing_stop"] = mp.trailing_stop

        # 일일 손실 체크
        daily_pnl = sum(t.get("pnl", 0) for t in state.get("daily_trades", []))
        if daily_pnl <= -strategy.DAILY_LOSS_LIMIT:
            logger.warning(f"일일 손실 한도 도달: {daily_pnl:+,.0f}원 → 매수 중단")
            alert.send(f"*⚠️ 일일 손실 한도 도달*\n실현손익: {daily_pnl:+,.0f}원\n신규 매수 중단")
        else:
            # ─── 2) 신규 매수 (후보 중 상위) ───
            candidates = state.get("screening", [])
            n_positions = len(positions)

            for cand in candidates:
                if n_positions >= strategy.MAX_POSITIONS:
                    break
                if cand["ticker"] in positions:
                    continue

                ticker = cand["ticker"]
                price = cand["price"]
                stop_loss = cand["stop_loss"]

                qty = strategy.calculate_position_size(price, stop_loss, capital, cash)
                if qty <= 0:
                    continue

                order = Order(ticker, OrderSide.BUY, price, qty, "momentum", cand["reason"])
                engine.set_current_prices({ticker: price})
                result = engine.execute_order(order)

                if result.success:
                    cost = result.filled_price * qty + result.total_cost
                    if cost > cash:
                        continue

                    positions[ticker] = {
                        "qty": qty,
                        "entry_price": result.filled_price,
                        "entry_date": today_dash,
                        "stop_loss": stop_loss,
                        "atr": 0,
                        "holding_days": 0,
                        "highest": result.filled_price,
                        "trailing_stop": stop_loss,
                    }
                    # ATR 가져오기
                    df = collector.load_from_db(ticker, start_date, today)
                    if not df.empty:
                        positions[ticker]["atr"] = df.iloc[-1].get("atr14", 0) or 0

                    cash -= cost
                    n_positions += 1

                    trade = {
                        "timestamp": today_dash,
                        "ticker": ticker,
                        "side": "buy",
                        "price": result.filled_price,
                        "quantity": qty,
                        "amount": result.filled_price * qty,
                        "commission": result.commission,
                        "tax": result.tax,
                        "slippage": result.slippage,
                        "pnl": 0,
                        "strategy": "momentum",
                        "memo": cand["reason"],
                    }
                    store.save_trade(trade)
                    state["daily_trades"].append(trade)

                    name = UNIVERSE.get(ticker, ticker)
                    alert.send_trade_alert({
                        "side": "buy", "ticker": f"{name}({ticker})",
                        "price": result.filled_price, "quantity": qty,
                        "amount": result.filled_price * qty,
                        "strategy": "momentum", "reason": cand["reason"],
                    })
                    logger.info(f"[매수] {ticker} {qty}주 @ {result.filled_price:,.0f}")

        # 상태 저장
        state["cash"] = cash
        state["positions"] = positions
        state["daily_pnl"] = daily_pnl
        _save_state(state)

        msg = f"*📊 09:05 모니터링 완료*\n"
        msg += f"보유: {len(positions)}종목\n"
        msg += f"현금: {cash:,.0f}원\n"
        msg += f"당일 거래: {len(state['daily_trades'])}건\n"
        msg += f"당일 손익: {daily_pnl:+,.0f}원"
        alert.send(msg)

        logger.info(f"모니터링 완료: 보유 {len(positions)}종목, 현금 {cash:,.0f}원")

    except Exception as e:
        logger.error(f"모니터링 실패: {e}", exc_info=True)
        alert.send(f"*❌ 모니터링 오류*\n{e}")


def job_intraday_monitor():
    """5분 간격 장중 모니터링 - 보유 포지션 손절/트레일링 체크

    09:10~15:20 사이에만 실행. 보유 포지션이 없으면 즉시 리턴.
    """
    now = datetime.now()
    hour, minute = now.hour, now.minute
    # 장 시작 직후/마감 직전 제외 (09:05 매수, 15:25 청산과 충돌 방지)
    if hour == 9 and minute < 10:
        return
    if hour == 15 and minute > 20:
        return

    state = _load_state()
    positions = state.get("positions", {})
    if not positions:
        return  # 보유 종목 없으면 API 호출 안 함

    logger.info(f"=== 장중 모니터링 ({now.strftime('%H:%M')}) - {len(positions)}종목 ===")
    alert = _get_alert()
    config = SystemConfig()

    try:
        db_path = str(LOG_DIR / "market.db")
        collector = MarketDataCollector(db_path)
        strategy = MomentumBreakout(config.strategy)
        engine = PaperTradingEngine(config.fees)
        store = DataStore(str(LOG_DIR / "trades.db"))

        today_dash = now.strftime("%Y-%m-%d")
        capital = state["capital"]
        cash = state["cash"]

        # 실시간 가격 조회
        held_tickers = list(positions.keys())
        live_prices = collector.fetch_live_prices(held_tickers)
        if not live_prices:
            logger.warning("실시간 가격 조회 실패 - 스킵")
            return

        sold_any = False
        for ticker in list(positions.keys()):
            if ticker not in live_prices:
                continue

            pos_info = positions[ticker]
            lp = live_prices[ticker]
            cur_price = lp["close"]
            cur_high = lp["high"]
            cur_atr = pos_info.get("atr", 0)

            # 포지션 복원
            strategy.on_new_position(
                ticker, pos_info["entry_price"], pos_info["entry_date"],
                pos_info["stop_loss"], cur_atr,
            )
            mp = strategy.positions[ticker]
            mp.holding_days = pos_info.get("holding_days", 0)
            mp.highest_since_entry = pos_info.get("highest", pos_info["entry_price"])
            mp.trailing_stop = pos_info.get("trailing_stop", pos_info["stop_loss"])

            # 청산 체크 (holding_days 증가 없이 - update에서 +1 되므로 보정)
            mp.holding_days -= 1  # update()가 +1 하므로 미리 빼둠
            exit_reason = strategy.check_position_exit(ticker, cur_price, cur_high, cur_atr)

            if exit_reason:
                # 매도 실행
                qty = pos_info["qty"]
                order = Order(ticker, OrderSide.SELL, cur_price, qty, "momentum", exit_reason)
                engine.set_current_prices({ticker: cur_price})
                result = engine.execute_order(order)

                if result.success:
                    pnl = (result.filled_price - pos_info["entry_price"]) * qty - result.total_cost
                    strategy.on_position_closed(ticker, result.filled_price, pnl)

                    trade = {
                        "timestamp": today_dash, "ticker": ticker, "side": "sell",
                        "price": result.filled_price, "quantity": qty,
                        "amount": result.filled_price * qty,
                        "commission": result.commission, "tax": result.tax,
                        "slippage": result.slippage, "pnl": pnl,
                        "strategy": "momentum", "memo": f"장중모니터링: {exit_reason}",
                    }
                    store.save_trade(trade)
                    state["daily_trades"].append(trade)

                    cash += result.filled_price * qty - result.total_cost
                    del positions[ticker]
                    sold_any = True

                    name = UNIVERSE.get(ticker, ticker)
                    pnl_pct = (result.filled_price - pos_info["entry_price"]) / pos_info["entry_price"] * 100
                    alert.send(
                        f"*🔔 장중 청산*\n"
                        f"종목: {name} ({ticker})\n"
                        f"사유: {exit_reason}\n"
                        f"체결: {result.filled_price:,.0f}원 × {qty}주\n"
                        f"손익: {pnl:+,.0f}원 ({pnl_pct:+.1f}%)"
                    )
                    logger.info(f"[장중매도] {ticker} {qty}주 @ {result.filled_price:,.0f} PnL={pnl:+,.0f}")
            else:
                # 고가 갱신만 저장 (trailing stop 추적)
                positions[ticker]["highest"] = mp.highest_since_entry
                positions[ticker]["trailing_stop"] = mp.trailing_stop

        if sold_any:
            # 일일 손실 체크
            daily_pnl = sum(t.get("pnl", 0) for t in state.get("daily_trades", []))
            if daily_pnl <= -strategy.DAILY_LOSS_LIMIT:
                # 나머지 포지션도 전체 청산
                for ticker in list(positions.keys()):
                    if ticker not in live_prices:
                        continue
                    pos_info = positions[ticker]
                    cur_price = live_prices[ticker]["close"]
                    qty = pos_info["qty"]
                    order = Order(ticker, OrderSide.SELL, cur_price, qty, "momentum", "일일손실한도")
                    engine.set_current_prices({ticker: cur_price})
                    result = engine.execute_order(order)
                    if result.success:
                        pnl = (result.filled_price - pos_info["entry_price"]) * qty - result.total_cost
                        trade = {
                            "timestamp": today_dash, "ticker": ticker, "side": "sell",
                            "price": result.filled_price, "quantity": qty,
                            "amount": result.filled_price * qty,
                            "commission": result.commission, "tax": result.tax,
                            "slippage": result.slippage, "pnl": pnl,
                            "strategy": "momentum", "memo": "일일손실한도 초과 전체청산",
                        }
                        store.save_trade(trade)
                        state["daily_trades"].append(trade)
                        cash += result.filled_price * qty - result.total_cost
                        del positions[ticker]
                alert.send(f"*⚠️ 일일 손실 한도 초과*\n실현손익: {daily_pnl:+,.0f}원\n전체 포지션 청산")

        # ─── 장중 신규 매수 스크리닝 (여유 슬롯이 있을 때) ───
        daily_pnl = sum(t.get("pnl", 0) for t in state.get("daily_trades", []))
        n_positions = len(positions)

        if (n_positions < strategy.MAX_POSITIONS
                and daily_pnl > -strategy.DAILY_LOSS_LIMIT
                and now.hour < 15):  # 15시 이후는 신규 매수 안 함

            start_date = (now - timedelta(days=120)).strftime("%Y%m%d")
            today = now.strftime("%Y%m%d")
            today_dash = now.strftime("%Y-%m-%d")

            for ticker, name in UNIVERSE.items():
                if n_positions >= strategy.MAX_POSITIONS:
                    break
                if ticker == "069500" or ticker in positions:
                    continue

                df = collector.load_from_db(ticker, start_date, today)
                if df.empty or len(df) < 60:
                    continue

                # 장중 실시간 가격으로 당일 봉 업데이트
                lp = collector.fetch_live_prices([ticker]).get(ticker)
                if not lp:
                    continue

                # 당일 실시간 데이터를 마지막 행에 반영
                last_date = df.index[-1]
                today_dt = pd.Timestamp(now.strftime("%Y-%m-%d"))
                if last_date < today_dt:
                    # 오늘 데이터가 DB에 없으면 실시간으로 추가
                    new_row = pd.DataFrame({
                        "open": [lp["open"]], "high": [lp["high"]],
                        "low": [lp["low"]], "close": [lp["close"]],
                        "volume": [lp["volume"]],
                    }, index=[today_dt])
                    df = pd.concat([df, new_row])

                sig = strategy.generate_signal(df, ticker)
                if not sig:
                    continue

                breakout = sig.breakout_level
                stop_by_pct = breakout * (1 - strategy.STOP_PCT)
                stop_by_atr = breakout - strategy.ATR_STOP_MULT * (df.iloc[-1].get("atr14", 0) or 0)
                stop_loss = max(stop_by_pct, stop_by_atr)

                price = lp["close"]
                qty = strategy.calculate_position_size(price, stop_loss, capital, cash)
                if qty <= 0:
                    continue

                order = Order(ticker, OrderSide.BUY, price, qty, "momentum", sig.reason)
                engine.set_current_prices({ticker: price})
                result = engine.execute_order(order)

                if result.success:
                    cost = result.filled_price * qty + result.total_cost
                    if cost > cash:
                        continue

                    positions[ticker] = {
                        "qty": qty, "entry_price": result.filled_price,
                        "entry_date": now.strftime("%Y-%m-%d"),
                        "stop_loss": stop_loss, "atr": df.iloc[-1].get("atr14", 0) or 0,
                        "holding_days": 0, "highest": result.filled_price,
                        "trailing_stop": stop_loss,
                    }
                    cash -= cost
                    n_positions += 1

                    trade = {
                        "timestamp": now.strftime("%Y-%m-%d"), "ticker": ticker,
                        "side": "buy", "price": result.filled_price, "quantity": qty,
                        "amount": result.filled_price * qty,
                        "commission": result.commission, "tax": result.tax,
                        "slippage": result.slippage, "pnl": 0,
                        "strategy": "momentum", "memo": f"장중스크리닝: {sig.reason}",
                    }
                    store.save_trade(trade)
                    state["daily_trades"].append(trade)

                    alert.send(
                        f"*🔔 장중 매수*\n"
                        f"종목: {name} ({ticker})\n"
                        f"체결: {result.filled_price:,.0f}원 × {qty}주\n"
                        f"손절: {stop_loss:,.0f}원\n"
                        f"사유: {sig.reason[:60]}"
                    )
                    logger.info(f"[장중매수] {name}({ticker}) {qty}주 @ {result.filled_price:,.0f}")

        # 상태 저장
        state["cash"] = cash
        state["positions"] = positions
        _save_state(state)

    except Exception as e:
        logger.error(f"장중 모니터링 실패: {e}", exc_info=True)


def job_close_positions():
    """15:25 장 마감 전 포지션 정리

    - 시간 청산 대상 (15거래일 초과) 강제 청산
    - 일일 손실 한도 초과 시 전체 청산
    - 손절가 근접 종목 경고
    """
    logger.info("=== 15:25 포지션 정리 ===")
    alert = _get_alert()
    state = _load_state()
    config = SystemConfig()

    try:
        db_path = str(LOG_DIR / "market.db")
        collector = MarketDataCollector(db_path)
        engine = PaperTradingEngine(config.fees)
        store = DataStore(str(LOG_DIR / "trades.db"))

        today = datetime.now().strftime("%Y%m%d")
        today_dash = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=120)).strftime("%Y%m%d")

        positions = state.get("positions", {})
        cash = state["cash"]
        closed = []

        daily_pnl = sum(t.get("pnl", 0) for t in state.get("daily_trades", []))
        force_close_all = daily_pnl <= -50_000

        # 실시간 가격 조회
        held_tickers = list(positions.keys())
        live_prices = collector.fetch_live_prices(held_tickers) if held_tickers else {}
        logger.info(f"실시간 가격 조회 (장마감): {len(live_prices)}/{len(held_tickers)}종목")

        for ticker in list(positions.keys()):
            pos_info = positions[ticker]

            if ticker in live_prices:
                cur_price = live_prices[ticker]["close"]
            else:
                df = collector.load_from_db(ticker, start_date, today)
                if df.empty:
                    continue
                cur_price = df.iloc[-1]["close"]

            name = UNIVERSE.get(ticker, ticker)

            should_close = False
            reason = ""

            # 일일 손실 한도 → 전체 청산
            if force_close_all:
                should_close = True
                reason = f"일일손실한도({daily_pnl:+,.0f}원) 전체청산"

            # 손절가 이탈 → 즉시 청산
            stop_loss = pos_info.get("stop_loss", 0)
            trailing = pos_info.get("trailing_stop", 0)
            if not should_close and stop_loss > 0 and cur_price <= stop_loss:
                should_close = True
                loss_pct = (cur_price - pos_info["entry_price"]) / pos_info["entry_price"] * 100
                reason = f"손절({loss_pct:+.1f}%, 현재가 {cur_price:,.0f} < 손절가 {stop_loss:,.0f})"

            # 트레일링 스탑 이탈 → 청산
            if not should_close and trailing > 0 and cur_price <= trailing:
                should_close = True
                highest = pos_info.get("highest", pos_info["entry_price"])
                drop_pct = (highest - cur_price) / highest * 100
                reason = f"트레일링스탑(고점 {highest:,.0f} 대비 -{drop_pct:.1f}%)"

            # 15거래일 초과 → 시간 청산
            holding = pos_info.get("holding_days", 0)
            if holding >= 15 and not should_close:
                should_close = True
                pnl_pct = (cur_price - pos_info["entry_price"]) / pos_info["entry_price"] * 100
                reason = f"시간청산({holding}일 보유, {pnl_pct:+.1f}%)"

            if should_close:
                qty = pos_info["qty"]
                order = Order(ticker, OrderSide.SELL, cur_price, qty, "momentum", reason)
                engine.set_current_prices({ticker: cur_price})
                result = engine.execute_order(order)

                if result.success:
                    pnl = (result.filled_price - pos_info["entry_price"]) * qty - result.total_cost
                    cash += result.filled_price * qty - result.total_cost

                    trade = {
                        "timestamp": today_dash,
                        "ticker": ticker,
                        "side": "sell",
                        "price": result.filled_price,
                        "quantity": qty,
                        "amount": result.filled_price * qty,
                        "commission": result.commission,
                        "tax": result.tax,
                        "slippage": result.slippage,
                        "pnl": pnl,
                        "strategy": "momentum",
                        "memo": reason,
                    }
                    store.save_trade(trade)
                    state["daily_trades"].append(trade)
                    closed.append(f"{name}({ticker}) PnL={pnl:+,.0f}원 [{reason}]")
                    del positions[ticker]
                    logger.info(f"[장마감청산] {ticker} {qty}주 PnL={pnl:+,.0f}원 ({reason})")

        state["cash"] = cash
        state["positions"] = positions
        _save_state(state)

        msg = f"*🔔 15:25 장 마감 전 정리*\n"
        msg += f"청산: {len(closed)}건\n"
        if closed:
            for c in closed:
                msg += f"• {c}\n"
        msg += f"잔여 보유: {len(positions)}종목\n"
        msg += f"현금: {cash:,.0f}원"
        alert.send(msg)

        logger.info(f"장 마감 전 정리 완료: {len(closed)}건 청산")

    except Exception as e:
        logger.error(f"포지션 정리 실패: {e}", exc_info=True)
        alert.send(f"*❌ 포지션 정리 오류*\n{e}")


def job_daily_settlement():
    """15:35 일일 정산 - PnL 기록, 포트폴리오 스냅샷, 텔레그램 리포트

    1) 보유 포지션 평가 (종가 기준)
    2) 일일 실현+미실현 손익 계산
    3) portfolio_snapshots 테이블에 기록
    4) 텔레그램으로 일일 리포트 전송
    """
    logger.info("=== 15:35 일일 정산 시작 ===")
    alert = _get_alert()
    state = _load_state()

    try:
        db_path = str(LOG_DIR / "market.db")
        collector = MarketDataCollector(db_path)
        store = DataStore(str(LOG_DIR / "trades.db"))

        today = datetime.now().strftime("%Y%m%d")
        today_dash = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=120)).strftime("%Y%m%d")

        positions = state.get("positions", {})
        cash = state["cash"]
        capital = state["capital"]

        # 보유 포지션 평가
        positions_value = 0
        pos_details = []
        for ticker, pos_info in positions.items():
            df = collector.load_from_db(ticker, start_date, today)
            if df.empty:
                continue
            cur_price = df.iloc[-1]["close"]
            qty = pos_info["qty"]
            mkt_val = cur_price * qty
            positions_value += mkt_val
            unrealized = (cur_price - pos_info["entry_price"]) * qty
            name = UNIVERSE.get(ticker, ticker)
            pos_details.append({
                "name": name, "ticker": ticker,
                "qty": qty, "entry": pos_info["entry_price"],
                "current": cur_price, "value": mkt_val,
                "pnl": unrealized, "days": pos_info.get("holding_days", 0),
            })

        total_value = cash + positions_value
        daily_realized = sum(t.get("pnl", 0) for t in state.get("daily_trades", []))
        daily_unrealized = sum(p["pnl"] for p in pos_details)
        daily_total = daily_realized + daily_unrealized

        # 이전 스냅샷 가져오기 (누적 수익 계산)
        snapshots = store.get_snapshots()
        prev_value = capital
        cumulative_pnl = total_value - capital
        if not snapshots.empty:
            prev_value = snapshots.iloc[-1]["total_value"]

        daily_return = (total_value - prev_value) / prev_value * 100 if prev_value > 0 else 0
        cum_return = cumulative_pnl / capital * 100

        # 최고점 대비 드로다운 계산
        peak = capital
        if not snapshots.empty:
            peak = max(snapshots["total_value"].max(), total_value)
        drawdown = (peak - total_value) / peak * 100 if peak > 0 else 0

        # 스냅샷 저장
        store.save_snapshot({
            "date": today_dash,
            "total_value": total_value,
            "cash": cash,
            "positions_value": positions_value,
            "daily_pnl": daily_total,
            "cumulative_pnl": cumulative_pnl,
            "drawdown": drawdown,
        })

        # 자본은 초기 투입금이므로 변경하지 않음 (누적 수익률 계산용)
        _save_state(state)

        # 텔레그램 일일 리포트
        msg = f"*📈 일일 정산 ({today_dash})*\n"
        msg += f"──────────────\n"
        msg += f"총 자산: {total_value:,.0f}원\n"
        msg += f"현금: {cash:,.0f}원\n"
        msg += f"포지션: {positions_value:,.0f}원\n"
        msg += f"──────────────\n"
        msg += f"일일 수익률: {daily_return:+.2f}%\n"
        msg += f"누적 수익률: {cum_return:+.2f}%\n"
        msg += f"드로다운: {drawdown:.2f}%\n"
        msg += f"실현 손익: {daily_realized:+,.0f}원\n"
        msg += f"미실현 손익: {daily_unrealized:+,.0f}원\n"
        msg += f"거래: {len(state.get('daily_trades', []))}건\n"

        if pos_details:
            msg += f"\n*보유 종목:*\n"
            for p in pos_details:
                pnl_pct = (p["current"] - p["entry"]) / p["entry"] * 100
                msg += (f"• {p['name']} {p['qty']}주 "
                        f"{p['current']:,.0f}원 "
                        f"({pnl_pct:+.1f}%, {p['days']}일)\n")

        alert.send(msg)
        logger.info(f"일일 정산 완료: 총자산 {total_value:,.0f}원, 수익률 {daily_return:+.2f}%")

    except Exception as e:
        logger.error(f"일일 정산 실패: {e}", exc_info=True)
        alert.send(f"*❌ 일일 정산 오류*\n{e}")


# ══════════════════════════════════════════════
# 기존 데이터/리포트 작업
# ══════════════════════════════════════════════

def job_daily_update():
    """매일 16:00 - 데이터 수집 + 일일 리포트 (실제 트레이딩 상태 반영)"""
    today = datetime.now().strftime("%Y%m%d")
    today_dash = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"=== 일일 업데이트 시작: {today_dash} ===")

    config = SystemConfig()
    db_path = str(LOG_DIR / "market.db")

    try:
        # 1) 데이터 수집 (증분)
        collector = MarketDataCollector(db_path)
        result = collector.collect_all(end=today)
        logger.info(f"데이터 수집 완료: {len(result)}종목")

        # 2) 실제 트레이딩 상태 로드
        state = _load_state()
        store = DataStore(str(LOG_DIR / "trades.db"))
        start_date = (datetime.now() - timedelta(days=120)).strftime("%Y%m%d")

        # 포트폴리오 (실제 상태에서)
        positions_value = 0
        positions_list = []
        for ticker, pos_info in state.get("positions", {}).items():
            df = collector.load_from_db(ticker, start_date, today)
            cur_price = df.iloc[-1]["close"] if not df.empty else pos_info["entry_price"]
            qty = pos_info["qty"]
            mkt_val = cur_price * qty
            positions_value += mkt_val
            unrealized = (cur_price - pos_info["entry_price"]) * qty
            name = UNIVERSE.get(ticker, ticker)
            positions_list.append({
                "ticker": ticker,
                "name": name,
                "quantity": qty,
                "entry_price": pos_info["entry_price"],
                "current_price": cur_price,
                "value": mkt_val,
                "pnl": unrealized,
                "pnl_pct": (cur_price - pos_info["entry_price"]) / pos_info["entry_price"] * 100,
                "holding_days": pos_info.get("holding_days", 0),
                "stop_loss": pos_info.get("stop_loss", 0),
                "trailing_stop": pos_info.get("trailing_stop", 0),
                "highest_since_entry": pos_info.get("highest", pos_info["entry_price"]),
            })

        cash = state["cash"]
        capital = state["capital"]
        total_value = cash + positions_value

        # 이전 스냅샷에서 전일 자산
        snapshots = store.get_snapshots()
        prev_value = capital
        if not snapshots.empty:
            prev_value = snapshots.iloc[-1]["total_value"]
        cum_return = (total_value - 10_000_000) / 10_000_000 * 100  # 초기자본 대비

        peak = 10_000_000
        if not snapshots.empty:
            peak = max(snapshots["total_value"].max(), total_value)
        drawdown = (peak - total_value) / peak * 100 if peak > 0 else 0

        portfolio = {
            "total_value": total_value,
            "cash": cash,
            "prev_value": prev_value,
            "cumulative_return": cum_return,
            "drawdown": drawdown,
        }

        # 당일 거래 (trades.db에서)
        trades_today = []
        try:
            all_trades = store.get_trades(today_dash, f"{today_dash}Z")
            if not all_trades.empty:
                for _, row in all_trades.iterrows():
                    ticker = row["ticker"]
                    name = UNIVERSE.get(ticker, ticker)
                    entry_price = row["price"]
                    pnl_val = row.get("pnl", 0) or 0
                    pnl_pct = (pnl_val / (entry_price * row["quantity"]) * 100) if entry_price * row["quantity"] > 0 else 0
                    trades_today.append({
                        "ticker": ticker,
                        "name": name,
                        "side": row["side"],
                        "price": entry_price,
                        "quantity": row["quantity"],
                        "amount": row["amount"],
                        "pnl": pnl_val,
                        "pnl_pct": pnl_pct,
                        "strategy": row.get("strategy", ""),
                        "reason": row.get("memo", ""),
                    })
        except Exception:
            pass

        # 시장 데이터 (KODEX 200)
        market_df = collector.load_from_db("069500", today, today)
        market_info = {"kospi_close": 0, "kospi_change_pct": 0, "regime": "", "adx": 0, "volume_ratio": 1.0}
        if not market_df.empty:
            row = market_df.iloc[-1]
            market_info["kospi_close"] = row.get("close", 0)
            market_info["adx"] = row.get("adx14", 0) or 0
            market_info["volume_ratio"] = row.get("vol_ratio", 1.0) or 1.0

        # 스크리닝 후보
        screening = []
        for c in state.get("screening", [])[:5]:
            # reason에서 '|' 제거 (마크다운 테이블 깨짐 방지)
            signal_text = c.get("reason", "").split(" | ")[0]

            # 돌파 레벨: 저장된 값 또는 reason에서 파싱
            breakout = c.get("breakout_level", 0)
            if not breakout:
                import re
                m = re.search(r"고가\(([\d,]+)\)", c.get("reason", ""))
                if m:
                    breakout = float(m.group(1).replace(",", ""))
                else:
                    breakout = c.get("price", 0)

            # 손절가도 돌파 레벨 기준으로 재계산
            stop = c.get("stop_loss", 0)
            if breakout and breakout != c.get("price", 0):
                stop_by_pct = breakout * 0.95
                stop = max(stop_by_pct, stop)

            screening.append({
                "ticker": c["ticker"],
                "name": c.get("name", ""),
                "signal": signal_text,
                "entry_price": breakout,
                "breakout_level": breakout,
                "stop_loss": stop,
                "strength": c.get("strength", 0),
            })

        # 3) 리포트 생성
        gen = ReportGenerator(
            config,
            logs_dir=str(LOG_DIR),
            trades_db=str(LOG_DIR / "trades.db"),
            market_db=db_path,
        )

        gen.generate_daily(
            date=today_dash,
            portfolio=portfolio,
            positions=positions_list,
            trades_today=trades_today,
            market=market_info,
            screening=screening,
        )
        logger.info(f"일일 리포트 생성 완료: {today_dash}")

    except Exception as e:
        logger.error(f"일일 업데이트 실패: {e}", exc_info=True)


def job_weekly_report():
    """매주 금요일 16:30 - 주간 리포트"""
    now = datetime.now()
    iso_year, iso_week, _ = now.isocalendar()
    week_label = f"{iso_year}-W{iso_week:02d}"
    logger.info(f"=== 주간 리포트 시작: {week_label} ===")

    config = SystemConfig()

    try:
        week_start = (now - timedelta(days=now.weekday())).strftime("%Y-%m-%d")
        week_end = now.strftime("%Y-%m-%d")

        state = _load_state()
        store = DataStore(str(LOG_DIR / "trades.db"))
        snapshots = store.get_snapshots()

        total_value = state["capital"]
        cum_return = (total_value - 10_000_000) / 10_000_000 * 100
        max_dd = 0
        if not snapshots.empty:
            peak = 10_000_000
            for _, s in snapshots.iterrows():
                peak = max(peak, s["total_value"])
                dd = (peak - s["total_value"]) / peak * 100
                max_dd = max(max_dd, dd)

        portfolio = {
            "total_value": total_value,
            "cumulative_return": cum_return,
            "max_drawdown": max_dd,
        }

        gen = ReportGenerator(
            config,
            logs_dir=str(LOG_DIR),
            trades_db=str(LOG_DIR / "trades.db"),
        )

        gen.generate_weekly(week_label, week_start, week_end, portfolio)
        logger.info(f"주간 리포트 생성 완료: {week_label}")

    except Exception as e:
        logger.error(f"주간 리포트 실패: {e}", exc_info=True)


def main():
    logger.info("스케줄러 데몬 시작")

    scheduler = BlockingScheduler()

    # ─── 트레이딩 스케줄 (평일만) ─────────────────
    # 08:30 프리스크리닝
    scheduler.add_job(
        job_prescreening,
        trigger="cron",
        hour=8, minute=30,
        day_of_week="mon-fri",
        id="prescreening",
        name="08:30 프리스크리닝",
        misfire_grace_time=600,
    )

    # 09:05 장 시작 모니터링
    scheduler.add_job(
        job_start_monitoring,
        trigger="cron",
        hour=9, minute=5,
        day_of_week="mon-fri",
        id="start_monitoring",
        name="09:05 모니터링 (매수/매도)",
        misfire_grace_time=600,
    )

    # 09:10~15:20 장중 모니터링 (5분 간격)
    scheduler.add_job(
        job_intraday_monitor,
        trigger="cron",
        hour="9-15", minute="*/5",
        day_of_week="mon-fri",
        id="intraday_monitor",
        name="장중 모니터링 (5분)",
        misfire_grace_time=120,
    )

    # 15:25 장 마감 전 포지션 정리
    scheduler.add_job(
        job_close_positions,
        trigger="cron",
        hour=15, minute=25,
        day_of_week="mon-fri",
        id="close_positions",
        name="15:25 장 마감 전 정리",
        misfire_grace_time=600,
    )

    # 15:35 일일 정산
    scheduler.add_job(
        job_daily_settlement,
        trigger="cron",
        hour=15, minute=35,
        day_of_week="mon-fri",
        id="daily_settlement",
        name="15:35 일일 정산",
        misfire_grace_time=600,
    )

    # ─── 데이터/리포트 스케줄 ─────────────────────
    # 매일 16:00 (월~금) - 데이터 수집 + 일일 리포트
    scheduler.add_job(
        job_daily_update,
        trigger="cron",
        hour=16, minute=0,
        day_of_week="mon-fri",
        id="daily_update",
        name="일일 데이터 수집 + 리포트",
        misfire_grace_time=3600,  # 1시간 내 놓친 작업도 실행
    )

    # 매주 금요일 16:30 - 주간 리포트
    scheduler.add_job(
        job_weekly_report,
        trigger="cron",
        hour=16, minute=30,
        day_of_week="fri",
        id="weekly_report",
        name="주간 리포트",
        misfire_grace_time=3600,
    )

    # 시그널 핸들러 (graceful shutdown)
    def shutdown(signum, frame):
        logger.info("스케줄러 종료 요청")
        scheduler.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # ─── 텔레그램 봇 (백그라운드) ──────────────────
    try:
        bot = TelegramBot(
            bot_token=os.environ.get("TELEGRAM_BOT_TOKEN", ""),
            chat_id=os.environ.get("TELEGRAM_CHAT_ID", ""),
        )
        bot.start_in_background()
        logger.info("텔레그램 봇 시작됨")
    except Exception as e:
        logger.error(f"텔레그램 봇 시작 실패: {e}")

    logger.info("등록된 작업:")
    for job in scheduler.get_jobs():
        logger.info(f"  - {job.name}: {job.trigger}")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("스케줄러 종료")


def run_scheduler():
    """스케줄러 실행 (Windows 서비스용)"""
    main()


if __name__ == "__main__":
    main()
