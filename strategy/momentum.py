"""모멘텀 브레이크아웃 전략 (메인)

진입 조건:
  - 20일 고가 돌파
  - ADX(14) > 20
  - 거래량 > 20일 평균의 1.3배
  - MA5 > MA20 (단기 추세 상승)

청산 조건:
  - 손절: 진입가 -5% 또는 ATR*2 이탈 중 좁은 것
  - 트레일링 스탑: 보유 중 고점 대비 2.5 ATR 이탈
  - 시간 청산: 15거래일 보유 후 강제 청산
  - 일일 손실 5만원 도달 시 전체 청산

포지션 사이징:
  - 최대 손실 3만원 / 손절폭
  - 한 종목에 자본의 60% 이하
  - 동시 보유 최대 2종목
"""
import logging
import pandas as pd
from dataclasses import dataclass, field
from typing import Optional
from .indicators import donchian_channel, atr, adx, sma
from ..config import StrategyConfig

logger = logging.getLogger(__name__)


@dataclass
class Signal:
    ticker: str
    action: str          # "buy" | "sell"
    price: float
    stop_loss: float
    take_profit: float
    strength: float      # 0.0 ~ 1.0
    reason: str
    breakout_level: float = 0  # 20일 고가 (돌파 레벨)


@dataclass
class MomentumPosition:
    """모멘텀 전략 포지션 추적 (트레일링 스탑 + 보유일수)"""
    ticker: str
    entry_price: float
    entry_date: str
    stop_loss: float         # 초기 손절가
    trailing_stop: float     # 트레일링 스탑 (동적)
    highest_since_entry: float  # 진입 후 최고가
    holding_days: int = 0
    atr_at_entry: float = 0  # 진입 시점 ATR

    def update(self, current_high: float, current_atr: float) -> str:
        """매일 호출: 트레일링 스탑 갱신, 보유일 증가. 청산 사유 리턴 (없으면 빈 문자열)"""
        self.holding_days += 1

        # 고점 갱신
        if current_high > self.highest_since_entry:
            self.highest_since_entry = current_high
            # 트레일링 스탑: 고점 대비 2.5 ATR
            new_trail = self.highest_since_entry - 2.5 * current_atr
            if new_trail > self.trailing_stop:
                self.trailing_stop = new_trail

        return ""

    def check_exit(self, current_price: float) -> str:
        """청산 조건 체크. 사유 문자열 리턴 (없으면 빈 문자열)"""
        # 1) 초기 손절
        if current_price <= self.stop_loss:
            loss_pct = (current_price - self.entry_price) / self.entry_price * 100
            return f"손절 ({loss_pct:+.1f}%, 손절가 {self.stop_loss:,.0f})"

        # 2) 트레일링 스탑
        if current_price <= self.trailing_stop:
            drop = (self.highest_since_entry - current_price) / self.highest_since_entry * 100
            return f"트레일링스탑 (고점 {self.highest_since_entry:,.0f} 대비 -{drop:.1f}%)"

        # 3) 시간 청산: 15거래일
        if self.holding_days >= 15:
            pnl_pct = (current_price - self.entry_price) / self.entry_price * 100
            return f"시간청산 ({self.holding_days}일 보유, {pnl_pct:+.1f}%)"

        return ""


class MomentumBreakout:
    """모멘텀 브레이크아웃 전략"""

    # ─── 기본값 (config로 오버라이드 가능) ────────
    LOOKBACK = 40              # 40일 고가 돌파
    ADX_THRESHOLD = 20         # ADX > 20
    VOLUME_MULT = 2.0          # 거래량 2배
    STOP_PCT = 0.05            # 진입가 -5%
    ATR_STOP_MULT = 3.0        # ATR 3배 손절
    TRAILING_ATR_MULT = 2.5    # 트레일링 2.5 ATR
    TIME_EXIT_DAYS = 15        # 15거래일 시간 청산
    MAX_LOSS_PER_TRADE = 30_000   # 1건당 최대 손실 3만원
    MAX_POSITION_PCT = 0.60    # 한 종목 자본의 60%
    MAX_POSITIONS = 2          # 동시 보유 최대 2종목
    DAILY_LOSS_LIMIT = 50_000  # 일일 손실 5만원

    def __init__(self, config: StrategyConfig = None):
        if config:
            self.LOOKBACK = config.momentum_lookback
            self.VOLUME_MULT = config.momentum_volume_mult
            self.ATR_STOP_MULT = config.momentum_atr_mult
        self.positions: dict[str, MomentumPosition] = {}
        self.trade_log: list[dict] = []
        self.daily_pnl: float = 0  # 당일 실현 손익 누적

    # ─── 진입 시그널 ──────────────────────────────
    def generate_signal(self, df: pd.DataFrame, ticker: str) -> Optional[Signal]:
        """일봉 데이터로 매수 시그널 생성"""
        if len(df) < 60:
            return None

        close = df["close"]
        high = df["high"]
        low = df["low"]
        volume = df["volume"].astype(float)

        latest = df.iloc[-1]
        prev_high_20 = high.iloc[-21:-1].max()  # 직전 20일 고가 (당일 제외)

        # 지표 계산
        ma5 = close.rolling(5).mean()
        ma20 = close.rolling(20).mean()
        vol_ma20 = volume.rolling(20).mean()
        adx_val = adx(high, low, close, 14)
        atr_val = atr(high, low, close, 14)

        cur_close = latest["close"]
        cur_high = latest["high"]
        cur_vol = latest["volume"]
        cur_ma5 = ma5.iloc[-1]
        cur_ma20 = ma20.iloc[-1]
        cur_vol_ma = vol_ma20.iloc[-1]
        cur_adx = adx_val.iloc[-1]
        cur_atr = atr_val.iloc[-1]

        if pd.isna(cur_adx) or pd.isna(cur_atr) or cur_vol_ma == 0:
            return None

        vol_ratio = cur_vol / cur_vol_ma

        # ─── 진입 조건 4가지 모두 충족 ───
        cond_breakout = cur_high > prev_high_20
        cond_adx = cur_adx > self.ADX_THRESHOLD
        cond_volume = vol_ratio > self.VOLUME_MULT
        cond_trend = cur_ma5 > cur_ma20

        reasons = []
        if cond_breakout:
            reasons.append(f"20일고가({prev_high_20:,.0f})돌파")
        if cond_adx:
            reasons.append(f"ADX={cur_adx:.1f}")
        if cond_volume:
            reasons.append(f"거래량{vol_ratio:.1f}배")
        if cond_trend:
            reasons.append("MA5>MA20")

        if not (cond_breakout and cond_adx and cond_volume and cond_trend):
            # 조건 미충족 → 로그만 (디버그)
            if cond_breakout:  # 돌파는 했는데 다른 조건 불충족
                failed = []
                if not cond_adx:
                    failed.append(f"ADX={cur_adx:.1f}<{self.ADX_THRESHOLD}")
                if not cond_volume:
                    failed.append(f"거래량{vol_ratio:.1f}배<{self.VOLUME_MULT}배")
                if not cond_trend:
                    failed.append(f"MA5({cur_ma5:,.0f})<MA20({cur_ma20:,.0f})")
                self._log("SKIP", ticker, cur_close,
                          f"20일돌파했으나 미충족: {', '.join(failed)}")
            return None

        # ─── 손절가 계산: -5% vs ATR*2 중 좁은 것 ───
        stop_by_pct = cur_close * (1 - self.STOP_PCT)
        stop_by_atr = cur_close - self.ATR_STOP_MULT * cur_atr
        stop_loss = max(stop_by_pct, stop_by_atr)  # 더 높은(좁은) 쪽

        reason_str = " + ".join(reasons)
        stop_type = "5%손절" if stop_loss == stop_by_pct else f"ATR*2손절"
        reason_str += f" | {stop_type}={stop_loss:,.0f}"

        # 시그널 강도: ADX와 거래량비율 기반
        strength = min((cur_adx - self.ADX_THRESHOLD) / 30, 1.0) * min(vol_ratio / 3, 1.0)

        self._log("SIGNAL", ticker, cur_close, reason_str)

        return Signal(
            ticker=ticker,
            action="buy",
            price=cur_close,
            stop_loss=stop_loss,
            take_profit=0,  # 트레일링으로 관리, 고정 익절 없음
            strength=max(strength, 0.1),
            reason=reason_str,
            breakout_level=prev_high_20,
        )

    # ─── 포지션 사이징 ────────────────────────────
    def calculate_position_size(self, price: float, stop_loss: float, capital: float, cash: float) -> int:
        """최대 손실 3만원 / 손절폭, 종목당 자본 60% 이하, 현금 한도"""
        if price <= stop_loss or stop_loss <= 0:
            return 0

        risk_per_share = price - stop_loss

        # 리스크 기반: 3만원 / 손절폭
        qty_by_risk = int(self.MAX_LOSS_PER_TRADE / risk_per_share)

        # 비중 제한: 자본의 60%
        qty_by_weight = int(capital * self.MAX_POSITION_PCT / price)

        # 현금 한도
        qty_by_cash = int(cash / (price * 1.002))  # 수수료 여유

        qty = min(qty_by_risk, qty_by_weight, qty_by_cash)
        qty = max(qty, 0)

        if qty > 0:
            self._log("SIZING", "-", price,
                      f"리스크={qty_by_risk}주, 비중={qty_by_weight}주, "
                      f"현금={qty_by_cash}주 → {qty}주 "
                      f"(손절폭={risk_per_share:,.0f}, 투자금={price*qty:,.0f})")

        return qty

    # ─── 포지션 관리 (매일 호출) ──────────────────
    def on_new_position(self, ticker: str, entry_price: float, entry_date: str, stop_loss: float, atr_val: float):
        """신규 포지션 등록"""
        trailing = entry_price - self.TRAILING_ATR_MULT * atr_val
        self.positions[ticker] = MomentumPosition(
            ticker=ticker,
            entry_price=entry_price,
            entry_date=entry_date,
            stop_loss=stop_loss,
            trailing_stop=trailing,
            highest_since_entry=entry_price,
            holding_days=0,
            atr_at_entry=atr_val,
        )
        self._log("ENTRY", ticker, entry_price,
                  f"손절={stop_loss:,.0f}, 트레일링초기={trailing:,.0f}, ATR={atr_val:,.0f}")

    def check_position_exit(self, ticker: str, current_price: float, current_high: float, current_atr: float) -> str:
        """보유 포지션 청산 조건 체크. 사유 리턴 (빈 문자열이면 보유 유지)"""
        pos = self.positions.get(ticker)
        if pos is None:
            return ""

        pos.update(current_high, current_atr)
        exit_reason = pos.check_exit(current_price)

        if exit_reason:
            self._log("EXIT", ticker, current_price, exit_reason)

        return exit_reason

    def on_position_closed(self, ticker: str, exit_price: float, pnl: float):
        """포지션 청산 후 정리"""
        pos = self.positions.pop(ticker, None)
        if pos:
            self._log("CLOSED", ticker, exit_price,
                      f"PnL={pnl:+,.0f}, 보유{pos.holding_days}일, "
                      f"진입={pos.entry_price:,.0f}→청산={exit_price:,.0f}")
        self.daily_pnl += pnl

    def check_daily_loss_limit(self) -> bool:
        """일일 실현 손실 5만원 초과 여부"""
        return self.daily_pnl <= -self.DAILY_LOSS_LIMIT

    def reset_daily(self):
        """매일 장 시작 시 호출"""
        self.daily_pnl = 0

    def can_open(self) -> bool:
        """신규 진입 가능 여부"""
        return len(self.positions) < self.MAX_POSITIONS

    # ─── 로그 ─────────────────────────────────────
    def _log(self, action: str, ticker: str, price: float, msg: str):
        entry = {"action": action, "ticker": ticker, "price": price, "msg": msg}
        self.trade_log.append(entry)
        logger.info(f"[{action:6s}] {ticker:>6s} {price:>10,.0f} | {msg}")

    def get_log(self) -> list[dict]:
        return self.trade_log
