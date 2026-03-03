"""평균회귀 전략 (서브)"""
import pandas as pd
from typing import Optional
from .momentum import Signal
from .indicators import bollinger_bands, rsi
from ..config import StrategyConfig


class MeanReversion:
    """볼린저밴드 + RSI 과매도/과매수 기반 평균회귀"""

    def __init__(self, config: StrategyConfig):
        self.bb_period = config.mean_rev_bb_period
        self.bb_std = config.mean_rev_bb_std
        self.rsi_period = config.mean_rev_rsi_period
        self.rsi_oversold = config.mean_rev_rsi_oversold
        self.rsi_overbought = config.mean_rev_rsi_overbought

    def generate_signal(self, df: pd.DataFrame, ticker: str) -> Optional[Signal]:
        if len(df) < self.bb_period + 10:
            return None

        upper, mid, lower = bollinger_bands(df["close"], self.bb_period, self.bb_std)
        rsi_val = rsi(df["close"], self.rsi_period)

        latest = df.iloc[-1]
        current_rsi = rsi_val.iloc[-1]

        # 매수: 하단밴드 터치 + RSI 과매도
        if latest["close"] <= lower.iloc[-1] and current_rsi < self.rsi_oversold:
            stop = latest["close"] * 0.97  # 3% 손절
            target = mid.iloc[-1]           # 중심선까지 목표
            strength = (self.rsi_oversold - current_rsi) / self.rsi_oversold
            return Signal(
                ticker=ticker, action="buy",
                price=latest["close"], stop_loss=stop, take_profit=target,
                strength=min(strength, 1.0),
                reason=f"BB 하단 터치, RSI {current_rsi:.1f}",
            )

        # 매도: 상단밴드 터치 + RSI 과매수
        if latest["close"] >= upper.iloc[-1] and current_rsi > self.rsi_overbought:
            return Signal(
                ticker=ticker, action="sell",
                price=latest["close"], stop_loss=0, take_profit=0,
                strength=min((current_rsi - self.rsi_overbought) / 30, 1.0),
                reason=f"BB 상단 터치, RSI {current_rsi:.1f}",
            )

        return None
