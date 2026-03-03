"""전략 엔진 - 레짐에 따라 전략 자동 전환"""
import pandas as pd
from typing import Optional
from .momentum import MomentumBreakout, Signal
from .mean_reversion import MeanReversion
from .regime import RegimeDetector
from ..config import SystemConfig, MarketRegime


class StrategyEngine:
    """시장 레짐에 따라 메인/서브 전략을 자동 전환"""

    def __init__(self, config: SystemConfig):
        self.momentum = MomentumBreakout(config.strategy)
        self.mean_rev = MeanReversion(config.strategy)
        self.regime_detector = RegimeDetector(config.strategy)
        self.current_regime = MarketRegime.TRENDING

    def update_regime(self, market_df: pd.DataFrame, vkospi: float = 0) -> MarketRegime:
        self.current_regime = self.regime_detector.detect(market_df, vkospi)
        return self.current_regime

    def generate_signals(self, universe_data: dict[str, pd.DataFrame]) -> list[Signal]:
        """전체 유니버스에 대해 현재 레짐에 맞는 시그널 생성"""
        signals = []

        for ticker, df in universe_data.items():
            signal = self._get_signal_for_regime(df, ticker)
            if signal is not None:
                signals.append(signal)

        # 시그널 강도로 정렬
        signals.sort(key=lambda s: s.strength, reverse=True)
        return signals

    def _get_signal_for_regime(self, df: pd.DataFrame, ticker: str) -> Optional[Signal]:
        if self.current_regime == MarketRegime.TRENDING:
            return self.momentum.generate_signal(df, ticker)
        elif self.current_regime == MarketRegime.MEAN_REVERTING:
            return self.mean_rev.generate_signal(df, ticker)
        elif self.current_regime == MarketRegime.HIGH_VOLATILITY:
            # 고변동성: 평균회귀로 보수적 진입, 강도 낮추기
            signal = self.mean_rev.generate_signal(df, ticker)
            if signal and signal.action == "buy":
                signal.strength *= 0.5  # 포지션 절반
            return signal
        return None
