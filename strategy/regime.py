"""시장 레짐 판별 - 추세/횡보/고변동성"""
import pandas as pd
from .indicators import adx, atr, sma
from ..config import MarketRegime, StrategyConfig


class RegimeDetector:
    """ADX + 변동성으로 시장 레짐 판별

    - ADX > 25 → 추세장 → 모멘텀 전략
    - ADX < 25 & 변동성 낮음 → 횡보장 → 평균회귀 전략
    - 변동성 높음 → 고변동성 → 포지션 축소
    """

    def __init__(self, config: StrategyConfig):
        self.adx_period = config.regime_adx_period
        self.adx_threshold = config.regime_adx_trend_threshold
        self.vix_threshold = config.regime_vix_high_threshold

    def detect(self, market_df: pd.DataFrame, vkospi: float = 0) -> MarketRegime:
        """시장 지수 데이터로 현재 레짐 판별

        Args:
            market_df: 코스피 지수 OHLCV
            vkospi: VKOSPI 값 (없으면 ATR로 대체)
        """
        if len(market_df) < self.adx_period + 20:
            return MarketRegime.MEAN_REVERTING

        adx_val = adx(market_df["high"], market_df["low"], market_df["close"], self.adx_period)
        current_adx = adx_val.iloc[-1]

        # 변동성 체크 (VKOSPI 또는 ATR 기반)
        if vkospi > 0:
            high_vol = vkospi > self.vix_threshold
        else:
            atr_val = atr(market_df["high"], market_df["low"], market_df["close"], 20)
            atr_pct = atr_val.iloc[-1] / market_df["close"].iloc[-1] * 100
            high_vol = atr_pct > 2.0  # ATR이 종가의 2% 이상이면 고변동성

        if high_vol:
            return MarketRegime.HIGH_VOLATILITY

        if current_adx > self.adx_threshold:
            return MarketRegime.TRENDING

        return MarketRegime.MEAN_REVERTING
