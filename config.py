"""전체 시스템 설정"""
from dataclasses import dataclass, field
from enum import Enum


class MarketRegime(Enum):
    TRENDING = "trending"
    MEAN_REVERTING = "mean_reverting"
    HIGH_VOLATILITY = "high_volatility"


class TradingMode(Enum):
    PAPER = "paper"
    LIVE = "live"


@dataclass
class RiskConfig:
    initial_capital: int = 10_000_000         # 자본금 1000만원
    max_drawdown_pct: float = 0.15            # 최대 드로다운 15%
    per_stock_loss_pct: float = 0.01          # 개별 종목 손실 1% (보수적)
    daily_loss_pct: float = 0.03              # 일일 손실 3%
    max_position_pct: float = 0.15            # 종목당 최대 비중 15%
    max_positions: int = 4                    # 최대 동시 보유 종목 수


@dataclass
class FeeConfig:
    buy_commission: float = 0.00015           # 매수 수수료 0.015%
    sell_commission: float = 0.00015          # 매도 수수료 0.015%
    tax_rate: float = 0.0018                  # 세금 0.18%
    slippage_pct: float = 0.001              # 슬리피지 0.1%


@dataclass
class StrategyConfig:
    # 모멘텀 브레이크아웃 (최적화된 파라미터, 2021-2026 백테스트 기반)
    momentum_lookback: int = 60               # 60일 고가 돌파
    momentum_volume_mult: float = 1.3         # 거래량 1.3배 이상
    momentum_atr_period: int = 14
    momentum_atr_mult: float = 3.0            # ATR 3배 손절

    # 평균회귀
    mean_rev_bb_period: int = 20              # 볼린저밴드 기간
    mean_rev_bb_std: float = 2.5              # 2.5σ (더 극단적 진입만)
    mean_rev_rsi_period: int = 14
    mean_rev_rsi_oversold: float = 25.0       # RSI 25 이하만 (보수적)
    mean_rev_rsi_overbought: float = 75.0     # RSI 75 이상만

    # 레짐 판별
    regime_adx_period: int = 14
    regime_adx_trend_threshold: float = 25.0  # ADX > 25 → 추세장
    regime_vix_high_threshold: float = 25.0   # VKOSPI > 25 → 고변동성


@dataclass
class SystemConfig:
    trading_mode: TradingMode = TradingMode.PAPER
    risk: RiskConfig = field(default_factory=RiskConfig)
    fees: FeeConfig = field(default_factory=FeeConfig)
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    universe: str = "KOSPI200"
    data_start_date: str = "2021-03-03"       # 5년치 데이터
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    claude_api_key: str = ""
    db_path: str = "logs/trades.db"
