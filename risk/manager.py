"""리스크 관리 - 포지션 사이징, 손절, 드로다운 체크"""
from dataclasses import dataclass, field
from ..config import RiskConfig, FeeConfig


@dataclass
class Position:
    ticker: str
    entry_price: float
    quantity: int
    stop_loss: float
    take_profit: float
    strategy: str
    entry_date: str

    @property
    def cost(self) -> float:
        return self.entry_price * self.quantity


@dataclass
class Portfolio:
    cash: float
    positions: dict[str, Position] = field(default_factory=dict)
    peak_value: float = 0
    daily_start_value: float = 0

    @property
    def positions_value(self) -> float:
        return sum(p.cost for p in self.positions.values())

    def total_value(self, current_prices: dict[str, float]) -> float:
        value = self.cash
        for ticker, pos in self.positions.items():
            price = current_prices.get(ticker, pos.entry_price)
            value += price * pos.quantity
        return value


class RiskManager:
    """리스크 관리 엔진"""

    def __init__(self, risk_config: RiskConfig, fee_config: FeeConfig):
        self.config = risk_config
        self.fees = fee_config
        self.portfolio = Portfolio(
            cash=risk_config.initial_capital,
            peak_value=risk_config.initial_capital,
            daily_start_value=risk_config.initial_capital,
        )

    def calculate_position_size(self, price: float, stop_loss: float) -> int:
        """켈리 기준 + 리스크 한도 기반 포지션 사이징

        최대 손실 = 자본금 * per_stock_loss_pct
        수량 = 최대 손실 / (진입가 - 손절가)
        """
        if price <= stop_loss or stop_loss <= 0:
            return 0

        total_value = self.portfolio.cash + self.portfolio.positions_value
        max_loss = total_value * self.config.per_stock_loss_pct
        risk_per_share = price - stop_loss

        # 리스크 기반 수량
        qty_by_risk = int(max_loss / risk_per_share)

        # 최대 비중 제한
        max_amount = total_value * self.config.max_position_pct
        qty_by_weight = int(max_amount / price)

        # 현금 제한 (수수료 포함)
        buy_cost_per_share = price * (1 + self.fees.buy_commission + self.fees.slippage_pct)
        qty_by_cash = int(self.portfolio.cash / buy_cost_per_share)

        qty = min(qty_by_risk, qty_by_weight, qty_by_cash)
        return max(qty, 0)

    def check_drawdown(self, current_prices: dict[str, float]) -> bool:
        """최대 드로다운 초과 여부 확인. True면 거래 중단."""
        total = self.portfolio.total_value(current_prices)
        self.portfolio.peak_value = max(self.portfolio.peak_value, total)
        drawdown = (self.portfolio.peak_value - total) / self.portfolio.peak_value
        return drawdown >= self.config.max_drawdown_pct

    def check_daily_loss(self, current_prices: dict[str, float]) -> bool:
        """일일 손실 한도 초과 여부. True면 당일 거래 중단."""
        total = self.portfolio.total_value(current_prices)
        daily_loss = (self.portfolio.daily_start_value - total) / self.portfolio.daily_start_value
        return daily_loss >= self.config.daily_loss_pct

    def check_stop_loss(self, ticker: str, current_price: float) -> bool:
        """개별 종목 손절가 도달 여부"""
        pos = self.portfolio.positions.get(ticker)
        if pos is None:
            return False
        return current_price <= pos.stop_loss

    def check_take_profit(self, ticker: str, current_price: float) -> bool:
        """개별 종목 익절가 도달 여부"""
        pos = self.portfolio.positions.get(ticker)
        if pos is None:
            return False
        return pos.take_profit > 0 and current_price >= pos.take_profit

    def can_open_position(self) -> bool:
        """신규 포지션 가능 여부"""
        return len(self.portfolio.positions) < self.config.max_positions

    def reset_daily(self, current_prices: dict[str, float]):
        """일일 시작 시 호출"""
        self.portfolio.daily_start_value = self.portfolio.total_value(current_prices)

    def get_drawdown(self, current_prices: dict[str, float]) -> float:
        total = self.portfolio.total_value(current_prices)
        if self.portfolio.peak_value == 0:
            return 0
        return (self.portfolio.peak_value - total) / self.portfolio.peak_value

    def calculate_fees(self, price: float, quantity: int, side: str) -> dict:
        """수수료/세금/슬리피지 계산"""
        amount = price * quantity
        commission = amount * (self.fees.buy_commission if side == "buy" else self.fees.sell_commission)
        tax = amount * self.fees.tax_rate if side == "sell" else 0
        slippage = amount * self.fees.slippage_pct
        return {
            "commission": commission,
            "tax": tax,
            "slippage": slippage,
            "total_cost": commission + tax + slippage,
        }
