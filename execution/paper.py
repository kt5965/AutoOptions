"""페이퍼 트레이딩 엔진 - 가상 체결"""
import random
from datetime import datetime
from .base import TradingEngine, Order, OrderSide, OrderResult
from ..config import FeeConfig


class PaperTradingEngine(TradingEngine):
    """실제 시장가 기반 가상 체결 엔진

    수수료, 슬리피지, 체결 지연을 현실적으로 시뮬레이션.
    """

    def __init__(self, fee_config: FeeConfig, price_provider=None):
        self.fees = fee_config
        self.price_provider = price_provider  # 백테스트 시 가격 데이터 주입
        self._current_prices: dict[str, float] = {}

    def set_current_prices(self, prices: dict[str, float]):
        """백테스팅용: 현재 시점 가격 세팅"""
        self._current_prices = prices

    def execute_order(self, order: Order) -> OrderResult:
        price = self._current_prices.get(order.ticker, order.price)

        # 슬리피지 시뮬레이션: 매수는 불리(높게), 매도는 불리(낮게)
        slippage_pct = self.fees.slippage_pct * (1 + random.uniform(0, 0.5))
        if order.side == OrderSide.BUY:
            filled_price = price * (1 + slippage_pct)
        else:
            filled_price = price * (1 - slippage_pct)

        amount = filled_price * order.quantity
        if order.side == OrderSide.BUY:
            commission = amount * self.fees.buy_commission
        else:
            commission = amount * self.fees.sell_commission
        tax = amount * self.fees.tax_rate if order.side == OrderSide.SELL else 0
        slippage = abs(filled_price - price) * order.quantity

        return OrderResult(
            success=True,
            order=order,
            filled_price=filled_price,
            filled_quantity=order.quantity,
            commission=commission,
            tax=tax,
            slippage=slippage,
            timestamp=datetime.now().isoformat(),
        )

    def get_current_price(self, ticker: str) -> float:
        return self._current_prices.get(ticker, 0)

    def is_market_open(self) -> bool:
        return True  # 페이퍼 트레이딩은 항상 가능
