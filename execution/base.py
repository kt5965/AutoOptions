"""주문 실행 인터페이스 - 페이퍼/실전 엔진 공통"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from datetime import datetime


class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"


@dataclass
class Order:
    ticker: str
    side: OrderSide
    price: float
    quantity: int
    strategy: str = ""
    memo: str = ""


@dataclass
class OrderResult:
    success: bool
    order: Order
    filled_price: float = 0
    filled_quantity: int = 0
    commission: float = 0
    tax: float = 0
    slippage: float = 0
    timestamp: str = ""
    error: str = ""

    @property
    def total_cost(self) -> float:
        return self.commission + self.tax + self.slippage

    @property
    def net_amount(self) -> float:
        amount = self.filled_price * self.filled_quantity
        if self.order.side == OrderSide.BUY:
            return amount + self.total_cost
        return amount - self.total_cost


class TradingEngine(ABC):
    """거래 엔진 공통 인터페이스"""

    @abstractmethod
    def execute_order(self, order: Order) -> OrderResult:
        ...

    @abstractmethod
    def get_current_price(self, ticker: str) -> float:
        ...

    @abstractmethod
    def is_market_open(self) -> bool:
        ...
