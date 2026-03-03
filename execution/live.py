"""실전 거래 엔진 - 증권사 API 연동 (스텁)

실전 전환 시 한투/키움 API를 여기에 연결.
PaperTradingEngine과 동일한 인터페이스.
"""
from datetime import datetime
from .base import TradingEngine, Order, OrderResult


class LiveTradingEngine(TradingEngine):
    """실전 거래 엔진 (미구현 - 증권사 API 연동 필요)"""

    def __init__(self, api_key: str = "", api_secret: str = ""):
        self.api_key = api_key
        self.api_secret = api_secret

    def execute_order(self, order: Order) -> OrderResult:
        # TODO: 증권사 API 연동
        raise NotImplementedError("실전 거래 엔진은 증권사 API 연동 후 사용 가능합니다.")

    def get_current_price(self, ticker: str) -> float:
        # TODO: 증권사 API로 실시간 호가 조회
        raise NotImplementedError

    def is_market_open(self) -> bool:
        # TODO: 장 운영시간 체크
        now = datetime.now()
        if now.weekday() >= 5:
            return False
        hour = now.hour * 100 + now.minute
        return 900 <= hour <= 1530
