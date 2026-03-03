"""텔레그램 알림"""
import os
import requests
from typing import Optional


class TelegramAlert:
    """텔레그램 봇으로 거래 알림 전송"""

    BASE_URL = "https://api.telegram.org/bot{token}"

    def __init__(self, bot_token: str = "", chat_id: str = ""):
        self.bot_token = bot_token or os.environ.get("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = chat_id or os.environ.get("TELEGRAM_CHAT_ID", "")
        self.enabled = bool(self.bot_token and self.chat_id)

    def send(self, message: str) -> bool:
        if not self.enabled:
            print(f"[ALERT] {message}")
            return False

        url = f"{self.BASE_URL.format(token=self.bot_token)}/sendMessage"
        try:
            resp = requests.post(url, json={
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "Markdown",
            }, timeout=10)
            return resp.ok
        except requests.RequestException as e:
            print(f"[ALERT ERROR] {e}")
            return False

    def send_trade_alert(self, trade: dict):
        side = "매수" if trade["side"] == "buy" else "매도"
        msg = (
            f"*{side} 체결*\n"
            f"종목: {trade['ticker']}\n"
            f"가격: {trade['price']:,.0f}원\n"
            f"수량: {trade['quantity']}주\n"
            f"금액: {trade['amount']:,.0f}원\n"
            f"전략: {trade.get('strategy', '-')}\n"
            f"사유: {trade.get('reason', '-')}"
        )
        self.send(msg)

    def send_daily_report(self, report: dict):
        msg = (
            f"*일일 리포트*\n"
            f"총 자산: {report['total_value']:,.0f}원\n"
            f"일일 수익률: {report['daily_return']:+.2f}%\n"
            f"누적 수익률: {report['cumulative_return']:+.2f}%\n"
            f"드로다운: {report['drawdown']:.2f}%\n"
            f"보유 종목: {report['positions_count']}개\n"
            f"현금: {report['cash']:,.0f}원"
        )
        self.send(msg)
