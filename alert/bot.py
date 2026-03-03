"""텔레그램 봇 명령어 인터페이스

롱폴링으로 메시지를 수신하고 명령어를 처리한다.
scheduler.py에서 백그라운드 데몬 스레드로 실행.
"""
import json
import logging
import threading
import time
import requests
from datetime import datetime
from pathlib import Path
from typing import Callable

logger = logging.getLogger(__name__)

STATE_FILE = Path(__file__).parent.parent / "logs" / "trading_state.json"
DB_PATH = Path(__file__).parent.parent / "logs" / "market.db"


def _load_state() -> dict:
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


class TelegramBot:
    """텔레그램 봇 명령어 핸들러 + 롱폴링"""

    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        self.offset = 0
        self._running = False
        self._collector = None  # lazy init

    def _get_collector(self):
        if self._collector is None:
            from ..data.collector import MarketDataCollector
            self._collector = MarketDataCollector(str(DB_PATH))
        return self._collector

    # ─── 메시지 송수신 ────────────────────────────

    def _send(self, text: str):
        """텔레그램 메시지 전송"""
        try:
            requests.post(f"{self.base_url}/sendMessage", json={
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "Markdown",
            }, timeout=10)
        except Exception as e:
            logger.error(f"봇 메시지 전송 실패: {e}")

    def _poll(self) -> list[dict]:
        """getUpdates 롱폴링"""
        try:
            resp = requests.get(f"{self.base_url}/getUpdates", params={
                "offset": self.offset,
                "timeout": 30,
            }, timeout=35)
            data = resp.json()
            if data.get("ok"):
                return data.get("result", [])
        except Exception as e:
            logger.warning(f"폴링 오류: {e}")
            time.sleep(5)
        return []

    # ─── 명령어 핸들러 ────────────────────────────

    def cmd_help(self, _args: str):
        self._send(
            "*사용 가능한 명령어*\n\n"
            "/status - 포트폴리오 현황\n"
            "/positions - 보유 종목 상세\n"
            "/screening - 스크리닝 결과\n"
            "/price 종목코드 - 실시간 시세\n"
            "/update - 최신 주가 데이터 갱신\n"
            "/help - 명령어 목록"
        )

    def cmd_status(self, _args: str):
        state = _load_state()
        if not state:
            self._send("상태 파일을 읽을 수 없습니다.")
            return

        capital = state.get("capital", 0)
        cash = state.get("cash", 0)
        positions = state.get("positions", {})
        daily_pnl = state.get("daily_pnl", 0)

        # 보유 종목 평가금액 합산
        pos_value = sum(p.get("current_value", p.get("entry_price", 0) * p.get("quantity", 0))
                        for p in positions.values())
        total = cash + pos_value
        ret_pct = (total - capital) / capital * 100 if capital else 0

        self._send(
            f"*포트폴리오 현황*\n\n"
            f"총 자산: {total:,.0f}원\n"
            f"현금: {cash:,.0f}원\n"
            f"보유 평가: {pos_value:,.0f}원\n"
            f"보유 종목: {len(positions)}개\n"
            f"누적 수익률: {ret_pct:+.2f}%\n"
            f"당일 손익: {daily_pnl:+,.0f}원"
        )

    def cmd_positions(self, _args: str):
        state = _load_state()
        positions = state.get("positions", {})

        if not positions:
            self._send("보유 종목이 없습니다.")
            return

        # 실시간 가격 조회
        collector = self._get_collector()
        tickers = list(positions.keys())
        live = collector.fetch_live_prices(tickers)

        lines = ["*보유 종목 상세*\n"]
        for ticker, pos in positions.items():
            name = pos.get("name", ticker)
            entry = pos.get("entry_price", 0)
            qty = pos.get("quantity", 0)
            stop = pos.get("stop_loss", 0)
            days = pos.get("holding_days", 0)
            trail = pos.get("trailing_stop", 0)

            cur = live.get(ticker, {}).get("close", 0)
            if cur:
                pnl_pct = (cur - entry) / entry * 100
                price_str = f"{cur:,.0f}원 ({pnl_pct:+.1f}%)"
            else:
                price_str = "조회불가"

            lines.append(
                f"*{name}* ({ticker})\n"
                f"  진입: {entry:,.0f}원 × {qty}주\n"
                f"  현재: {price_str}\n"
                f"  손절: {stop:,.0f} / 트레일링: {trail:,.0f}\n"
                f"  보유: {days}일"
            )

        self._send("\n\n".join(lines))

    def cmd_screening(self, _args: str):
        state = _load_state()
        screening = state.get("screening", [])

        if not screening:
            self._send("스크리닝 결과가 없습니다.")
            return

        lines = ["*스크리닝 결과 (매수 후보)*\n"]
        for i, s in enumerate(screening, 1):
            name = s.get("name", s.get("ticker", "?"))
            ticker = s.get("ticker", "")
            price = s.get("price", 0)
            stop = s.get("stop_loss", 0)
            strength = s.get("strength", 0)
            reason = s.get("reason", "")

            lines.append(
                f"{i}. *{name}* ({ticker})\n"
                f"   돌파레벨: {price:,.0f}원\n"
                f"   손절가: {stop:,.0f}원\n"
                f"   강도: {strength:.2f}\n"
                f"   {reason}"
            )

        self._send("\n\n".join(lines))

    def cmd_price(self, args: str):
        ticker = args.strip()
        if not ticker:
            self._send("사용법: /price 종목코드\n예: /price 005930")
            return

        # 종목명 조회
        from ..data.collector import UNIVERSE
        name = UNIVERSE.get(ticker, "")

        collector = self._get_collector()
        live = collector.fetch_live_prices([ticker])
        data = live.get(ticker)

        if not data:
            # UNIVERSE에 없으면 pykrx에서 이름 조회
            if not name:
                try:
                    from pykrx import stock as pykrx_stock
                    name = pykrx_stock.get_market_ticker_name(ticker) or ticker
                except Exception:
                    name = ticker
            self._send(f"{name} ({ticker}): 시세 조회 실패 (장외 시간이거나 잘못된 종목코드)")
            return

        if not name:
            try:
                from pykrx import stock as pykrx_stock
                name = pykrx_stock.get_market_ticker_name(ticker) or ticker
            except Exception:
                name = ticker

        self._send(
            f"*{name}* ({ticker})\n\n"
            f"시가: {data['open']:,.0f}원\n"
            f"고가: {data['high']:,.0f}원\n"
            f"저가: {data['low']:,.0f}원\n"
            f"현재가: {data['close']:,.0f}원\n"
            f"거래량: {data['volume']:,.0f}"
        )

    def cmd_update(self, _args: str):
        self._send("주가 데이터 갱신을 시작합니다... (1-2분 소요)")
        try:
            collector = self._get_collector()
            today = datetime.now().strftime("%Y%m%d")
            results = collector.collect_all(end=today)
            success = sum(1 for v in results.values() if v is not None and not v.empty)
            self._send(f"데이터 갱신 완료: {success}/{len(results)} 종목 업데이트")
        except Exception as e:
            logger.error(f"데이터 갱신 실패: {e}")
            self._send(f"데이터 갱신 실패: {e}")

    # ─── 메인 루프 ────────────────────────────────

    COMMANDS: dict[str, str] = {
        "/help": "cmd_help",
        "/start": "cmd_help",
        "/status": "cmd_status",
        "/positions": "cmd_positions",
        "/screening": "cmd_screening",
        "/price": "cmd_price",
        "/update": "cmd_update",
    }

    def _handle_message(self, message: dict):
        """메시지 처리"""
        chat_id = str(message.get("chat", {}).get("id", ""))
        if chat_id != self.chat_id:
            return  # 다른 사용자 무시

        text = message.get("text", "").strip()
        if not text.startswith("/"):
            return

        parts = text.split(None, 1)
        cmd = parts[0].lower().split("@")[0]  # /price@botname → /price
        args = parts[1] if len(parts) > 1 else ""

        handler_name = self.COMMANDS.get(cmd)
        if handler_name:
            handler = getattr(self, handler_name)
            try:
                handler(args)
            except Exception as e:
                logger.error(f"명령어 처리 오류 [{cmd}]: {e}")
                self._send(f"명령어 처리 중 오류: {e}")
        else:
            self._send(f"알 수 없는 명령어: {cmd}\n/help 로 명령어 목록을 확인하세요.")

    def run(self):
        """롱폴링 메인 루프 (블로킹)"""
        self._running = True
        logger.info("텔레그램 봇 시작")
        self._send("봇이 시작되었습니다. /help 로 명령어를 확인하세요.")

        while self._running:
            updates = self._poll()
            for update in updates:
                self.offset = update["update_id"] + 1
                msg = update.get("message")
                if msg:
                    self._handle_message(msg)

    def stop(self):
        self._running = False

    def start_in_background(self) -> threading.Thread:
        """데몬 스레드로 봇 실행"""
        t = threading.Thread(target=self.run, daemon=True, name="telegram-bot")
        t.start()
        logger.info("텔레그램 봇 백그라운드 스레드 시작")
        return t
