"""백테스팅 엔진 - 과거 데이터로 전략 검증

실제 전략 로직(MomentumBreakout)과 동일한 청산 조건을 사용:
- 손절: 진입가 -5% 또는 ATR*3 중 좁은 것 (일봉 저가 기준)
- 트레일링 스탑: 고점 대비 2.5 ATR
- 시간 청산: 15거래일
- 일일 손실 한도: 5만원
"""
import pandas as pd
from datetime import datetime
from .config import SystemConfig, MarketRegime
from .data.collector import MarketDataCollector
from .data.store import DataStore
from .strategy.engine import StrategyEngine
from .strategy.momentum import MomentumBreakout
from .risk.manager import RiskManager, Position
from .execution.paper import PaperTradingEngine
from .execution.base import Order, OrderSide


class BacktestResult:
    def __init__(self):
        self.daily_values: list[dict] = []
        self.trades: list[dict] = []
        self.initial_capital: float = 0
        self.final_value: float = 0

    @property
    def total_return_pct(self) -> float:
        if self.initial_capital == 0:
            return 0
        return (self.final_value - self.initial_capital) / self.initial_capital * 100

    @property
    def max_drawdown_pct(self) -> float:
        if not self.daily_values:
            return 0
        peak = 0
        max_dd = 0
        for d in self.daily_values:
            peak = max(peak, d["total_value"])
            dd = (peak - d["total_value"]) / peak if peak > 0 else 0
            max_dd = max(max_dd, dd)
        return max_dd * 100

    @property
    def trade_count(self) -> int:
        return len(self.trades)

    @property
    def win_rate(self) -> float:
        sells = [t for t in self.trades if t["side"] == "sell" and t.get("pnl", 0) != 0]
        if not sells:
            return 0
        wins = sum(1 for t in sells if t["pnl"] > 0)
        return wins / len(sells) * 100

    @property
    def sharpe_ratio(self) -> float:
        if len(self.daily_values) < 2:
            return 0
        returns = pd.Series([d["daily_return"] for d in self.daily_values])
        if returns.std() == 0:
            return 0
        return (returns.mean() / returns.std()) * (252 ** 0.5)

    @property
    def avg_pnl(self) -> float:
        sells = [t for t in self.trades if t["side"] == "sell"]
        if not sells:
            return 0
        return sum(t["pnl"] for t in sells) / len(sells)

    @property
    def profit_factor(self) -> float:
        sells = [t for t in self.trades if t["side"] == "sell"]
        gross_profit = sum(t["pnl"] for t in sells if t["pnl"] > 0)
        gross_loss = abs(sum(t["pnl"] for t in sells if t["pnl"] < 0))
        if gross_loss == 0:
            return float("inf") if gross_profit > 0 else 0
        return gross_profit / gross_loss

    def summary(self) -> str:
        sells = [t for t in self.trades if t["side"] == "sell"]
        exit_reasons = {}
        for t in sells:
            reason = t.get("memo", "기타")
            # 사유 카테고리 분류
            if "손절" in reason:
                cat = "손절"
            elif "트레일링" in reason:
                cat = "트레일링스탑"
            elif "시간" in reason:
                cat = "시간청산"
            elif "드로다운" in reason:
                cat = "드로다운청산"
            elif "일일손실" in reason:
                cat = "일일손실한도"
            else:
                cat = "기타"
            exit_reasons[cat] = exit_reasons.get(cat, 0) + 1

        reason_str = ", ".join(f"{k}={v}" for k, v in sorted(exit_reasons.items(), key=lambda x: -x[1]))

        return (
            f"=== 백테스트 결과 ===\n"
            f"기간: {self.daily_values[0]['date']} ~ {self.daily_values[-1]['date']}\n"
            f"초기 자본: {self.initial_capital:,.0f}원\n"
            f"최종 자산: {self.final_value:,.0f}원\n"
            f"총 수익률: {self.total_return_pct:+.2f}%\n"
            f"최대 드로다운: {self.max_drawdown_pct:.2f}%\n"
            f"거래 횟수: {self.trade_count}회 (매도 {len(sells)}회)\n"
            f"승률: {self.win_rate:.1f}%\n"
            f"평균 손익: {self.avg_pnl:+,.0f}원\n"
            f"손익비: {self.profit_factor:.2f}\n"
            f"샤프 비율: {self.sharpe_ratio:.2f}\n"
            f"청산 사유: {reason_str}\n"
        )


class Backtester:
    """백테스팅 실행기 - MomentumBreakout 전략의 실제 청산 로직 사용"""

    def __init__(self, config: SystemConfig):
        self.config = config
        self.strategy = StrategyEngine(config)
        self.momentum = MomentumBreakout(config.strategy)
        self.risk = RiskManager(config.risk, config.fees)
        self.engine = PaperTradingEngine(config.fees)
        self.store = DataStore(config.db_path)
        self.result = BacktestResult()

    def run(self, universe_data: dict[str, pd.DataFrame], market_df: pd.DataFrame) -> BacktestResult:
        """백테스트 실행

        Args:
            universe_data: {ticker: OHLCV DataFrame} 전체 유니버스 데이터
            market_df: 코스피 지수 OHLCV (레짐 판별용)
        """
        self.result = BacktestResult()
        self.result.initial_capital = self.config.risk.initial_capital
        self.risk = RiskManager(self.config.risk, self.config.fees)
        self.momentum = MomentumBreakout(self.config.strategy)

        # 공통 날짜 인덱스 구하기
        dates = market_df.index.sort_values()
        warmup = max(self.config.strategy.momentum_lookback, self.config.strategy.mean_rev_bb_period) + 20

        prev_value = self.config.risk.initial_capital
        drawdown_cooldown = 0
        COOLDOWN_DAYS = 10
        daily_realized_pnl = 0  # 당일 실현 손익

        for i in range(warmup, len(dates)):
            date = dates[i]
            date_str = date.strftime("%Y-%m-%d")

            # 현재 시점까지의 데이터만 사용 (미래 데이터 누수 방지)
            market_slice = market_df.loc[:date]

            # 레짐 판별
            regime = self.strategy.update_regime(market_slice)

            # 현재 가격/고가/저가/ATR 딕셔너리
            current_prices = {}
            current_highs = {}
            current_lows = {}
            current_atrs = {}
            universe_slice = {}

            for ticker, df in universe_data.items():
                ticker_data = df.loc[:date]
                if len(ticker_data) > warmup and date in df.index:
                    universe_slice[ticker] = ticker_data
                    row = df.loc[date]
                    current_prices[ticker] = row["close"]
                    current_highs[ticker] = row["high"]
                    current_lows[ticker] = row["low"]
                    current_atrs[ticker] = row.get("atr14", 0) or 0

            self.engine.set_current_prices(current_prices)

            # 일일 리스크 체크
            self.risk.reset_daily(current_prices)
            daily_realized_pnl = 0
            self.momentum.reset_daily()

            if drawdown_cooldown > 0:
                drawdown_cooldown -= 1
                if drawdown_cooldown == 0:
                    current_value = self.risk.portfolio.total_value(current_prices)
                    self.risk.portfolio.peak_value = current_value
                # 쿨다운 중에도 기존 포지션의 트레일링/holding_days는 갱신
                self._update_positions(current_highs, current_atrs)
            elif self.risk.check_drawdown(current_prices):
                self._close_all_positions(current_prices, date_str, "드로다운 한도 초과")
                drawdown_cooldown = COOLDOWN_DAYS
            else:
                # 청산 체크 (low 기반 손절 + 트레일링 + 시간청산)
                daily_realized_pnl = self._check_exits(
                    current_prices, current_highs, current_lows, current_atrs, date_str
                )

                # 일일 손실 한도 체크
                if daily_realized_pnl <= -self.momentum.DAILY_LOSS_LIMIT:
                    self._close_all_positions(current_prices, date_str, "일일손실한도 초과")
                elif not self.risk.check_daily_loss(current_prices):
                    signals = self.strategy.generate_signals(universe_slice)
                    self._process_signals(signals, current_prices, current_atrs, date_str)

            # 일일 스냅샷
            total = self.risk.portfolio.total_value(current_prices)
            daily_return = (total - prev_value) / prev_value if prev_value > 0 else 0
            self.result.daily_values.append({
                "date": date_str,
                "total_value": total,
                "cash": self.risk.portfolio.cash,
                "positions_count": len(self.risk.portfolio.positions),
                "daily_return": daily_return,
                "regime": regime.value,
                "drawdown": self.risk.get_drawdown(current_prices) * 100,
            })
            prev_value = total

        self.result.final_value = prev_value
        return self.result

    def _update_positions(self, current_highs: dict, current_atrs: dict):
        """포지션 트레일링 스탑/holding_days만 갱신 (청산 없이)"""
        for ticker in list(self.momentum.positions.keys()):
            mp = self.momentum.positions.get(ticker)
            if mp is None:
                continue
            cur_high = current_highs.get(ticker, mp.highest_since_entry)
            cur_atr = current_atrs.get(ticker, mp.atr_at_entry)
            mp.update(cur_high, cur_atr)

    def _check_exits(self, current_prices: dict, current_highs: dict,
                     current_lows: dict, current_atrs: dict, date_str: str) -> float:
        """MomentumBreakout 청산 로직 사용. 저가 기반 손절 포함.
        Returns: 당일 실현 손익 합계
        """
        realized_pnl = 0
        tickers_to_close = []

        for ticker, pos in list(self.risk.portfolio.positions.items()):
            mp = self.momentum.positions.get(ticker)
            if mp is None:
                continue

            cur_price = current_prices.get(ticker, pos.entry_price)
            cur_high = current_highs.get(ticker, cur_price)
            cur_low = current_lows.get(ticker, cur_price)
            cur_atr = current_atrs.get(ticker, mp.atr_at_entry)

            # 저가가 손절가/트레일링 이하 → 해당 가격으로 체결 (더 현실적)
            if cur_low <= mp.stop_loss:
                # 장중 저가가 손절가 이하 → 손절가로 체결
                loss_pct = (mp.stop_loss - mp.entry_price) / mp.entry_price * 100
                tickers_to_close.append((ticker, f"손절 ({loss_pct:+.1f}%)", mp.stop_loss))
                continue
            elif cur_low <= mp.trailing_stop:
                drop = (mp.highest_since_entry - mp.trailing_stop) / mp.highest_since_entry * 100
                tickers_to_close.append((
                    ticker,
                    f"트레일링스탑 (고점 {mp.highest_since_entry:,.0f} 대비 -{drop:.1f}%)",
                    mp.trailing_stop
                ))
                continue

            # 트레일링 스탑/holding_days 갱신
            exit_reason = self.momentum.check_position_exit(ticker, cur_price, cur_high, cur_atr)
            if exit_reason:
                tickers_to_close.append((ticker, exit_reason, cur_price))

        for ticker, reason, exit_price in tickers_to_close:
            pnl = self._close_position(ticker, current_prices, date_str, reason, exit_price)
            realized_pnl += pnl

        return realized_pnl

    def _close_all_positions(self, current_prices: dict, date_str: str, reason: str):
        tickers = list(self.risk.portfolio.positions.keys())
        for ticker in tickers:
            price = current_prices.get(ticker, 0)
            self._close_position(ticker, current_prices, date_str, reason, price)

    def _close_position(self, ticker: str, current_prices: dict,
                        date_str: str, reason: str, exit_price: float = None) -> float:
        """포지션 청산. Returns PnL."""
        pos = self.risk.portfolio.positions.get(ticker)
        if pos is None:
            return 0

        price = exit_price or current_prices.get(ticker, pos.entry_price)
        order = Order(ticker=ticker, side=OrderSide.SELL, price=price,
                      quantity=pos.quantity, strategy=pos.strategy, memo=reason)
        result = self.engine.execute_order(order)

        pnl = 0
        if result.success:
            pnl = (result.filled_price - pos.entry_price) * pos.quantity - result.total_cost
            self.risk.portfolio.cash += result.filled_price * result.filled_quantity - result.total_cost
            del self.risk.portfolio.positions[ticker]

            # MomentumBreakout 포지션도 정리
            self.momentum.on_position_closed(ticker, result.filled_price, pnl)

            self.result.trades.append({
                "timestamp": date_str, "ticker": ticker, "side": "sell",
                "price": result.filled_price, "quantity": result.filled_quantity,
                "amount": result.filled_price * result.filled_quantity,
                "commission": result.commission, "tax": result.tax,
                "slippage": result.slippage, "pnl": pnl,
                "strategy": pos.strategy, "memo": reason,
            })

        return pnl

    def _process_signals(self, signals, current_prices: dict,
                         current_atrs: dict, date_str: str):
        for signal in signals:
            if signal.action == "buy" and self.risk.can_open_position():
                if signal.ticker in self.risk.portfolio.positions:
                    continue

                qty = self.risk.calculate_position_size(signal.price, signal.stop_loss)
                if qty <= 0:
                    continue

                strategy_name = "momentum" if "고점" in signal.reason or "돌파" in signal.reason else "mean_rev"
                order = Order(
                    ticker=signal.ticker, side=OrderSide.BUY,
                    price=signal.price, quantity=qty,
                    strategy=strategy_name, memo=signal.reason,
                )
                result = self.engine.execute_order(order)

                if result.success:
                    self.risk.portfolio.cash -= result.net_amount
                    self.risk.portfolio.positions[signal.ticker] = Position(
                        ticker=signal.ticker,
                        entry_price=result.filled_price,
                        quantity=result.filled_quantity,
                        stop_loss=signal.stop_loss,
                        take_profit=signal.take_profit,
                        strategy=order.strategy,
                        entry_date=date_str,
                    )

                    # MomentumBreakout에도 포지션 등록 (트레일링 추적)
                    atr_val = current_atrs.get(signal.ticker, 0)
                    self.momentum.on_new_position(
                        signal.ticker, result.filled_price, date_str,
                        signal.stop_loss, atr_val,
                    )

                    self.result.trades.append({
                        "timestamp": date_str, "ticker": signal.ticker, "side": "buy",
                        "price": result.filled_price, "quantity": result.filled_quantity,
                        "amount": result.filled_price * result.filled_quantity,
                        "commission": result.commission, "tax": result.tax,
                        "slippage": result.slippage, "pnl": 0,
                        "strategy": order.strategy, "memo": signal.reason,
                    })

            elif signal.action == "sell" and signal.ticker in self.risk.portfolio.positions:
                price = current_prices.get(signal.ticker, 0)
                self._close_position(signal.ticker, current_prices, date_str, signal.reason, price)
