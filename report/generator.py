"""일일/주간 리포트 생성 (마크다운)

- 일일: logs/daily/YYYY-MM-DD.md
- 주간: logs/weekly/YYYY-Wnn.md
"""
import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from ..config import SystemConfig, RiskConfig, FeeConfig, StrategyConfig
from ..data.collector import MarketDataCollector, UNIVERSE
from ..strategy.momentum import MomentumBreakout

logger = logging.getLogger(__name__)


class ReportGenerator:
    """일일/주간 마크다운 리포트 생성"""

    def __init__(
        self,
        config: SystemConfig,
        logs_dir: str = "auto/logs",
        market_db: str = "auto/logs/market.db",
        trades_db: str = "auto/logs/trades.db",
    ):
        self.config = config
        self.logs_dir = Path(logs_dir)
        self.market_db = market_db
        self.trades_db = trades_db

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  일일 리포트
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    def generate_daily(
        self,
        date: str,
        portfolio: dict,
        positions: list[dict],
        trades_today: list[dict],
        market: dict,
        screening: list[dict],
    ) -> str:
        """일일 리포트 생성 및 저장

        Args:
            date: "YYYY-MM-DD"
            portfolio: {total_value, cash, prev_value, cumulative_return, drawdown}
            positions: [{ticker, name, entry_price, current_price, pnl_pct, holding_days,
                         stop_loss, trailing_stop, highest_since_entry}, ...]
            trades_today: [{side, ticker, name, reason, pnl_pct}, ...]
            market: {kospi_close, kospi_change_pct, regime, adx, volume_ratio}
            screening: [{ticker, name, signal, entry_price, stop_loss, breakout_level}, ...]

        Returns:
            생성된 마크다운 텍스트
        """
        # 일변 수익률
        daily_change = 0
        if portfolio.get("prev_value", 0) > 0:
            daily_change = (portfolio["total_value"] - portfolio["prev_value"]) / portfolio["prev_value"] * 100

        # 최근 거래 통계
        recent_stats = self._get_recent_trade_stats(date)

        md = f"""# 일일 리포트 {date}

## 포트폴리오 현황
- 총 자산: {portfolio['total_value']:,.0f}원 (전일 대비 {daily_change:+.1f}%)
- 현금: {portfolio['cash']:,.0f}원
- 누적 수익률: {portfolio.get('cumulative_return', 0):+.1f}%
- 현재 드로다운: {portfolio.get('drawdown', 0):-.1f}%

## 보유 종목
"""
        if positions:
            md += "| 종목 | 매수가 | 현재가 | 수익률 | 손절가 | 트레일링 | 보유일 | 상태 |\n"
            md += "|------|--------|--------|--------|--------|----------|--------|------|\n"
            for p in positions:
                # 상태 판단
                cur = p["current_price"]
                sl = p.get("stop_loss", 0)
                trail = p.get("trailing_stop", 0)
                days = p.get("holding_days", 0)
                status = "보유"
                if sl > 0 and cur <= sl:
                    status = "⚠️ 손절가 이탈"
                elif trail > 0 and cur <= trail:
                    status = "⚠️ 트레일링 이탈"
                elif days >= 15:
                    status = "⚠️ 시간청산 대상"
                elif sl > 0 and cur < sl * 1.02:
                    status = "⚡ 손절 임박"

                md += (
                    f"| {p['name']} | {p['entry_price']:,.0f} | {cur:,.0f} "
                    f"| {p['pnl_pct']:+.1f}% | {sl:,.0f} | {trail:,.0f} "
                    f"| {days}일 | {status} |\n"
                )

            # 리스크 경고
            alerts = []
            for p in positions:
                cur = p["current_price"]
                sl = p.get("stop_loss", 0)
                trail = p.get("trailing_stop", 0)
                if sl > 0 and cur <= sl:
                    loss_pct = (cur - p["entry_price"]) / p["entry_price"] * 100
                    alerts.append(
                        f"- ❌ **{p['name']}**: 현재가 {cur:,.0f} < 손절가 {sl:,.0f} "
                        f"(손실 {loss_pct:+.1f}%) → 즉시 손절 필요"
                    )
                elif trail > 0 and cur <= trail:
                    alerts.append(
                        f"- ❌ **{p['name']}**: 현재가 {cur:,.0f} < 트레일링 {trail:,.0f} → 트레일링 청산 필요"
                    )
            if alerts:
                md += "\n### ⚠️ 리스크 경고\n"
                md += "\n".join(alerts) + "\n"
        else:
            md += "- 보유 종목 없음\n"

        # 오늘 거래
        md += "\n## 오늘 거래\n"
        buys = [t for t in trades_today if t["side"] == "buy"]
        sells = [t for t in trades_today if t["side"] == "sell"]

        if buys:
            for t in buys:
                md += f"- 매수: {t['name']} ({t.get('reason', '')})\n"
        else:
            md += "- 매수: 없음\n"

        if sells:
            for t in sells:
                pnl = f"{t['pnl_pct']:+.1f}%" if 'pnl_pct' in t else ""
                md += f"- 매도: {t['name']} ({t.get('reason', '')}, {pnl})\n"
        else:
            md += "- 매도: 없음\n"

        # 미매매 사유 (보유 종목이 있는데 매도가 없는 경우)
        if positions and not sells:
            md += "\n### 미매매 사유\n"
            for p in positions:
                cur = p["current_price"]
                sl = p.get("stop_loss", 0)
                trail = p.get("trailing_stop", 0)
                days = p.get("holding_days", 0)
                if sl > 0 and cur <= sl:
                    md += f"- **{p['name']}**: 손절가({sl:,.0f}) 이탈 상태이나 매도 미체결 (장중 모니터링 미실행)\n"
                elif trail > 0 and cur <= trail:
                    md += f"- **{p['name']}**: 트레일링({trail:,.0f}) 이탈 상태이나 매도 미체결\n"
                elif days >= 15:
                    md += f"- **{p['name']}**: 시간청산 대상({days}일)이나 매도 미체결\n"
                else:
                    reason_parts = []
                    if sl > 0:
                        sl_dist = (cur - sl) / sl * 100
                        reason_parts.append(f"손절가까지 {sl_dist:.1f}%")
                    if days > 0:
                        reason_parts.append(f"보유 {days}일/{15}일")
                    md += f"- **{p['name']}**: 청산 조건 미충족 ({', '.join(reason_parts)})\n"

        # 시장 상태
        regime_kr = {"trending": "추세", "mean_reverting": "횡보", "high_volatility": "고변동성"}
        md += f"""
## 시장 상태
- 코스피: {market.get('kospi_close', 0):,.0f} ({market.get('kospi_change_pct', 0):+.1f}%)
- 레짐: {regime_kr.get(market.get('regime', ''), market.get('regime', '-'))} (ADX: {market.get('adx', 0):.0f})
- 거래량: 평균 대비 {market.get('volume_ratio', 0)*100:.0f}%
"""

        # 스크리닝
        md += "\n## 스크리닝 결과 (내일 관심 종목)\n"
        if screening:
            md += "| 종목 | 신호 | 돌파 레벨 | 예상 손절가 |\n"
            md += "|------|------|----------|------------|\n"
            for s in screening:
                # 예상 진입가 = 돌파 레벨 (20일 고가)
                breakout = s.get("breakout_level", s["entry_price"])
                md += f"| {s['name']} | {s['signal']} | {breakout:,.0f} | {s['stop_loss']:,.0f} |\n"
        else:
            md += "- 관심 종목 없음\n"

        # 주요 지표
        md += f"""
## 주요 지표
- 이번 주 승률: {recent_stats['week_wins']}/{recent_stats['week_total']} ({recent_stats['week_winrate']:.0f}%)
- 최근 10거래 손익비: {recent_stats['profit_factor']:.1f}
"""

        # 저장
        daily_dir = self.logs_dir / "daily"
        daily_dir.mkdir(parents=True, exist_ok=True)
        fpath = daily_dir / f"{date}.md"
        fpath.write_text(md, encoding="utf-8")
        logger.info(f"일일 리포트 저장: {fpath}")

        return md

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  주간 리포트
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    def generate_weekly(
        self,
        week_label: str,
        week_start: str,
        week_end: str,
        portfolio: dict,
        param_changes: list[dict] = None,
    ) -> str:
        """주간 리포트 생성 및 저장

        Args:
            week_label: "YYYY-Wnn"
            week_start, week_end: "YYYY-MM-DD"
            portfolio: {total_value, initial_capital, cumulative_return, max_drawdown}
            param_changes: [{date, param, old, new, reason}, ...]
        """
        trades = self._load_trades(week_start, week_end)
        snapshots = self._load_snapshots(week_start, week_end)

        # 주간 수익률
        if not snapshots.empty:
            start_val = snapshots.iloc[0]["total_value"]
            end_val = snapshots.iloc[-1]["total_value"]
            week_return = (end_val - start_val) / start_val * 100
        else:
            start_val = end_val = portfolio.get("total_value", 0)
            week_return = 0

        # 거래 통계
        sells = trades[trades["side"] == "sell"] if not trades.empty else pd.DataFrame()
        buy_count = len(trades[trades["side"] == "buy"]) if not trades.empty else 0
        sell_count = len(sells)
        wins = len(sells[sells["pnl"] > 0]) if not sells.empty else 0
        losses = sell_count - wins
        winrate = (wins / sell_count * 100) if sell_count > 0 else 0
        total_pnl = sells["pnl"].sum() if not sells.empty else 0

        avg_win = sells[sells["pnl"] > 0]["pnl"].mean() if wins > 0 else 0
        avg_loss = abs(sells[sells["pnl"] <= 0]["pnl"].mean()) if losses > 0 else 0
        profit_factor = (avg_win / avg_loss) if avg_loss > 0 else 0

        md = f"""# 주간 리포트 {week_label}
> {week_start} ~ {week_end}

## 주간 성과 요약
- 시작 자산: {start_val:,.0f}원
- 종료 자산: {end_val:,.0f}원
- 주간 수익률: {week_return:+.1f}%
- 누적 수익률: {portfolio.get('cumulative_return', 0):+.1f}%
- 최대 드로다운: {portfolio.get('max_drawdown', 0):.1f}%

## 거래 통계
- 매수: {buy_count}건, 매도: {sell_count}건
- 승률: {wins}승 {losses}패 ({winrate:.0f}%)
- 총 실현 손익: {total_pnl:+,.0f}원
- 평균 수익: {avg_win:+,.0f}원 / 평균 손실: {-avg_loss:,.0f}원
- 손익비: {profit_factor:.2f}
"""

        # 전략별 성과
        md += "\n## 전략별 성과\n"
        if not sells.empty and "strategy" in sells.columns:
            md += "| 전략 | 거래수 | 승률 | 총 PnL | 평균 PnL |\n"
            md += "|------|--------|------|--------|----------|\n"
            for strat, grp in sells.groupby("strategy"):
                s_wins = (grp["pnl"] > 0).sum()
                s_total = len(grp)
                s_wr = s_wins / s_total * 100 if s_total > 0 else 0
                md += (
                    f"| {strat} | {s_total} | {s_wr:.0f}% "
                    f"| {grp['pnl'].sum():+,.0f}원 | {grp['pnl'].mean():+,.0f}원 |\n"
                )
        else:
            md += "- 이번 주 청산 거래 없음\n"

        # 청산 사유 통계
        md += "\n## 청산 사유 통계\n"
        if not sells.empty and "memo" in sells.columns:
            md += "| 사유 | 건수 | 총 PnL | 평균 PnL |\n"
            md += "|------|------|--------|----------|\n"
            for memo, grp in sells.groupby("memo"):
                # 사유를 간결하게 (앞부분만)
                short_memo = memo.split("(")[0].strip() if memo else "-"
                md += f"| {short_memo} | {len(grp)} | {grp['pnl'].sum():+,.0f}원 | {grp['pnl'].mean():+,.0f}원 |\n"
        else:
            md += "- 청산 거래 없음\n"

        # 파라미터 변경 이력
        md += "\n## 파라미터 변경 이력\n"
        if param_changes:
            md += "| 날짜 | 파라미터 | 이전 | 변경 | 사유 |\n"
            md += "|------|----------|------|------|------|\n"
            for ch in param_changes:
                md += f"| {ch['date']} | {ch['param']} | {ch['old']} | {ch['new']} | {ch.get('reason','-')} |\n"
        else:
            md += "- 이번 주 변경 없음\n"

        # 현재 주요 파라미터
        rc = self.config.risk
        fc = self.config.fees
        mom = MomentumBreakout
        md += f"""
## 현재 주요 파라미터
### 리스크
| 파라미터 | 값 |
|----------|----|
| 자본금 | {rc.initial_capital:,}원 |
| 최대 드로다운 | {rc.max_drawdown_pct*100:.0f}% |
| 종목당 최대 손실 | {rc.per_stock_max_loss:,}원 |
| 일일 손실 한도 | {rc.daily_loss_limit:,}원 |
| 종목당 최대 비중 | {rc.max_position_pct*100:.0f}% |
| 동시 보유 한도 | {rc.max_positions}종목 |

### 모멘텀 전략
| 파라미터 | 값 |
|----------|----|
| 돌파 기간 | {mom.LOOKBACK}일 |
| ADX 문턱 | {mom.ADX_THRESHOLD} |
| 거래량 배수 | {mom.VOLUME_MULT}x |
| 손절 (%) | {mom.STOP_PCT*100:.0f}% |
| 손절 (ATR) | ATR x {mom.ATR_STOP_MULT} |
| 트레일링 | 고점 - ATR x {mom.TRAILING_ATR_MULT} |
| 시간 청산 | {mom.TIME_EXIT_DAYS}일 |

### 수수료
| 항목 | 값 |
|------|----|
| 매수 수수료 | {fc.buy_commission*100:.3f}% |
| 매도 수수료 | {fc.sell_commission*100:.3f}% |
| 세금 | {fc.tax_rate*100:.2f}% |
| 슬리피지 | {fc.slippage_pct*100:.1f}% |
"""

        # 저장
        weekly_dir = self.logs_dir / "weekly"
        weekly_dir.mkdir(parents=True, exist_ok=True)
        fpath = weekly_dir / f"{week_label}.md"
        fpath.write_text(md, encoding="utf-8")
        logger.info(f"주간 리포트 저장: {fpath}")

        return md

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  백테스트 결과로 전체 기간 리포트 일괄 생성
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    def generate_from_backtest(
        self,
        daily_values: list[dict],
        trades: list[dict],
        universe_data: dict = None,
    ) -> dict:
        """백테스트 daily_values + trades로 일일/주간 리포트 일괄 생성

        Returns: {"daily_count": n, "weekly_count": n}
        """
        if not daily_values:
            return {"daily_count": 0, "weekly_count": 0}

        trades_df = pd.DataFrame(trades) if trades else pd.DataFrame()
        daily_count = 0
        weekly_count = 0

        # 일별 리포트
        prev_value = daily_values[0]["total_value"]
        initial_capital = self.config.risk.initial_capital

        for dv in daily_values:
            date = dv["date"]

            portfolio = {
                "total_value": dv["total_value"],
                "cash": dv["cash"],
                "prev_value": prev_value,
                "cumulative_return": (dv["total_value"] - initial_capital) / initial_capital * 100,
                "drawdown": dv.get("drawdown", 0),
            }

            # 오늘 거래
            today_trades = []
            if not trades_df.empty:
                day_t = trades_df[trades_df["timestamp"] == date]
                for _, t in day_t.iterrows():
                    pnl_pct = 0
                    if t["side"] == "sell" and t.get("price", 0) > 0:
                        # 매도의 경우 memo에서 정보 추출
                        pnl_pct = t.get("pnl", 0) / max(t["amount"], 1) * 100
                    today_trades.append({
                        "side": t["side"],
                        "ticker": t["ticker"],
                        "name": UNIVERSE.get(t["ticker"], t["ticker"]),
                        "reason": t.get("memo", ""),
                        "pnl_pct": pnl_pct,
                    })

            # 시장 데이터 (간략)
            market = {
                "kospi_close": 0,
                "kospi_change_pct": 0,
                "regime": dv.get("regime", ""),
                "adx": 0,
                "volume_ratio": 1.0,
            }

            self.generate_daily(
                date=date,
                portfolio=portfolio,
                positions=[],  # 백테스트에서 당일 보유 종목 상세는 생략
                trades_today=today_trades,
                market=market,
                screening=[],
            )
            daily_count += 1
            prev_value = dv["total_value"]

        # 주간 리포트 (금요일마다)
        dates = [datetime.strptime(d["date"], "%Y-%m-%d") for d in daily_values]
        weeks_done = set()

        for dt in dates:
            iso_year, iso_week, iso_day = dt.isocalendar()
            week_label = f"{iso_year}-W{iso_week:02d}"
            if week_label in weeks_done:
                continue
            if iso_day < 5 and dt != dates[-1]:
                continue  # 금요일이 아니면 스킵 (마지막 날 제외)

            # 주의 월~금 범위
            week_start_dt = dt - timedelta(days=dt.weekday())
            week_end_dt = week_start_dt + timedelta(days=4)
            week_start = week_start_dt.strftime("%Y-%m-%d")
            week_end = week_end_dt.strftime("%Y-%m-%d")

            # 해당 주의 마지막 daily_value
            week_dv = [d for d in daily_values if week_start <= d["date"] <= week_end]
            if not week_dv:
                continue

            last_dv = week_dv[-1]
            portfolio = {
                "total_value": last_dv["total_value"],
                "initial_capital": initial_capital,
                "cumulative_return": (last_dv["total_value"] - initial_capital) / initial_capital * 100,
                "max_drawdown": max(d.get("drawdown", 0) for d in week_dv),
            }

            self.generate_weekly(week_label, week_start, week_end, portfolio)
            weeks_done.add(week_label)
            weekly_count += 1

        return {"daily_count": daily_count, "weekly_count": weekly_count}

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  내부 유틸
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    def _load_trades(self, start: str, end: str) -> pd.DataFrame:
        try:
            with sqlite3.connect(self.trades_db) as conn:
                return pd.read_sql_query(
                    "SELECT * FROM trades WHERE timestamp BETWEEN ? AND ? ORDER BY timestamp",
                    conn, params=[start, end],
                )
        except Exception:
            return pd.DataFrame()

    def _load_snapshots(self, start: str, end: str) -> pd.DataFrame:
        try:
            with sqlite3.connect(self.trades_db) as conn:
                return pd.read_sql_query(
                    "SELECT * FROM portfolio_snapshots WHERE date BETWEEN ? AND ? ORDER BY date",
                    conn, params=[start, end],
                )
        except Exception:
            return pd.DataFrame()

    def _get_recent_trade_stats(self, date: str) -> dict:
        """최근 거래 통계 (이번 주 승률, 최근 10거래 손익비)"""
        result = {"week_wins": 0, "week_total": 0, "week_winrate": 0, "profit_factor": 0}

        try:
            dt = datetime.strptime(date, "%Y-%m-%d")
            week_start = (dt - timedelta(days=dt.weekday())).strftime("%Y-%m-%d")

            with sqlite3.connect(self.trades_db) as conn:
                # 이번 주 매도 거래
                week_sells = pd.read_sql_query(
                    "SELECT pnl FROM trades WHERE side='sell' AND timestamp BETWEEN ? AND ? ORDER BY timestamp",
                    conn, params=[week_start, date],
                )
                if not week_sells.empty:
                    result["week_wins"] = int((week_sells["pnl"] > 0).sum())
                    result["week_total"] = len(week_sells)
                    result["week_winrate"] = result["week_wins"] / result["week_total"] * 100

                # 최근 10건 매도
                recent = pd.read_sql_query(
                    "SELECT pnl FROM trades WHERE side='sell' AND timestamp <= ? ORDER BY timestamp DESC LIMIT 10",
                    conn, params=[date],
                )
                if not recent.empty:
                    avg_win = recent[recent["pnl"] > 0]["pnl"].mean() if (recent["pnl"] > 0).any() else 0
                    avg_loss = abs(recent[recent["pnl"] <= 0]["pnl"].mean()) if (recent["pnl"] <= 0).any() else 0
                    result["profit_factor"] = avg_win / avg_loss if avg_loss > 0 else 0

        except Exception:
            pass

        return result
