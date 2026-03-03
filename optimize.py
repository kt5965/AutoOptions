#!/usr/bin/env python3
"""전략 파라미터 최적화 (고속)

1단계: 모든 종목의 일별 지표를 미리 계산 (1회)
2단계: 파라미터별로 진입/청산 필터링만 반복 (빠름)
"""
import sys
import os
import json
import time
import itertools
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
from auto.config import SystemConfig, FeeConfig
from auto.data.collector import MarketDataCollector

LOGS_DIR = Path(__file__).parent / "logs"

# ─── 1단계: 지표 사전 계산 ─────────────────────────


def precompute_indicators(universe: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """모든 종목에 대해 필요한 지표를 미리 계산"""
    result = {}
    for ticker, df in universe.items():
        if len(df) < 60:
            continue
        d = df.copy()
        d["ma5"] = d["close"].rolling(5).mean()
        d["ma20"] = d["close"].rolling(20).mean()
        d["vol_ma20"] = d["volume"].astype(float).rolling(20).mean()
        d["vol_ratio"] = d["volume"].astype(float) / d["vol_ma20"]

        # N일 고가 (당일 제외) - 여러 lookback 미리 계산
        for lb in [10, 15, 20, 30, 40, 60]:
            d[f"prev_high_{lb}"] = d["high"].shift(1).rolling(lb).max()

        # ADX, ATR은 이미 DB에 있으면 사용
        if "atr14" not in d.columns or d["atr14"].isna().all():
            # ATR 계산
            tr = pd.concat([
                d["high"] - d["low"],
                (d["high"] - d["close"].shift(1)).abs(),
                (d["low"] - d["close"].shift(1)).abs()
            ], axis=1).max(axis=1)
            d["atr14"] = tr.ewm(span=14, adjust=False).mean()

        if "adx14" not in d.columns or d["adx14"].isna().all():
            # ADX 계산
            tr = pd.concat([
                d["high"] - d["low"],
                (d["high"] - d["close"].shift(1)).abs(),
                (d["low"] - d["close"].shift(1)).abs()
            ], axis=1).max(axis=1)
            atr14 = tr.ewm(span=14, adjust=False).mean()
            up = d["high"] - d["high"].shift(1)
            down = d["low"].shift(1) - d["low"]
            plus_dm = pd.Series(np.where((up > down) & (up > 0), up, 0), index=d.index)
            minus_dm = pd.Series(np.where((down > up) & (down > 0), down, 0), index=d.index)
            plus_di = 100 * plus_dm.ewm(span=14, adjust=False).mean() / atr14
            minus_di = 100 * minus_dm.ewm(span=14, adjust=False).mean() / atr14
            dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
            d["adx14"] = dx.ewm(span=14, adjust=False).mean()

        result[ticker] = d
    return result


# ─── 2단계: 고속 백테스트 ──────────────────────────


def fast_backtest(indicators: dict[str, pd.DataFrame],
                  market_df: pd.DataFrame,
                  params: dict) -> dict:
    """지표 사전 계산 결과를 사용한 고속 백테스트"""

    # 파라미터 추출
    lookback = params["lookback"]
    adx_thresh = params["adx_threshold"]
    vol_mult = params["volume_mult"]
    stop_pct = params["stop_pct"]
    atr_stop_mult = params["atr_stop_mult"]
    trail_atr_mult = params["trailing_atr_mult"]
    time_exit = params["time_exit_days"]
    max_positions = params["max_positions"]
    daily_loss_limit = params["daily_loss_limit"]
    max_loss_per_trade = params["max_loss_per_trade"]
    max_pos_pct = params["max_position_pct"]

    # 수수료
    buy_fee = 0.00015 + 0.001  # 수수료 + 슬리피지
    sell_fee = 0.00015 + 0.0018 + 0.001  # 수수료 + 세금 + 슬리피지

    capital = 10_000_000
    cash = capital
    peak = capital
    max_dd = 0

    # 포지션: {ticker: {entry_price, stop_loss, trailing_stop, highest, holding_days, qty, atr}}
    positions = {}
    trades = []
    daily_values = []

    dates = market_df.index.sort_values()
    high_col = f"prev_high_{lookback}"

    prev_value = capital

    for i in range(60, len(dates)):
        date = dates[i]
        date_str = date.strftime("%Y-%m-%d")

        daily_pnl = 0

        # 현재 가격 수집
        current_data = {}
        for ticker, df in indicators.items():
            if date in df.index:
                row = df.loc[date]
                if pd.notna(row.get("adx14")) and pd.notna(row.get("atr14")):
                    current_data[ticker] = row

        # 포트폴리오 평가
        pos_value = 0
        for t, p in positions.items():
            if t in current_data:
                pos_value += current_data[t]["close"] * p["qty"]
            else:
                pos_value += p["entry_price"] * p["qty"]
        total_value = cash + pos_value

        # ── 청산 체크 ──
        to_close = []
        for ticker in list(positions.keys()):
            pos = positions[ticker]
            if ticker not in current_data:
                continue

            row = current_data[ticker]
            cur_price = row["close"]
            cur_high = row["high"]
            cur_low = row["low"]
            cur_atr = row["atr14"]

            pos["holding_days"] += 1

            # 고점 갱신 + 트레일링
            if cur_high > pos["highest"]:
                pos["highest"] = cur_high
                new_trail = pos["highest"] - trail_atr_mult * cur_atr
                if new_trail > pos["trailing_stop"]:
                    pos["trailing_stop"] = new_trail

            # 저가 기반 손절 체크
            if cur_low <= pos["stop_loss"]:
                to_close.append((ticker, pos["stop_loss"], "손절"))
            elif cur_low <= pos["trailing_stop"]:
                to_close.append((ticker, pos["trailing_stop"], "트레일링스탑"))
            elif pos["holding_days"] >= time_exit:
                to_close.append((ticker, cur_price, "시간청산"))

        for ticker, exit_price, reason in to_close:
            pos = positions.pop(ticker)
            proceeds = exit_price * pos["qty"] * (1 - sell_fee)
            cost = pos["entry_price"] * pos["qty"] * (1 + buy_fee)
            pnl = proceeds - cost
            cash += proceeds
            daily_pnl += pnl
            trades.append({
                "timestamp": date_str, "ticker": ticker, "side": "sell",
                "price": exit_price, "quantity": pos["qty"],
                "pnl": pnl, "memo": reason,
            })

        # 일일 손실 한도 체크 → 전체 청산
        if daily_pnl <= -daily_loss_limit:
            for ticker in list(positions.keys()):
                pos = positions.pop(ticker)
                if ticker in current_data:
                    exit_price = current_data[ticker]["close"]
                else:
                    exit_price = pos["entry_price"]
                proceeds = exit_price * pos["qty"] * (1 - sell_fee)
                cost = pos["entry_price"] * pos["qty"] * (1 + buy_fee)
                pnl = proceeds - cost
                cash += proceeds
                daily_pnl += pnl
                trades.append({
                    "timestamp": date_str, "ticker": ticker, "side": "sell",
                    "price": exit_price, "quantity": pos["qty"],
                    "pnl": pnl, "memo": "일일손실한도",
                })

        # ── 진입 체크 ──
        if len(positions) < max_positions and daily_pnl > -daily_loss_limit:
            candidates = []
            for ticker, row in current_data.items():
                if ticker in positions:
                    continue
                if ticker == "069500":  # 지수 ETF 제외
                    continue

                prev_high = row.get(high_col)
                if pd.isna(prev_high):
                    continue

                cur_high_val = row["high"]
                cur_adx = row["adx14"]
                vol_ratio = row.get("vol_ratio", 0)
                cur_ma5 = row.get("ma5", 0)
                cur_ma20 = row.get("ma20", 0)

                if pd.isna(vol_ratio) or pd.isna(cur_ma5) or pd.isna(cur_ma20):
                    continue

                # 진입 조건
                if (cur_high_val > prev_high and
                    cur_adx > adx_thresh and
                    vol_ratio > vol_mult and
                    cur_ma5 > cur_ma20):

                    strength = min((cur_adx - adx_thresh) / 30, 1.0) * min(vol_ratio / 3, 1.0)
                    candidates.append((ticker, row, strength))

            # 강도순 정렬
            candidates.sort(key=lambda x: x[2], reverse=True)

            for ticker, row, strength in candidates:
                if len(positions) >= max_positions:
                    break

                cur_price = row["close"]
                cur_atr = row["atr14"]

                # 손절가
                stop_by_pct = cur_price * (1 - stop_pct)
                stop_by_atr = cur_price - atr_stop_mult * cur_atr
                stop_loss = max(stop_by_pct, stop_by_atr)

                # 포지션 사이징
                risk_per_share = cur_price - stop_loss
                if risk_per_share <= 0:
                    continue

                qty_risk = int(max_loss_per_trade / risk_per_share)
                qty_weight = int(total_value * max_pos_pct / cur_price)
                qty_cash = int(cash / (cur_price * (1 + buy_fee)))
                qty = min(qty_risk, qty_weight, qty_cash)
                if qty <= 0:
                    continue

                cost = cur_price * qty * (1 + buy_fee)
                if cost > cash:
                    continue

                cash -= cost
                trailing = cur_price - trail_atr_mult * cur_atr
                positions[ticker] = {
                    "entry_price": cur_price,
                    "stop_loss": stop_loss,
                    "trailing_stop": trailing,
                    "highest": cur_price,
                    "holding_days": 0,
                    "qty": qty,
                    "atr": cur_atr,
                }
                trades.append({
                    "timestamp": date_str, "ticker": ticker, "side": "buy",
                    "price": cur_price, "quantity": qty,
                    "pnl": 0, "memo": f"ADX={cur_adx:.0f} VOL={vol_ratio:.1f}x",
                })

        # 일일 스냅샷
        pos_value = sum(
            current_data.get(t, {}).get("close", p["entry_price"]) * p["qty"]
            if isinstance(current_data.get(t), pd.Series) else p["entry_price"] * p["qty"]
            for t, p in positions.items()
        )
        # 더 안전한 평가
        pos_value = 0
        for t, p in positions.items():
            if t in current_data:
                pos_value += current_data[t]["close"] * p["qty"]
            else:
                pos_value += p["entry_price"] * p["qty"]

        total_value = cash + pos_value
        daily_return = (total_value - prev_value) / prev_value if prev_value > 0 else 0

        peak = max(peak, total_value)
        dd = (peak - total_value) / peak if peak > 0 else 0
        max_dd = max(max_dd, dd)

        daily_values.append({
            "date": date_str,
            "total_value": total_value,
            "daily_return": daily_return,
        })
        prev_value = total_value

    # 결과 계산
    sells = [t for t in trades if t["side"] == "sell"]
    wins = sum(1 for t in sells if t["pnl"] > 0)
    win_rate = (wins / len(sells) * 100) if sells else 0

    gross_profit = sum(t["pnl"] for t in sells if t["pnl"] > 0)
    gross_loss = abs(sum(t["pnl"] for t in sells if t["pnl"] < 0))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else (float("inf") if gross_profit > 0 else 0)

    avg_pnl = (sum(t["pnl"] for t in sells) / len(sells)) if sells else 0

    returns = pd.Series([d["daily_return"] for d in daily_values])
    sharpe = (returns.mean() / returns.std() * (252**0.5)) if returns.std() > 0 else 0

    final_value = daily_values[-1]["total_value"] if daily_values else capital
    total_return = (final_value - capital) / capital * 100

    # 청산 사유 분포
    exit_reasons = {}
    for t in sells:
        r = t.get("memo", "기타")
        exit_reasons[r] = exit_reasons.get(r, 0) + 1

    return {
        "total_return": total_return,
        "max_drawdown": max_dd * 100,
        "sharpe": sharpe,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "trade_count": len(sells),
        "avg_pnl": avg_pnl,
        "final_value": final_value,
        "exit_reasons": exit_reasons,
    }


def optimize():
    # 데이터 로드
    collector = MarketDataCollector(str(LOGS_DIR / "market.db"))
    start, end = "20210303", "20260303"

    print("데이터 로딩...")
    market_df = collector.load_from_db("069500", start, end)
    universe_raw = collector.load_all_from_db(start, end)
    print(f"  마켓: {len(market_df)}일, 유니버스: {len(universe_raw)}종목")

    print("지표 사전 계산...")
    t0 = time.time()
    indicators = precompute_indicators(universe_raw)
    print(f"  완료: {time.time()-t0:.1f}초, {len(indicators)}종목\n")

    # 단일 벤치마크
    print("벤치마크 (현재 파라미터)...")
    current_params = {
        "lookback": 40, "adx_threshold": 20, "volume_mult": 2.0,
        "stop_pct": 0.05, "atr_stop_mult": 3.0, "trailing_atr_mult": 2.5,
        "time_exit_days": 15, "max_positions": 2, "daily_loss_limit": 50_000,
        "max_loss_per_trade": 30_000, "max_position_pct": 0.60,
    }
    t0 = time.time()
    bench = fast_backtest(indicators, market_df, current_params)
    bench_time = time.time() - t0
    print(f"  1회 소요: {bench_time:.2f}초")
    print(f"  현재: ret={bench['total_return']:+.1f}% dd={bench['max_drawdown']:.1f}% "
          f"sharpe={bench['sharpe']:.2f} wr={bench['win_rate']:.0f}% trades={bench['trade_count']}\n")

    # ─── 파라미터 그리드 ────────────────────────────
    grid = {
        "lookback":          [10, 15, 20, 30, 40, 60],
        "adx_threshold":     [15, 20, 25, 30],
        "volume_mult":       [1.3, 1.5, 2.0, 2.5],
        "stop_pct":          [0.03, 0.05, 0.07, 0.10],
        "atr_stop_mult":     [1.5, 2.0, 2.5, 3.0],
        "trailing_atr_mult": [1.5, 2.0, 2.5, 3.0],
        "time_exit_days":    [7, 10, 15, 20, 30],
    }

    fixed = {
        "max_positions": 3,
        "daily_loss_limit": 50_000,
        "max_loss_per_trade": 30_000,
        "max_position_pct": 0.30,
    }

    keys = list(grid.keys())
    values = list(grid.values())
    combos = list(itertools.product(*values))
    total = len(combos)
    est_time = total * bench_time
    print(f"=== 그리드 서치 ===")
    print(f"조합: {total}개, 예상 소요: {est_time/60:.0f}분")
    print(f"파라미터: {', '.join(keys)}\n")

    results = []
    t_start = time.time()

    for idx, combo in enumerate(combos):
        params = dict(zip(keys, combo))
        params.update(fixed)

        try:
            r = fast_backtest(indicators, market_df, params)
            r["params"] = params
            results.append(r)
        except Exception as e:
            pass

        if (idx + 1) % 500 == 0:
            elapsed = time.time() - t_start
            eta = elapsed / (idx + 1) * (total - idx - 1)
            best_so_far = max(results, key=lambda x: x["sharpe"]) if results else None
            best_str = f"best sharpe={best_so_far['sharpe']:.2f} ret={best_so_far['total_return']:+.1f}%" if best_so_far else ""
            print(f"  [{idx+1}/{total}] {elapsed:.0f}초 경과, ETA {eta:.0f}초 | {best_str}")

    elapsed = time.time() - t_start
    print(f"\n완료: {elapsed:.0f}초 ({elapsed/60:.1f}분)")

    if not results:
        print("결과 없음")
        return

    # ─── 결과 정렬 ──────────────────────────────────
    # 필터: 거래 10회 이상
    results = [r for r in results if r["trade_count"] >= 10]
    results.sort(key=lambda r: r["sharpe"], reverse=True)

    print(f"\n{'='*110}")
    print(f"=== 상위 30개 (샤프비율 기준, 거래 10회+) ===")
    print(f"{'='*110}")
    print(f"{'#':>3} {'수익률':>8} {'MDD':>7} {'샤프':>6} {'승률':>5} {'PF':>5} "
          f"{'거래':>4} {'평균PnL':>10} | {'LB':>3} {'ADX':>3} {'VOL':>4} "
          f"{'STOP':>5} {'ATR_S':>5} {'TRAIL':>5} {'TIME':>4}")
    print("-" * 110)

    for i, r in enumerate(results[:30]):
        p = r["params"]
        pf = r["profit_factor"]
        pf_str = f"{pf:.2f}" if pf != float("inf") else " inf"
        print(f"{i+1:>3} {r['total_return']:>+7.1f}% {r['max_drawdown']:>6.1f}% "
              f"{r['sharpe']:>6.2f} {r['win_rate']:>4.0f}% {pf_str:>5} "
              f"{r['trade_count']:>4} {r['avg_pnl']:>+9,.0f} | "
              f"{p['lookback']:>3} {p['adx_threshold']:>3} {p['volume_mult']:>4.1f} "
              f"{p['stop_pct']:>5.2f} {p['atr_stop_mult']:>5.1f} "
              f"{p['trailing_atr_mult']:>5.1f} {p['time_exit_days']:>4}")

    # 최적 파라미터
    best = results[0]
    print(f"\n{'='*110}")
    print(f"=== 최적 파라미터 ===")
    bp = best["params"]
    print(f"  lookback:          {bp['lookback']}일")
    print(f"  adx_threshold:     {bp['adx_threshold']}")
    print(f"  volume_mult:       {bp['volume_mult']}배")
    print(f"  stop_pct:          {bp['stop_pct']*100:.0f}%")
    print(f"  atr_stop_mult:     {bp['atr_stop_mult']}")
    print(f"  trailing_atr_mult: {bp['trailing_atr_mult']}")
    print(f"  time_exit_days:    {bp['time_exit_days']}일")
    print(f"\n  수익률: {best['total_return']:+.2f}%")
    print(f"  최대 DD: {best['max_drawdown']:.2f}%")
    print(f"  샤프: {best['sharpe']:.2f}")
    print(f"  승률: {best['win_rate']:.1f}%")
    print(f"  손익비: {best['profit_factor']:.2f}")
    print(f"  거래: {best['trade_count']}회")
    print(f"  청산 사유: {best['exit_reasons']}")

    # 결과 저장
    rows = []
    for r in results:
        row = {**r["params"]}
        for k in ["total_return", "max_drawdown", "sharpe", "win_rate",
                   "profit_factor", "trade_count", "avg_pnl", "final_value"]:
            row[k] = r[k]
        rows.append(row)
    pd.DataFrame(rows).to_csv(LOGS_DIR / "optimization_results.csv", index=False)

    best_save = {"params": best["params"], "results": {k: v for k, v in best.items() if k != "params"}}
    best_save["results"]["exit_reasons"] = str(best["exit_reasons"])
    with open(LOGS_DIR / "best_params.json", "w") as f:
        json.dump(best_save, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n저장: optimization_results.csv, best_params.json")

    # ─── 고정 파라미터도 탐색 ──────────────────────
    print(f"\n{'='*110}")
    print(f"=== 2차: 고정 파라미터 탐색 (상위 5개 × 포지션/비중) ===")

    top5 = results[:5]
    fixed_grid = {
        "max_positions":    [2, 3, 4, 5],
        "max_position_pct": [0.15, 0.20, 0.30, 0.40],
    }
    fixed_combos = list(itertools.product(*fixed_grid.values()))

    results2 = []
    for base in top5:
        for mp, mpp in fixed_combos:
            params = {**base["params"], "max_positions": mp, "max_position_pct": mpp}
            try:
                r = fast_backtest(indicators, market_df, params)
                r["params"] = params
                results2.append(r)
            except Exception:
                pass

    results2 = [r for r in results2 if r["trade_count"] >= 10]
    results2.sort(key=lambda r: r["sharpe"], reverse=True)

    print(f"{'#':>3} {'수익률':>8} {'MDD':>7} {'샤프':>6} {'승률':>5} {'PF':>5} "
          f"{'거래':>4} | {'LB':>3} {'ADX':>3} {'VOL':>4} "
          f"{'STOP':>5} {'TRAIL':>5} {'TIME':>4} {'POS':>3} {'WT%':>4}")
    print("-" * 100)

    for i, r in enumerate(results2[:15]):
        p = r["params"]
        pf_str = f"{r['profit_factor']:.2f}" if r["profit_factor"] != float("inf") else " inf"
        print(f"{i+1:>3} {r['total_return']:>+7.1f}% {r['max_drawdown']:>6.1f}% "
              f"{r['sharpe']:>6.2f} {r['win_rate']:>4.0f}% {pf_str:>5} "
              f"{r['trade_count']:>4} | "
              f"{p['lookback']:>3} {p['adx_threshold']:>3} {p['volume_mult']:>4.1f} "
              f"{p['stop_pct']:>5.2f} {p['trailing_atr_mult']:>5.1f} "
              f"{p['time_exit_days']:>4} {p['max_positions']:>3} {p['max_position_pct']*100:>3.0f}%")

    if results2:
        final_best = results2[0]
        final_save = {"params": final_best["params"],
                      "results": {k: v for k, v in final_best.items() if k != "params"}}
        final_save["results"]["exit_reasons"] = str(final_best.get("exit_reasons", {}))
        with open(LOGS_DIR / "best_params.json", "w") as f:
            json.dump(final_save, f, ensure_ascii=False, indent=2, default=str)
        print(f"\n최종 최적 파라미터 저장: best_params.json")


if __name__ == "__main__":
    optimize()
