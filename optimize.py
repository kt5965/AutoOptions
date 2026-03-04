#!/usr/bin/env python3
"""전략 파라미터 최적화 (numpy 벡터화)

1단계: 전종목 지표를 numpy 패널로 변환 (1회)
2단계: 파라미터별 백테스트 반복 (패널 기반, ~0.3초/회)
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
from auto.data.collector import MarketDataCollector

LOGS_DIR = Path(__file__).parent / "logs"


def build_panel(universe: dict[str, pd.DataFrame], market_df: pd.DataFrame) -> dict:
    """전종목 일별 데이터를 numpy 배열로 변환"""
    dates = market_df.index.sort_values()
    date_to_idx = {d: i for i, d in enumerate(dates)}
    n_dates = len(dates)

    tickers = [t for t in universe if t != "069500" and len(universe[t]) >= 60]
    n_tickers = len(tickers)

    # 배열 초기화
    close = np.full((n_dates, n_tickers), np.nan)
    high = np.full((n_dates, n_tickers), np.nan)
    low = np.full((n_dates, n_tickers), np.nan)
    atr14 = np.full((n_dates, n_tickers), np.nan)
    adx14 = np.full((n_dates, n_tickers), np.nan)
    vol_ratio = np.full((n_dates, n_tickers), np.nan)
    ma5 = np.full((n_dates, n_tickers), np.nan)
    ma20 = np.full((n_dates, n_tickers), np.nan)

    lookbacks = [10, 15, 20, 30, 40, 60]
    prev_highs = {lb: np.full((n_dates, n_tickers), np.nan) for lb in lookbacks}

    for j, ticker in enumerate(tickers):
        df = universe[ticker].copy()

        # 지표 계산
        df["ma5"] = df["close"].rolling(5).mean()
        df["ma20"] = df["close"].rolling(20).mean()
        vol_ma = df["volume"].astype(float).rolling(20).mean()
        df["vol_ratio"] = df["volume"].astype(float) / vol_ma

        for lb in lookbacks:
            df[f"ph_{lb}"] = df["high"].shift(1).rolling(lb).max()

        if "atr14" not in df.columns or df["atr14"].isna().all():
            tr = pd.concat([
                df["high"] - df["low"],
                (df["high"] - df["close"].shift(1)).abs(),
                (df["low"] - df["close"].shift(1)).abs()
            ], axis=1).max(axis=1)
            df["atr14"] = tr.ewm(span=14, adjust=False).mean()

        if "adx14" not in df.columns or df["adx14"].isna().all():
            tr = pd.concat([
                df["high"] - df["low"],
                (df["high"] - df["close"].shift(1)).abs(),
                (df["low"] - df["close"].shift(1)).abs()
            ], axis=1).max(axis=1)
            a14 = tr.ewm(span=14, adjust=False).mean()
            up = df["high"] - df["high"].shift(1)
            down = df["low"].shift(1) - df["low"]
            plus_dm = pd.Series(np.where((up > down) & (up > 0), up, 0), index=df.index)
            minus_dm = pd.Series(np.where((down > up) & (down > 0), down, 0), index=df.index)
            plus_di = 100 * plus_dm.ewm(span=14, adjust=False).mean() / a14
            minus_di = 100 * minus_dm.ewm(span=14, adjust=False).mean() / a14
            dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
            df["adx14"] = dx.ewm(span=14, adjust=False).mean()

        # 패널에 매핑
        for dt in df.index:
            if dt not in date_to_idx:
                continue
            i = date_to_idx[dt]
            close[i, j] = df.loc[dt, "close"]
            high[i, j] = df.loc[dt, "high"]
            low[i, j] = df.loc[dt, "low"]
            atr14[i, j] = df.loc[dt, "atr14"]
            adx14[i, j] = df.loc[dt, "adx14"]
            vol_ratio[i, j] = df.loc[dt, "vol_ratio"]
            ma5[i, j] = df.loc[dt, "ma5"]
            ma20[i, j] = df.loc[dt, "ma20"]
            for lb in lookbacks:
                prev_highs[lb][i, j] = df.loc[dt, f"ph_{lb}"]

    return {
        "dates": dates, "tickers": tickers,
        "close": close, "high": high, "low": low,
        "atr14": atr14, "adx14": adx14, "vol_ratio": vol_ratio,
        "ma5": ma5, "ma20": ma20, "prev_highs": prev_highs,
        "n_dates": n_dates, "n_tickers": n_tickers,
    }


def fast_backtest(panel: dict, params: dict) -> dict:
    """numpy 패널 기반 고속 백테스트"""
    lookback = params["lookback"]
    adx_thresh = params["adx_threshold"]
    vol_mult = params["volume_mult"]
    stop_pct = params["stop_pct"]
    atr_stop_mult = params["atr_stop_mult"]
    trail_mult = params["trailing_atr_mult"]
    time_exit = params["time_exit_days"]
    max_pos = params["max_positions"]
    daily_limit = params["daily_loss_limit"]
    max_loss_trade = params["max_loss_per_trade"]
    max_pos_pct = params["max_position_pct"]

    buy_fee = 0.00115   # 수수료 + 슬리피지
    sell_fee = 0.00295   # 수수료 + 세금 + 슬리피지

    capital = 10_000_000.0
    cash = capital
    peak_val = capital

    cl = panel["close"]
    hi = panel["high"]
    lo = panel["low"]
    at = panel["atr14"]
    ad = panel["adx14"]
    vr = panel["vol_ratio"]
    m5 = panel["ma5"]
    m20 = panel["ma20"]
    ph = panel["prev_highs"].get(lookback)

    if ph is None:
        return _empty_result(capital)

    n_dates = panel["n_dates"]

    # 포지션 추적 (최대 max_pos 개)
    pos_ticker = np.full(max_pos, -1, dtype=np.int32)  # ticker index, -1 = 빈 슬롯
    pos_entry = np.zeros(max_pos)
    pos_stop = np.zeros(max_pos)
    pos_trail = np.zeros(max_pos)
    pos_highest = np.zeros(max_pos)
    pos_days = np.zeros(max_pos, dtype=np.int32)
    pos_qty = np.zeros(max_pos, dtype=np.int32)

    sell_pnls = []
    sell_reasons = []  # 0=손절 1=트레일 2=시간 3=일일손실
    daily_returns = []
    prev_value = capital
    max_dd = 0.0

    for i in range(60, n_dates):
        daily_pnl = 0.0

        # 포트폴리오 평가
        pv = 0.0
        for s in range(max_pos):
            j = pos_ticker[s]
            if j < 0:
                continue
            c = cl[i, j]
            pv += (c if not np.isnan(c) else pos_entry[s]) * pos_qty[s]
        total_value = cash + pv

        # ── 청산 ──
        for s in range(max_pos):
            j = pos_ticker[s]
            if j < 0:
                continue

            c = cl[i, j]
            h = hi[i, j]
            l = lo[i, j]
            a = at[i, j]

            if np.isnan(c):
                continue

            pos_days[s] += 1

            # 고점/트레일링 갱신
            if not np.isnan(h) and h > pos_highest[s]:
                pos_highest[s] = h
                if not np.isnan(a):
                    nt = h - trail_mult * a
                    if nt > pos_trail[s]:
                        pos_trail[s] = nt

            reason = -1
            exit_price = c
            if not np.isnan(l):
                if l <= pos_stop[s]:
                    reason = 0
                    exit_price = pos_stop[s]
                elif l <= pos_trail[s]:
                    reason = 1
                    exit_price = pos_trail[s]

            if reason < 0 and pos_days[s] >= time_exit:
                reason = 2
                exit_price = c

            if reason >= 0:
                proceeds = exit_price * pos_qty[s] * (1 - sell_fee)
                cost_basis = pos_entry[s] * pos_qty[s] * (1 + buy_fee)
                pnl = proceeds - cost_basis
                cash += proceeds
                daily_pnl += pnl
                sell_pnls.append(pnl)
                sell_reasons.append(reason)
                pos_ticker[s] = -1

        # 일일 손실 한도
        if daily_pnl <= -daily_limit:
            for s in range(max_pos):
                j = pos_ticker[s]
                if j < 0:
                    continue
                c = cl[i, j]
                ep = c if not np.isnan(c) else pos_entry[s]
                proceeds = ep * pos_qty[s] * (1 - sell_fee)
                cost_basis = pos_entry[s] * pos_qty[s] * (1 + buy_fee)
                pnl = proceeds - cost_basis
                cash += proceeds
                daily_pnl += pnl
                sell_pnls.append(pnl)
                sell_reasons.append(3)
                pos_ticker[s] = -1

        # ── 진입 ──
        n_held = int(np.sum(pos_ticker >= 0))
        if n_held < max_pos and daily_pnl > -daily_limit:
            # 벡터화 조건
            hi_i = hi[i]
            ph_i = ph[i]
            ad_i = ad[i]
            vr_i = vr[i]
            m5_i = m5[i]
            m20_i = m20[i]
            cl_i = cl[i]
            at_i = at[i]

            valid = ~np.isnan(cl_i) & ~np.isnan(at_i) & ~np.isnan(ph_i)
            cond = valid & (hi_i > ph_i) & (ad_i > adx_thresh) & (vr_i > vol_mult) & (m5_i > m20_i)

            # 이미 보유 종목 제외
            for s in range(max_pos):
                if pos_ticker[s] >= 0:
                    cond[pos_ticker[s]] = False

            cand_idx = np.where(cond)[0]
            if len(cand_idx) > 0:
                strengths = np.clip((ad_i[cand_idx] - adx_thresh) / 30, 0, 1) * \
                            np.clip(vr_i[cand_idx] / 3, 0, 1)
                order = np.argsort(-strengths)
                cand_idx = cand_idx[order]

                for j in cand_idx:
                    if n_held >= max_pos:
                        break

                    cp = cl_i[j]
                    ca = at_i[j]

                    sl_pct = cp * (1 - stop_pct)
                    sl_atr = cp - atr_stop_mult * ca
                    sl = max(sl_pct, sl_atr)

                    risk = cp - sl
                    if risk <= 0:
                        continue

                    qr = int(max_loss_trade / risk)
                    qw = int(total_value * max_pos_pct / cp)
                    qc = int(cash / (cp * (1 + buy_fee)))
                    qty = min(qr, qw, qc)
                    if qty <= 0:
                        continue

                    cost = cp * qty * (1 + buy_fee)
                    if cost > cash:
                        continue

                    # 빈 슬롯 찾기
                    slot = -1
                    for s in range(max_pos):
                        if pos_ticker[s] < 0:
                            slot = s
                            break
                    if slot < 0:
                        break

                    cash -= cost
                    pos_ticker[slot] = j
                    pos_entry[slot] = cp
                    pos_stop[slot] = sl
                    pos_trail[slot] = cp - trail_mult * ca
                    pos_highest[slot] = cp
                    pos_days[slot] = 0
                    pos_qty[slot] = qty
                    n_held += 1

        # 일일 스냅샷
        pv = 0.0
        for s in range(max_pos):
            j = pos_ticker[s]
            if j < 0:
                continue
            c = cl[i, j]
            pv += (c if not np.isnan(c) else pos_entry[s]) * pos_qty[s]
        tv = cash + pv
        dr = (tv - prev_value) / prev_value if prev_value > 0 else 0
        daily_returns.append(dr)

        peak_val = max(peak_val, tv)
        dd = (peak_val - tv) / peak_val if peak_val > 0 else 0
        max_dd = max(max_dd, dd)
        prev_value = tv

    # 결과 집계
    n_sells = len(sell_pnls)
    if n_sells == 0:
        return _empty_result(prev_value, max_dd)

    pnl_arr = np.array(sell_pnls)
    wins = int(np.sum(pnl_arr > 0))
    gp = float(np.sum(pnl_arr[pnl_arr > 0]))
    gl = float(np.abs(np.sum(pnl_arr[pnl_arr < 0])))

    ret_arr = np.array(daily_returns)
    std = ret_arr.std()
    sharpe = float(ret_arr.mean() / std * (252**0.5)) if std > 0 else 0

    reason_map = {0: "손절", 1: "트레일링", 2: "시간청산", 3: "일일손실"}
    exit_reasons = {}
    for rc in sell_reasons:
        name = reason_map.get(rc, "기타")
        exit_reasons[name] = exit_reasons.get(name, 0) + 1

    return {
        "total_return": (prev_value - 10_000_000) / 10_000_000 * 100,
        "max_drawdown": max_dd * 100,
        "sharpe": sharpe,
        "win_rate": wins / n_sells * 100,
        "profit_factor": gp / gl if gl > 0 else (float("inf") if gp > 0 else 0),
        "trade_count": n_sells,
        "avg_pnl": float(pnl_arr.mean()),
        "final_value": prev_value,
        "exit_reasons": exit_reasons,
    }


def _empty_result(final=10_000_000, max_dd=0):
    return {"total_return": (final - 10_000_000) / 10_000_000 * 100,
            "max_drawdown": max_dd * 100, "sharpe": 0, "win_rate": 0,
            "profit_factor": 0, "trade_count": 0, "avg_pnl": 0,
            "final_value": final, "exit_reasons": {}}


def optimize():
    collector = MarketDataCollector(str(LOGS_DIR / "market.db"))
    start, end = "20210303", "20260303"

    print("데이터 로딩...")
    market_df = collector.load_from_db("069500", start, end)
    universe_raw = collector.load_all_from_db(start, end)
    print(f"  마켓: {len(market_df)}일, 유니버스: {len(universe_raw)}종목")

    print("numpy 패널 구축...")
    t0 = time.time()
    panel = build_panel(universe_raw, market_df)
    print(f"  완료: {time.time()-t0:.1f}초, {panel['n_tickers']}종목 × {panel['n_dates']}일\n")

    # 벤치마크
    current = {
        "lookback": 40, "adx_threshold": 20, "volume_mult": 2.0,
        "stop_pct": 0.05, "atr_stop_mult": 3.0, "trailing_atr_mult": 2.5,
        "time_exit_days": 15, "max_positions": 2, "daily_loss_limit": 50_000,
        "max_loss_per_trade": 30_000, "max_position_pct": 0.60,
    }
    t0 = time.time()
    bench = fast_backtest(panel, current)
    bt = time.time() - t0
    print(f"벤치마크: {bt:.2f}초/회")
    print(f"  현재: ret={bench['total_return']:+.1f}% dd={bench['max_drawdown']:.1f}% "
          f"sharpe={bench['sharpe']:.2f} wr={bench['win_rate']:.0f}% trades={bench['trade_count']}\n")

    # ─── 그리드 ────────────────────────────
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
        "max_positions": 3, "daily_loss_limit": 50_000,
        "max_loss_per_trade": 30_000, "max_position_pct": 0.30,
    }

    keys = list(grid.keys())
    combos = list(itertools.product(*grid.values()))
    total = len(combos)
    print(f"=== 1차 그리드 서치: {total}개 조합, 예상 {total*bt/60:.0f}분 ===\n")

    results = []
    t_start = time.time()

    for idx, combo in enumerate(combos):
        params = dict(zip(keys, combo))
        params.update(fixed)
        try:
            r = fast_backtest(panel, params)
            r["params"] = params
            results.append(r)
        except Exception:
            pass

        if (idx + 1) % 1000 == 0:
            el = time.time() - t_start
            eta = el / (idx+1) * (total - idx - 1)
            best = max(results, key=lambda x: x["sharpe"]) if results else None
            bs = f"best sharpe={best['sharpe']:.2f} ret={best['total_return']:+.1f}%" if best else ""
            print(f"  [{idx+1}/{total}] {el:.0f}s, ETA {eta:.0f}s | {bs}")

    elapsed = time.time() - t_start
    print(f"\n1차 완료: {elapsed:.0f}초 ({elapsed/60:.1f}분)")

    results = [r for r in results if r["trade_count"] >= 10]
    results.sort(key=lambda r: r["sharpe"], reverse=True)

    print(f"\n{'='*110}")
    print(f"=== 상위 30개 (샤프비율 기준) ===")
    print(f"{'='*110}")
    hdr = (f"{'#':>3} {'수익률':>8} {'MDD':>7} {'샤프':>6} {'승률':>5} {'PF':>5} "
           f"{'거래':>4} {'평균PnL':>10} | {'LB':>3} {'ADX':>3} {'VOL':>4} "
           f"{'STOP':>5} {'ATR_S':>5} {'TRAIL':>5} {'TIME':>4}")
    print(hdr)
    print("-" * 110)

    for i, r in enumerate(results[:30]):
        p = r["params"]
        pf = r["profit_factor"]
        pf_s = f"{pf:.2f}" if pf != float("inf") else " inf"
        print(f"{i+1:>3} {r['total_return']:>+7.1f}% {r['max_drawdown']:>6.1f}% "
              f"{r['sharpe']:>6.2f} {r['win_rate']:>4.0f}% {pf_s:>5} "
              f"{r['trade_count']:>4} {r['avg_pnl']:>+9,.0f} | "
              f"{p['lookback']:>3} {p['adx_threshold']:>3} {p['volume_mult']:>4.1f} "
              f"{p['stop_pct']:>5.2f} {p['atr_stop_mult']:>5.1f} "
              f"{p['trailing_atr_mult']:>5.1f} {p['time_exit_days']:>4}")

    best = results[0]
    bp = best["params"]
    print(f"\n=== 1차 최적 ===")
    for k, v in bp.items():
        if k in grid:
            print(f"  {k}: {v}")
    print(f"  ret={best['total_return']:+.2f}% dd={best['max_drawdown']:.2f}% "
          f"sharpe={best['sharpe']:.2f} wr={best['win_rate']:.1f}% pf={best['profit_factor']:.2f}")
    print(f"  청산: {best['exit_reasons']}")

    # ─── 2차: 포지션/비중 ──────────────────
    print(f"\n=== 2차: 상위 5개 × 포지션/비중 ===")
    top5 = results[:5]
    results2 = []
    for base in top5:
        for mp in [2, 3, 4, 5]:
            for mpp in [0.15, 0.20, 0.30, 0.40, 0.60]:
                params = {**base["params"], "max_positions": mp, "max_position_pct": mpp}
                try:
                    r = fast_backtest(panel, params)
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
        pf_s = f"{r['profit_factor']:.2f}" if r["profit_factor"] != float("inf") else " inf"
        print(f"{i+1:>3} {r['total_return']:>+7.1f}% {r['max_drawdown']:>6.1f}% "
              f"{r['sharpe']:>6.2f} {r['win_rate']:>4.0f}% {pf_s:>5} "
              f"{r['trade_count']:>4} | "
              f"{p['lookback']:>3} {p['adx_threshold']:>3} {p['volume_mult']:>4.1f} "
              f"{p['stop_pct']:>5.2f} {p['trailing_atr_mult']:>5.1f} "
              f"{p['time_exit_days']:>4} {p['max_positions']:>3} {p['max_position_pct']*100:>3.0f}%")

    # 저장
    rows = []
    all_results = results + results2
    for r in all_results:
        row = {**r["params"]}
        for k in ["total_return", "max_drawdown", "sharpe", "win_rate",
                   "profit_factor", "trade_count", "avg_pnl", "final_value"]:
            row[k] = r[k]
        rows.append(row)
    pd.DataFrame(rows).to_csv(LOGS_DIR / "optimization_results.csv", index=False)

    final_best = results2[0] if results2 else results[0]
    with open(LOGS_DIR / "best_params.json", "w") as f:
        save = {"params": final_best["params"],
                "results": {k: v for k, v in final_best.items() if k not in ("params",)}}
        save["results"]["exit_reasons"] = str(final_best.get("exit_reasons", {}))
        json.dump(save, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n저장 완료: optimization_results.csv, best_params.json")


if __name__ == "__main__":
    optimize()
