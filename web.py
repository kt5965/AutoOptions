"""DB 데이터 조회 웹 UI"""
import sys
import os
import sqlite3
import json
from pathlib import Path
from flask import Flask, render_template_string, jsonify, request

# auto 패키지 import를 위해 상위 디렉토리 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

app = Flask(__name__)
DB_PATH = Path(__file__).parent / "logs" / "market.db"
TRADES_DB_PATH = Path(__file__).parent / "logs" / "trades.db"
WATCHLIST_PATH = Path(__file__).parent / "logs" / "watchlist.json"


def _load_watchlist() -> dict:
    """{"starred": ["005930", ...]}"""
    if WATCHLIST_PATH.exists():
        try:
            return json.loads(WATCHLIST_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"starred": []}


def _save_watchlist(wl: dict):
    WATCHLIST_PATH.write_text(json.dumps(wl, ensure_ascii=False, indent=2), encoding="utf-8")

HTML = """
<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>주식 데이터 대시보드</title>
<style>
  * { margin:0; padding:0; box-sizing:border-box; }
  body { font-family:'Segoe UI',sans-serif; background:#0f1117; color:#e0e0e0; }
  .header { background:#161b22; padding:16px 24px; border-bottom:1px solid #30363d; display:flex; justify-content:space-between; align-items:center; }
  .header h1 { font-size:20px; color:#58a6ff; }
  .stats { display:flex; gap:12px; }
  .stat-card { background:#21262d; padding:10px 16px; border-radius:8px; text-align:center; }
  .stat-card .num { font-size:22px; font-weight:700; color:#58a6ff; }
  .stat-card .label { font-size:11px; color:#8b949e; margin-top:2px; }
  .container { display:flex; height:calc(100vh - 65px); }
  .sidebar { width:280px; background:#161b22; border-right:1px solid #30363d; overflow-y:auto; flex-shrink:0; }
  .search { padding:12px; position:sticky; top:0; background:#161b22; z-index:1; }
  .search input { width:100%; padding:8px 12px; background:#21262d; border:1px solid #30363d; border-radius:6px; color:#e0e0e0; font-size:13px; outline:none; }
  .search input:focus { border-color:#58a6ff; }
  .ticker-list { padding:0 8px 8px; }
  .ticker-item { padding:10px 12px; border-radius:6px; cursor:pointer; display:flex; justify-content:space-between; align-items:center; font-size:13px; }
  .ticker-item:hover { background:#21262d; }
  .ticker-item.active { background:#1f6feb33; border-left:3px solid #58a6ff; }
  .ticker-item .code { color:#8b949e; font-size:11px; }
  .ticker-item .name { font-weight:500; }
  .ticker-item .right { text-align:right; }
  .ticker-item .days { font-size:11px; color:#8b949e; }
  .main { flex:1; overflow-y:auto; padding:20px; }
  .panel { background:#161b22; border:1px solid #30363d; border-radius:10px; margin-bottom:16px; overflow:hidden; }
  .panel-header { padding:14px 18px; border-bottom:1px solid #30363d; font-weight:600; font-size:14px; display:flex; justify-content:space-between; }
  .panel-body { padding:16px 18px; }
  .chart-container { height:350px; position:relative; }
  canvas { width:100%!important; height:100%!important; }
  table { width:100%; border-collapse:collapse; font-size:12px; }
  th { text-align:left; padding:8px 10px; color:#8b949e; font-weight:500; border-bottom:1px solid #30363d; position:sticky; top:0; background:#161b22; }
  td { padding:7px 10px; border-bottom:1px solid #21262d; }
  tr:hover td { background:#21262d; }
  .up { color:#f85149; }
  .down { color:#58a6ff; }
  .badge { display:inline-block; padding:2px 8px; border-radius:10px; font-size:11px; font-weight:500; }
  .badge-high { background:#f8514922; color:#f85149; }
  .badge-mid { background:#d2992222; color:#d29922; }
  .badge-low { background:#58a6ff22; color:#58a6ff; }
  .indicator-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:12px; }
  .ind-card { background:#21262d; padding:12px; border-radius:8px; text-align:center; }
  .ind-card .val { font-size:18px; font-weight:700; margin:4px 0; }
  .ind-card .lbl { font-size:11px; color:#8b949e; }
  .table-wrap { max-height:400px; overflow-y:auto; }
  .empty { text-align:center; padding:60px; color:#8b949e; }
  .empty svg { width:48px; height:48px; margin-bottom:12px; opacity:0.3; }
  /* 탭 네비게이션 */
  .tab-nav { display:flex; gap:0; background:#161b22; border-bottom:1px solid #30363d; padding:0 24px; }
  .tab-btn { padding:12px 20px; font-size:13px; font-weight:500; color:#8b949e; cursor:pointer; border:none; background:none; border-bottom:2px solid transparent; transition:all 0.2s; }
  .tab-btn:hover { color:#e0e0e0; }
  .tab-btn.active { color:#58a6ff; border-bottom-color:#58a6ff; }
  .tab-content { display:none; }
  .tab-content.active { display:flex; }
  /* 매매내역 전용 */
  .trades-container { flex:1; overflow-y:auto; padding:20px; }
  .filter-bar { display:flex; gap:12px; margin-bottom:16px; align-items:center; flex-wrap:wrap; }
  .filter-bar select, .filter-bar input { padding:8px 12px; background:#21262d; border:1px solid #30363d; border-radius:6px; color:#e0e0e0; font-size:13px; outline:none; }
  .filter-bar select:focus, .filter-bar input:focus { border-color:#58a6ff; }
  .summary-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:12px; margin-bottom:16px; }
  .summary-card { background:#161b22; border:1px solid #30363d; border-radius:10px; padding:16px; text-align:center; }
  .summary-card .val { font-size:22px; font-weight:700; margin:4px 0; }
  .summary-card .lbl { font-size:11px; color:#8b949e; }
  .trade-buy { color:#f85149; }
  .trade-sell { color:#58a6ff; }
  .pnl-plus { color:#3fb950; }
  .pnl-minus { color:#f85149; }
  .trade-memo { max-width:300px; font-size:11px; color:#8b949e; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
  .equity-chart-wrap { height:250px; margin-bottom:16px; }
  /* 별표/관심종목 */
  .star-btn { background:none; border:none; cursor:pointer; font-size:16px; padding:2px; opacity:0.3; transition:all 0.15s; }
  .star-btn:hover { opacity:0.7; }
  .star-btn.starred { opacity:1; }
  .ticker-item .left { display:flex; align-items:center; gap:6px; }
  .filter-row { display:flex; gap:8px; padding:8px 12px; align-items:center; }
  .filter-row label { font-size:11px; color:#8b949e; cursor:pointer; display:flex; align-items:center; gap:4px; }
  .filter-row input[type=checkbox] { accent-color:#58a6ff; }
  .add-ticker-bar { padding:8px 12px; border-top:1px solid #30363d; }
  .add-ticker-bar form { display:flex; gap:6px; }
  .add-ticker-bar input { flex:1; padding:7px 10px; background:#21262d; border:1px solid #30363d; border-radius:6px; color:#e0e0e0; font-size:12px; outline:none; }
  .add-ticker-bar input:focus { border-color:#58a6ff; }
  .add-ticker-bar button { padding:7px 12px; background:#238636; border:none; border-radius:6px; color:#fff; font-size:12px; cursor:pointer; white-space:nowrap; }
  .add-ticker-bar button:hover { background:#2ea043; }
  .add-ticker-bar .status { font-size:11px; color:#8b949e; padding:4px 0; min-height:18px; }
  .ticker-item.starred-item { border-left:3px solid #d29922; }
</style>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
</head>
<body>

<div class="header">
  <h1>Stock Data Dashboard</h1>
  <div class="stats" id="globalStats"></div>
</div>

<div class="tab-nav">
  <button class="tab-btn active" onclick="switchTab('market')">종목 데이터</button>
  <button class="tab-btn" onclick="switchTab('trades')">매매내역</button>
</div>

<div class="container tab-content active" id="tab-market">
  <div class="sidebar">
    <div class="search"><input type="text" id="searchInput" placeholder="종목명/코드 검색..." oninput="filterTickers()"></div>
    <div class="filter-row">
      <label><input type="checkbox" id="starredOnly" onchange="filterTickers()"> 관심종목만</label>
    </div>
    <div class="ticker-list" id="tickerList"></div>
    <div class="add-ticker-bar">
      <form onsubmit="addTicker(event)">
        <input type="text" id="addTickerInput" placeholder="종목코드 (예: 005930)">
        <button type="submit">추가</button>
      </form>
      <div class="status" id="addTickerStatus"></div>
    </div>
  </div>
  <div class="main" id="mainContent">
    <div class="empty">
      <svg viewBox="0 0 24 24" fill="currentColor"><path d="M3 13h2v-2H3v2zm0 4h2v-2H3v2zm0-8h2V7H3v2zm4 4h14v-2H7v2zm0 4h14v-2H7v2zM7 7v2h14V7H7z"/></svg>
      <p>좌측에서 종목을 선택하세요</p>
    </div>
  </div>
</div>

<div class="container tab-content" id="tab-trades" style="flex-direction:column;">
  <div class="trades-container" id="tradesContent">
    <div class="empty"><p>로딩 중...</p></div>
  </div>
</div>

<script>
let allTickers = [];
let starredSet = new Set();
let priceChart = null;
let volChart = null;

async function init() {
  const [tickerRes, starRes] = await Promise.all([
    fetch('/api/tickers'),
    fetch('/api/watchlist')
  ]);
  const data = await tickerRes.json();
  const starData = await starRes.json();
  allTickers = data.tickers;
  starredSet = new Set(starData.starred || []);
  renderStats(data.stats);
  renderTickerList(allTickers);
}

function renderStats(s) {
  document.getElementById('globalStats').innerHTML = `
    <div class="stat-card"><div class="num">${s.tickers}</div><div class="label">종목</div></div>
    <div class="stat-card"><div class="num">${(s.total_rows/1000).toFixed(1)}K</div><div class="label">데이터 행</div></div>
    <div class="stat-card"><div class="num">${s.min_date||'-'}</div><div class="label">시작일</div></div>
    <div class="stat-card"><div class="num">${s.max_date||'-'}</div><div class="label">최근일</div></div>
  `;
}

function renderTickerList(tickers) {
  const el = document.getElementById('tickerList');
  // 별표 종목 먼저 정렬
  const sorted = [...tickers].sort((a,b) => {
    const aS = starredSet.has(a.ticker) ? 0 : 1;
    const bS = starredSet.has(b.ticker) ? 0 : 1;
    return aS - bS;
  });
  el.innerHTML = sorted.map(t => {
    const isStarred = starredSet.has(t.ticker);
    const starCls = isStarred ? 'starred' : '';
    const itemCls = isStarred ? 'starred-item' : '';
    return `
    <div class="ticker-item ${itemCls}" id="ti-${t.ticker}">
      <div class="left">
        <button class="star-btn ${starCls}" onclick="toggleStar(event,'${t.ticker}')" title="관심종목">${isStarred ? '★' : '☆'}</button>
        <div onclick="selectTicker('${t.ticker}')" style="cursor:pointer">
          <div class="name">${t.name}</div><div class="code">${t.ticker}</div>
        </div>
      </div>
      <div class="right"><div class="days">${t.rows}일</div></div>
    </div>`;
  }).join('');
}

function filterTickers() {
  const q = document.getElementById('searchInput').value.toLowerCase();
  const starOnly = document.getElementById('starredOnly').checked;
  let filtered = allTickers;
  if(starOnly) filtered = filtered.filter(t => starredSet.has(t.ticker));
  if(q) filtered = filtered.filter(t => t.name.toLowerCase().includes(q) || t.ticker.includes(q));
  renderTickerList(filtered);
}

async function toggleStar(e, ticker) {
  e.stopPropagation();
  const action = starredSet.has(ticker) ? 'remove' : 'add';
  const res = await fetch('/api/watchlist', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({ticker, action})
  });
  const data = await res.json();
  starredSet = new Set(data.starred || []);
  filterTickers();
}

async function addTicker(e) {
  e.preventDefault();
  const input = document.getElementById('addTickerInput');
  const status = document.getElementById('addTickerStatus');
  const ticker = input.value.trim();
  if(!ticker) return;
  if(allTickers.some(t=>t.ticker===ticker)) {
    status.textContent = ticker + ' 이미 있음';
    return;
  }
  status.textContent = ticker + ' 데이터 수집 중...';
  try {
    const res = await fetch('/api/add_ticker', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({ticker})
    });
    const data = await res.json();
    if(data.success) {
      status.textContent = data.name + ' 추가 완료 (' + data.rows + '일)';
      // 별표 자동 등록
      await fetch('/api/watchlist', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body:JSON.stringify({ticker, action:'add'})
      });
      starredSet.add(ticker);
      allTickers.push({ticker, name:data.name, rows:data.rows});
      input.value = '';
      filterTickers();
    } else {
      status.textContent = '실패: ' + (data.error||'알 수 없는 오류');
    }
  } catch(err) {
    status.textContent = '오류: ' + err.message;
  }
}

async function selectTicker(ticker) {
  document.querySelectorAll('.ticker-item').forEach(e=>e.classList.remove('active'));
  const el = document.getElementById('ti-'+ticker);
  if(el) el.classList.add('active');

  const res = await fetch(`/api/data/${ticker}`);
  const d = await res.json();
  renderDetail(d);
}

function renderDetail(d) {
  const latest = d.rows[d.rows.length-1];
  const prev = d.rows.length>1 ? d.rows[d.rows.length-2] : latest;
  const chg = latest.close - prev.close;
  const chgPct = prev.close ? (chg/prev.close*100) : 0;
  const cls = chg>=0 ? 'up' : 'down';

  document.getElementById('mainContent').innerHTML = `
    <div class="panel">
      <div class="panel-header">
        <span>${d.name} (${d.ticker})</span>
        <span class="${cls}">${latest.close.toLocaleString()}원 (${chg>=0?'+':''}${chgPct.toFixed(2)}%)</span>
      </div>
      <div class="panel-body">
        <div class="indicator-grid">
          <div class="ind-card"><div class="lbl">RSI(14)</div><div class="val ${latest.rsi14>70?'up':latest.rsi14<30?'down':''}">${(latest.rsi14||0).toFixed(1)}</div><div class="lbl">${latest.rsi14>70?'과매수':latest.rsi14<30?'과매도':'중립'}</div></div>
          <div class="ind-card"><div class="lbl">ADX(14)</div><div class="val">${(latest.adx14||0).toFixed(1)}</div><div class="lbl">${latest.adx14>25?'추세 강':'추세 약'}</div></div>
          <div class="ind-card"><div class="lbl">ATR(14)</div><div class="val">${(latest.atr14||0).toFixed(0)}</div><div class="lbl">변동성</div></div>
          <div class="ind-card"><div class="lbl">거래량비율</div><div class="val ${latest.vol_ratio>2?'up':''}">${(latest.vol_ratio||0).toFixed(2)}x</div><div class="lbl">20일 대비</div></div>
        </div>
      </div>
    </div>

    <div class="panel">
      <div class="panel-header"><span>가격 + 이동평균 + 볼린저밴드</span></div>
      <div class="panel-body"><div class="chart-container"><canvas id="priceCanvas"></canvas></div></div>
    </div>

    <div class="panel">
      <div class="panel-header"><span>거래량</span></div>
      <div class="panel-body"><div class="chart-container" style="height:150px"><canvas id="volCanvas"></canvas></div></div>
    </div>

    <div class="panel">
      <div class="panel-header"><span>최근 데이터</span></div>
      <div class="panel-body"><div class="table-wrap">
        <table>
          <thead><tr><th>날짜</th><th>종가</th><th>MA5</th><th>MA20</th><th>MA60</th><th>RSI</th><th>ADX</th><th>ATR</th><th>거래량비율</th></tr></thead>
          <tbody>${d.rows.slice(-30).reverse().map(r=>`
            <tr>
              <td>${r.date}</td>
              <td>${r.close.toLocaleString()}</td>
              <td>${(r.ma5||0).toLocaleString()}</td>
              <td>${(r.ma20||0).toLocaleString()}</td>
              <td>${(r.ma60||0).toLocaleString()}</td>
              <td class="${r.rsi14>70?'up':r.rsi14<30?'down':''}">${(r.rsi14||0).toFixed(1)}</td>
              <td>${(r.adx14||0).toFixed(1)}</td>
              <td>${(r.atr14||0).toFixed(0)}</td>
              <td class="${r.vol_ratio>2?'up':''}">${(r.vol_ratio||0).toFixed(2)}</td>
            </tr>`).join('')}
          </tbody>
        </table>
      </div></div>
    </div>
  `;

  drawCharts(d.rows);
}

function drawCharts(rows) {
  const labels = rows.map(r=>r.date);
  const closes = rows.map(r=>r.close);
  const ma5 = rows.map(r=>r.ma5);
  const ma20 = rows.map(r=>r.ma20);
  const ma60 = rows.map(r=>r.ma60);
  const bbU = rows.map(r=>r.bb_upper);
  const bbL = rows.map(r=>r.bb_lower);
  const vols = rows.map(r=>r.volume);

  const common = { responsive:true, maintainAspectRatio:false, plugins:{legend:{labels:{color:'#8b949e',font:{size:11}}}},
    scales:{ x:{ticks:{color:'#8b949e',maxTicksLimit:12,font:{size:10}},grid:{color:'#21262d'}}, y:{ticks:{color:'#8b949e',font:{size:10}},grid:{color:'#21262d'}} } };

  if(priceChart) priceChart.destroy();
  priceChart = new Chart(document.getElementById('priceCanvas'), {
    type:'line', data:{labels, datasets:[
      {label:'종가',data:closes,borderColor:'#e0e0e0',borderWidth:1.5,pointRadius:0,fill:false},
      {label:'MA5',data:ma5,borderColor:'#f85149',borderWidth:1,pointRadius:0,fill:false},
      {label:'MA20',data:ma20,borderColor:'#58a6ff',borderWidth:1,pointRadius:0,fill:false},
      {label:'MA60',data:ma60,borderColor:'#d29922',borderWidth:1,pointRadius:0,fill:false},
      {label:'BB상단',data:bbU,borderColor:'#8b949e44',borderWidth:1,pointRadius:0,borderDash:[4,4],fill:false},
      {label:'BB하단',data:bbL,borderColor:'#8b949e44',borderWidth:1,pointRadius:0,borderDash:[4,4],fill:'-1',backgroundColor:'#8b949e0a'},
    ]}, options:common
  });

  if(volChart) volChart.destroy();
  const volColors = vols.map((v,i) => {
    if(i===0) return '#58a6ff66';
    return closes[i]>=closes[i-1] ? '#f8514966' : '#58a6ff66';
  });
  volChart = new Chart(document.getElementById('volCanvas'), {
    type:'bar', data:{labels, datasets:[{label:'거래량',data:vols,backgroundColor:volColors}]},
    options:{...common, plugins:{legend:{display:false}}}
  });
}

// === 탭 전환 ===
function switchTab(tab) {
  document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(c=>c.classList.remove('active'));
  document.getElementById('tab-'+tab).classList.add('active');
  document.querySelectorAll('.tab-btn').forEach(b=>{
    if((tab==='market' && b.textContent==='종목 데이터') || (tab==='trades' && b.textContent==='매매내역'))
      b.classList.add('active');
  });
  if(tab==='trades') loadTrades();
}

// === 매매내역 ===
let allTrades = [];
let allSnapshots = [];
let equityChart = null;

async function loadTrades() {
  const [tRes, sRes] = await Promise.all([
    fetch('/api/trades'),
    fetch('/api/snapshots')
  ]);
  const tData = await tRes.json();
  const sData = await sRes.json();
  allTrades = tData.trades;
  allSnapshots = sData.snapshots;
  renderTrades(allTrades, allSnapshots);
}

function renderTrades(trades, snapshots) {
  const el = document.getElementById('tradesContent');

  // 통계 계산
  const buys = trades.filter(t=>t.side==='buy');
  const sells = trades.filter(t=>t.side==='sell');
  const totalPnl = trades.reduce((s,t)=>s+(t.pnl||0),0);
  const wins = sells.filter(t=>t.pnl>0);
  const losses = sells.filter(t=>t.pnl<0);
  const winRate = sells.length>0 ? (wins.length/sells.length*100).toFixed(1) : '0.0';
  const totalComm = trades.reduce((s,t)=>s+(t.commission||0)+(t.tax||0)+(t.slippage||0),0);

  // 최근 자산
  const latestSnap = snapshots.length>0 ? snapshots[snapshots.length-1] : null;
  const totalValue = latestSnap ? latestSnap.total_value : 0;
  const cumPnl = latestSnap ? latestSnap.cumulative_pnl : 0;

  el.innerHTML = `
    <div class="summary-grid">
      <div class="summary-card">
        <div class="lbl">총 자산</div>
        <div class="val">${totalValue ? Math.round(totalValue).toLocaleString() : '-'}원</div>
      </div>
      <div class="summary-card">
        <div class="lbl">누적 손익</div>
        <div class="val ${cumPnl>=0?'pnl-plus':'pnl-minus'}">${cumPnl>=0?'+':''}${Math.round(cumPnl).toLocaleString()}원</div>
      </div>
      <div class="summary-card">
        <div class="lbl">총 거래</div>
        <div class="val">${trades.length}건</div>
        <div class="lbl">매수 ${buys.length} / 매도 ${sells.length}</div>
      </div>
      <div class="summary-card">
        <div class="lbl">승률 (매도 기준)</div>
        <div class="val">${winRate}%</div>
        <div class="lbl">${wins.length}승 ${losses.length}패</div>
      </div>
      <div class="summary-card">
        <div class="lbl">실현 손익</div>
        <div class="val ${totalPnl>=0?'pnl-plus':'pnl-minus'}">${totalPnl>=0?'+':''}${Math.round(totalPnl).toLocaleString()}원</div>
      </div>
      <div class="summary-card">
        <div class="lbl">총 비용</div>
        <div class="val">${Math.round(totalComm).toLocaleString()}원</div>
        <div class="lbl">수수료+세금+슬리피지</div>
      </div>
    </div>

    ${snapshots.length>1 ? '<div class="panel"><div class="panel-header"><span>자산 추이</span></div><div class="panel-body"><div class="equity-chart-wrap"><canvas id="equityCanvas"></canvas></div></div></div>' : ''}

    <div class="panel">
      <div class="panel-header">
        <span>거래 내역 (${trades.length}건)</span>
        <div class="filter-bar">
          <select id="sideFilter" onchange="applyTradeFilter()">
            <option value="all">전체</option>
            <option value="buy">매수</option>
            <option value="sell">매도</option>
          </select>
          <select id="strategyFilter" onchange="applyTradeFilter()">
            <option value="all">전략 전체</option>
            ${[...new Set(trades.map(t=>t.strategy).filter(Boolean))].map(s=>'<option value="'+s+'">'+s+'</option>').join('')}
          </select>
        </div>
      </div>
      <div class="panel-body"><div class="table-wrap" style="max-height:500px;">
        <table>
          <thead><tr>
            <th>일시</th><th>종목</th><th>구분</th><th>가격</th><th>수량</th>
            <th>금액</th><th>수수료</th><th>세금</th><th>손익</th><th>전략</th><th>메모</th>
          </tr></thead>
          <tbody id="tradesBody">
            ${renderTradeRows(trades)}
          </tbody>
        </table>
      </div></div>
    </div>
  `;

  if(snapshots.length>1) drawEquityChart(snapshots);
}

function renderTradeRows(trades) {
  return trades.slice().reverse().map(t => {
    const sideClass = t.side==='buy' ? 'trade-buy' : 'trade-sell';
    const sideText = t.side==='buy' ? '매수' : '매도';
    const pnlClass = t.pnl>0 ? 'pnl-plus' : t.pnl<0 ? 'pnl-minus' : '';
    const pnlText = t.pnl ? (t.pnl>0?'+':'')+Math.round(t.pnl).toLocaleString() : '-';
    return `<tr>
      <td>${t.timestamp}</td>
      <td><strong>${t.name||t.ticker}</strong> <small style="color:#888">${t.ticker}</small></td>
      <td class="${sideClass}"><strong>${sideText}</strong></td>
      <td>${Math.round(t.price).toLocaleString()}</td>
      <td>${t.quantity}</td>
      <td>${Math.round(t.amount).toLocaleString()}</td>
      <td>${Math.round(t.commission).toLocaleString()}</td>
      <td>${Math.round(t.tax).toLocaleString()}</td>
      <td class="${pnlClass}">${pnlText}</td>
      <td><span class="badge badge-mid">${t.strategy||'-'}</span></td>
      <td class="trade-memo" title="${(t.memo||'').replace(/"/g,'&quot;')}">${t.memo||''}</td>
    </tr>`;
  }).join('');
}

function applyTradeFilter() {
  const side = document.getElementById('sideFilter').value;
  const strategy = document.getElementById('strategyFilter').value;
  let filtered = allTrades;
  if(side!=='all') filtered = filtered.filter(t=>t.side===side);
  if(strategy!=='all') filtered = filtered.filter(t=>t.strategy===strategy);
  document.getElementById('tradesBody').innerHTML = renderTradeRows(filtered);
}

function drawEquityChart(snapshots) {
  const canvas = document.getElementById('equityCanvas');
  if(!canvas) return;
  if(equityChart) equityChart.destroy();

  const labels = snapshots.map(s=>s.date);
  const values = snapshots.map(s=>s.total_value);

  equityChart = new Chart(canvas, {
    type:'line',
    data:{labels, datasets:[{
      label:'총 자산',
      data:values,
      borderColor:'#58a6ff',
      borderWidth:2,
      pointRadius:snapshots.length>50?0:3,
      fill:true,
      backgroundColor:'#58a6ff15',
    }]},
    options:{
      responsive:true,
      maintainAspectRatio:false,
      plugins:{legend:{labels:{color:'#8b949e'}}},
      scales:{
        x:{ticks:{color:'#8b949e',maxTicksLimit:10,font:{size:10}},grid:{color:'#21262d'}},
        y:{ticks:{color:'#8b949e',font:{size:10},callback:v=>Math.round(v/10000)+'만'},grid:{color:'#21262d'}}
      }
    }
  });
}

init();
</script>
</body>
</html>
"""


def get_db():
    return sqlite3.connect(DB_PATH)


@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/api/tickers")
def api_tickers():
    conn = get_db()
    rows = conn.execute("""
        SELECT ticker, name, COUNT(*) as rows, MIN(date) as min_date, MAX(date) as max_date
        FROM ohlcv GROUP BY ticker ORDER BY name
    """).fetchall()

    stats_row = conn.execute("SELECT COUNT(*), COUNT(DISTINCT ticker), MIN(date), MAX(date) FROM ohlcv").fetchone()
    conn.close()

    tickers = [{"ticker": r[0], "name": r[1] or r[0], "rows": r[2], "min_date": r[3], "max_date": r[4]} for r in rows]
    stats = {"total_rows": stats_row[0], "tickers": stats_row[1], "min_date": stats_row[2], "max_date": stats_row[3]}

    return jsonify({"tickers": tickers, "stats": stats})


@app.route("/api/data/<ticker>")
def api_data(ticker):
    conn = get_db()
    rows = conn.execute("""
        SELECT date, open, high, low, close, volume,
               ma5, ma20, ma60, rsi14, bb_upper, bb_mid, bb_lower,
               atr14, adx14, vol_ratio, name
        FROM ohlcv WHERE ticker = ? ORDER BY date
    """, (ticker,)).fetchall()
    conn.close()

    cols = ["date","open","high","low","close","volume","ma5","ma20","ma60","rsi14","bb_upper","bb_mid","bb_lower","atr14","adx14","vol_ratio","name"]
    data = [dict(zip(cols, r)) for r in rows]
    name = data[0]["name"] if data else ticker

    return jsonify({"ticker": ticker, "name": name, "rows": data})


def get_trades_db():
    return sqlite3.connect(TRADES_DB_PATH)


@app.route("/api/trades")
def api_trades():
    if not TRADES_DB_PATH.exists():
        return jsonify({"trades": []})
    conn = get_trades_db()
    rows = conn.execute("""
        SELECT id, timestamp, ticker, side, price, quantity, amount,
               commission, tax, slippage, pnl, strategy, memo
        FROM trades ORDER BY timestamp DESC, id DESC
    """).fetchall()
    conn.close()
    cols = ["id", "timestamp", "ticker", "side", "price", "quantity", "amount",
            "commission", "tax", "slippage", "pnl", "strategy", "memo"]
    trades = [dict(zip(cols, r)) for r in rows]
    # 종목명 매핑
    from auto.data.collector import UNIVERSE
    for t in trades:
        t["name"] = UNIVERSE.get(t["ticker"], t["ticker"])
    return jsonify({"trades": trades})


@app.route("/api/snapshots")
def api_snapshots():
    if not TRADES_DB_PATH.exists():
        return jsonify({"snapshots": []})
    conn = get_trades_db()
    rows = conn.execute("""
        SELECT date, total_value, cash, positions_value,
               daily_pnl, cumulative_pnl, drawdown
        FROM portfolio_snapshots ORDER BY date
    """).fetchall()
    conn.close()
    cols = ["date", "total_value", "cash", "positions_value",
            "daily_pnl", "cumulative_pnl", "drawdown"]
    snapshots = [dict(zip(cols, r)) for r in rows]
    return jsonify({"snapshots": snapshots})


@app.route("/api/watchlist", methods=["GET"])
def api_watchlist_get():
    wl = _load_watchlist()
    return jsonify(wl)


@app.route("/api/watchlist", methods=["POST"])
def api_watchlist_post():
    body = request.get_json(force=True)
    ticker = body.get("ticker", "").strip()
    action = body.get("action", "add")
    wl = _load_watchlist()
    starred = wl.get("starred", [])
    if action == "add" and ticker and ticker not in starred:
        starred.append(ticker)
    elif action == "remove" and ticker in starred:
        starred.remove(ticker)
    wl["starred"] = starred
    _save_watchlist(wl)
    return jsonify(wl)


@app.route("/api/add_ticker", methods=["POST"])
def api_add_ticker():
    """pykrx로 새 종목 데이터를 수집해서 market.db에 저장"""
    body = request.get_json(force=True)
    ticker = body.get("ticker", "").strip()
    if not ticker:
        return jsonify({"success": False, "error": "종목코드 필요"})

    try:
        from pykrx import stock as pykrx_stock
        from datetime import datetime, timedelta

        # 종목명 조회
        name = pykrx_stock.get_market_ticker_name(ticker)
        if not name:
            return jsonify({"success": False, "error": f"{ticker} 종목을 찾을 수 없습니다"})

        # 최근 1년 데이터 수집
        end = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
        df = pykrx_stock.get_market_ohlcv_by_date(start, end, ticker)

        if df.empty:
            return jsonify({"success": False, "error": f"{ticker} 데이터 없음"})

        col_map = {"시가": "open", "고가": "high", "저가": "low",
                   "종가": "close", "거래량": "volume"}
        df = df.rename(columns=col_map)
        df.index.name = "date"
        df = df[["open", "high", "low", "close", "volume"]]
        df["ticker"] = ticker

        # market.db에 저장
        conn = get_db()
        for date_idx, row in df.iterrows():
            date_str = date_idx.strftime("%Y-%m-%d")
            conn.execute("""
                INSERT OR REPLACE INTO ohlcv (ticker, date, name, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (ticker, date_str, name, int(row["open"]), int(row["high"]),
                  int(row["low"]), int(row["close"]), int(row["volume"])))
        conn.commit()
        conn.close()

        return jsonify({"success": True, "name": name, "rows": len(df)})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
