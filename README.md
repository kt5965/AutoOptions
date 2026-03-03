# 📈 AutoOptions — 퀀트 자동매매 시스템

KOSPI200 종목 대상 퀀트 자동매매 시스템.  
시장 레짐(추세/횡보/고변동성)을 자동 판별하여 모멘텀·평균회귀 전략을 전환하고, 백테스팅·페이퍼 트레이딩·텔레그램 알림까지 자동화합니다.

---

## 🏗 프로젝트 구조

```
auto/
├── config.py              # 전체 시스템 설정 (리스크, 수수료, 전략 파라미터)
├── main.py                # 메인 실행 (백테스트 / 페이퍼 트레이딩)
├── scheduler.py           # 스케줄러 데몬 (systemd 서비스)
├── backtest.py            # 백테스팅 프레임워크
├── optimize.py            # 전략 파라미터 최적화
├── run_backtest.py        # 백테스트 실행 스크립트
├── web.py                 # 웹 대시보드 (Flask)
├── install_service.sh     # systemd 서비스 등록 스크립트
│
├── strategy/              # 전략 엔진
│   ├── engine.py          #   레짐 기반 전략 자동 전환
│   ├── momentum.py        #   모멘텀 브레이크아웃 전략
│   ├── mean_reversion.py  #   평균회귀 전략 (볼린저밴드 + RSI)
│   ├── regime.py          #   시장 레짐 판별 (ADX, VKOSPI)
│   └── indicators.py      #   기술적 지표 계산
│
├── data/                  # 데이터 수집/저장
│   ├── collector.py       #   pykrx 기반 KOSPI200 데이터 수집
│   └── store.py           #   SQLite 저장/캐싱
│
├── execution/             # 매매 실행
│   ├── base.py            #   주문/체결 인터페이스
│   ├── paper.py           #   페이퍼 트레이딩 (가상 체결)
│   └── live.py            #   실전 거래 (증권사 API 연동 스텁)
│
├── risk/                  # 리스크 관리
│   └── manager.py         #   포지션 사이징, 손절, 드로다운 제한
│
├── agent/                 # AI 분석
│   └── analyst.py         #   Claude API 기반 시황 분석 에이전트
│
├── alert/                 # 알림
│   ├── telegram.py        #   텔레그램 알림 전송
│   └── bot.py             #   텔레그램 봇 (명령어 인터페이스)
│
├── report/                # 리포트
│   └── generator.py       #   일일/주간 리포트 생성
│
└── logs/                  # 런타임 데이터 (git 제외)
    ├── trades.db          #   거래 기록 DB
    ├── market.db          #   시장 데이터 DB
    └── cache/             #   종목 OHLCV 캐시
```

---

## ⚡ 주요 기능

### 전략 엔진
- **시장 레짐 자동 판별** — ADX(추세 강도) + VKOSPI(변동성)로 3가지 레짐 분류
- **모멘텀 브레이크아웃** — N일 고가 돌파 + 거래량 급증 시 진입
- **평균회귀** — 볼린저밴드 2.5σ + RSI 25 이하 극단 진입
- **레짐별 자동 전환** — 추세장→모멘텀, 횡보장→평균회귀, 고변동성→보수적 평균회귀

### 리스크 관리
- 종목당 최대 비중 15%, 동시 보유 5종목
- 개별 종목 손절 1%, 일일 손실 한도 3%
- 최대 드로다운 15% 초과 시 전체 매매 중단
- ATR 기반 트레일링 스탑

### 자동화
- **스케줄러** — 장 전 프리스크리닝 → 장중 모니터링 → 장 마감 정리 → 일일 리포트
- **텔레그램 봇** — 매매 알림, 일일 리포트, 명령어로 상태 조회
- **systemd 서비스** — 서버 재부팅 시 자동 재시작

### 백테스팅
- pykrx로 KOSPI200 전 종목 5년치 데이터 수집
- 수수료(0.015%) + 세금(0.18%) + 슬리피지(0.1%) 반영
- 전략 파라미터 그리드 서치 최적화

---

## 🚀 시작하기

### 1. 클론 & 환경 설정

```bash
git clone https://github.com/kt5965/AutoOptions.git
cd AutoOptions
```

### 2. 가상환경 & 패키지 설치

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. 환경변수 설정

```bash
cp .env.example .env
```

`.env` 파일을 편집하여 값을 채워주세요:

```env
TELEGRAM_BOT_TOKEN=봇토큰
TELEGRAM_CHAT_ID=채팅ID
CLAUDE_API_KEY=claude_api_key    # 선택사항
```

### 4. 백테스트 실행

```bash
python -m auto.run_backtest
```

### 5. 스케줄러 실행 (페이퍼 트레이딩)

```bash
# 직접 실행
python -m auto.scheduler

# 또는 systemd 서비스로 등록
sudo bash install_service.sh
```

### 6. 웹 대시보드

```bash
python -m auto.web
# http://localhost:5000
```

---

## ⚙️ 설정

[config.py](config.py)에서 주요 파라미터를 조정할 수 있습니다:

| 설정 | 기본값 | 설명 |
|------|--------|------|
| `initial_capital` | 10,000,000원 | 초기 자본금 |
| `max_drawdown_pct` | 15% | 최대 드로다운 한도 |
| `max_positions` | 5 | 동시 보유 종목 수 |
| `momentum_lookback` | 40일 | 모멘텀 돌파 기준 기간 |
| `mean_rev_bb_std` | 2.5σ | 볼린저밴드 진입 기준 |
| `mean_rev_rsi_oversold` | 25 | RSI 과매도 진입 기준 |

---

## 📅 스케줄러 일정

| 시간 | 작업 | 설명 |
|------|------|------|
| 08:30 | 프리스크리닝 | 유니버스 스캔, 매수 후보 선별 |
| 09:05 | 장 시작 모니터링 | 시그널 체크 + 주문 |
| 매 5분 | 장중 모니터링 | 09:10~15:20, 손절/트레일링 체크 |
| 15:25 | 포지션 정리 | 장 마감 전 정리 |
| 15:35 | 일일 정산 | PnL 기록, 리포트 생성 |
| 16:00 | 데이터 수집 | 당일 데이터 저장 + 일일 리포트 |
| 16:30 (금) | 주간 리포트 | 주간 성과 정리 |

---

## 🔧 개발

### 의존성

```
pykrx >= 1.0.45      # 한국 주식 데이터
pandas >= 2.0.0       # 데이터 처리
numpy >= 1.24.0       # 수치 계산
requests >= 2.31.0    # HTTP 요청
anthropic >= 0.40.0   # Claude AI API
APScheduler >= 3.10.0 # 작업 스케줄링
```

### 브랜치 전략

- `main` — 안정 버전
- `dev` — 개발 브랜치 (여기서 작업 후 main에 merge)
- `feature/*` — 기능 개발 브랜치

---

## ⚠️ 주의사항

- 이 프로젝트는 **교육 및 연구 목적**입니다
- **투자 조언이 아닙니다** — 모든 투자 판단과 책임은 본인에게 있습니다
- 실전 거래 전환 시 증권사 API 연동이 필요합니다 (`execution/live.py`)
- 옵션 거래는 원금 손실 위험이 있습니다

---

## 📝 License

Private repository — 팀 내부 사용
