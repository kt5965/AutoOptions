"""AI 에이전트 - Claude API로 시황 분석 및 전략 제안"""
import json
from dataclasses import dataclass
from typing import Optional

try:
    import anthropic
except ImportError:
    anthropic = None


@dataclass
class AnalysisResult:
    regime_opinion: str          # "trending" | "mean_reverting" | "high_volatility"
    action_suggestions: list[dict]  # [{"ticker": "005930", "action": "buy", "reason": "..."}]
    parameter_adjustments: dict  # {"momentum_lookback": 25, ...}
    risk_warning: str
    summary: str


class AIAnalyst:
    """Claude API 기반 시황 분석 에이전트"""

    def __init__(self, api_key: str = "", model: str = "claude-sonnet-4-20250514"):
        self.api_key = api_key
        self.model = model
        self.client = None
        if api_key and anthropic:
            self.client = anthropic.Anthropic(api_key=api_key)

    def analyze(
        self,
        portfolio_summary: dict,
        market_data: dict,
        recent_trades: list[dict],
        current_regime: str,
    ) -> Optional[AnalysisResult]:
        """포트폴리오 상태 + 시장 데이터를 Claude에게 전달하고 분석 받기"""
        if not self.client:
            return None

        prompt = self._build_prompt(portfolio_summary, market_data, recent_trades, current_regime)

        response = self.client.messages.create(
            model=self.model,
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}],
            system="당신은 한국 주식시장 전문 퀀트 분석가입니다. JSON 형식으로 분석 결과를 반환하세요.",
        )

        return self._parse_response(response.content[0].text)

    def _build_prompt(self, portfolio: dict, market: dict, trades: list, regime: str) -> str:
        return f"""현재 포트폴리오 상태:
{json.dumps(portfolio, ensure_ascii=False, indent=2)}

시장 데이터:
{json.dumps(market, ensure_ascii=False, indent=2)}

최근 거래 내역:
{json.dumps(trades, ensure_ascii=False, indent=2)}

현재 시장 레짐 판별: {regime}

다음 JSON 형식으로 분석 결과를 반환하세요:
{{
  "regime_opinion": "trending|mean_reverting|high_volatility",
  "action_suggestions": [{{"ticker": "종목코드", "action": "buy|sell|hold", "reason": "이유"}}],
  "parameter_adjustments": {{"파라미터명": 값}},
  "risk_warning": "리스크 경고 메시지",
  "summary": "전체 요약"
}}"""

    def _parse_response(self, text: str) -> Optional[AnalysisResult]:
        try:
            # JSON 블록 추출
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0]
            elif "```" in text:
                text = text.split("```")[1].split("```")[0]
            data = json.loads(text.strip())
            return AnalysisResult(**data)
        except (json.JSONDecodeError, KeyError, TypeError):
            return None
