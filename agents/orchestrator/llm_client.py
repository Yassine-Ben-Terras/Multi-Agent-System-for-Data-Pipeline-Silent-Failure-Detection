"""
Orchestrator — LLM Client

Calls the Anthropic Claude API to generate a root cause hypothesis
when rule-based correlation is inconclusive.

Tiered strategy (as per implementation guide):
  - Rule-based first (Correlator) — no LLM cost
  - claude-haiku  → classification tasks (known pattern, fast + cheap)
  - claude-sonnet → novel pattern reasoning (full hypothesis generation)

The LLM receives ONLY metadata — never raw warehouse data.
Input: anomaly signal details (types, severities, models, counts, z-scores)
Output: structured JSON with root_cause, explanation, recommended_actions
"""

from __future__ import annotations

import json
import logging
import os
from typing import List, Optional

from config.schemas import AnomalySignal

logger = logging.getLogger(__name__)

MODEL_REASONING       = os.getenv("LLM_MODEL_REASONING",       "claude-sonnet-4-20250514")
MODEL_CLASSIFICATION  = os.getenv("LLM_MODEL_CLASSIFICATION",  "claude-haiku-4-5-20251001")
MAX_TOKENS            = 1024


HYPOTHESIS_PROMPT = """You are a senior data engineer analysing anomaly signals from a data pipeline monitoring system.

You have received the following anomaly signals from a single pipeline run. Your job is to determine the most likely root cause.

ANOMALY SIGNALS:
{signals_json}

Analyse these signals and respond with ONLY a valid JSON object in this exact format:
{{
  "root_cause": "one short phrase describing the root cause (e.g. 'upstream_api_schema_change')",
  "confidence": 0.85,
  "explanation": "2-3 sentence explanation of what likely happened and why these signals are related",
  "recommended_actions": ["action 1", "action 2"],
  "is_false_positive": false
}}

Rules:
- root_cause must be a snake_case string, max 5 words
- confidence must be a float between 0.0 and 1.0
- explanation must reference the specific signals provided
- recommended_actions must be concrete, actionable steps
- Respond with JSON only — no preamble, no markdown fences
"""


class LLMClient:
    """
    Wraps Anthropic Claude API for root cause hypothesis generation.
    Handles retries, response parsing, and fallback on error.
    """

    def __init__(self, api_key: Optional[str] = None):
        self._api_key = api_key or os.getenv("ANTHROPIC_API_KEY", "")
        self._enabled = bool(self._api_key)

        if not self._enabled:
            logger.warning(
                "ANTHROPIC_API_KEY not set — LLM calls disabled, "
                "rule-based correlation only"
            )

    def generate_hypothesis(
        self,
        signals: List[AnomalySignal],
        use_fast_model: bool = False,
    ) -> Optional[str]:
        """
        Generate a root cause hypothesis for the given signals.
        Returns the LLM's explanation string, or None if disabled/failed.
        """
        if not self._enabled:
            return None

        model = MODEL_CLASSIFICATION if use_fast_model else MODEL_REASONING

        signals_summary = [
            {
                "anomaly_type": s.anomaly_type.value,
                "model_name":   s.model_name,
                "severity":     s.severity.value,
                "confidence":   s.confidence,
                "details":      s.details,
            }
            for s in signals
        ]

        prompt = HYPOTHESIS_PROMPT.format(
            signals_json=json.dumps(signals_summary, indent=2)
        )

        try:
            import anthropic
            client = anthropic.Anthropic(api_key=self._api_key)

            response = client.messages.create(
                model=model,
                max_tokens=MAX_TOKENS,
                messages=[{"role": "user", "content": prompt}],
            )

            raw = response.content[0].text.strip()
            parsed = json.loads(raw)

            logger.info(
                "llm_hypothesis_generated | model=%s root_cause=%s confidence=%.2f",
                model,
                parsed.get("root_cause", "unknown"),
                parsed.get("confidence", 0.0),
            )

            return parsed.get("explanation", raw)

        except json.JSONDecodeError as e:
            logger.warning("llm_response_not_json | error=%s", str(e))
            return raw if "raw" in dir() else None

        except Exception as e:
            logger.error("llm_call_failed | model=%s error=%s", model, str(e))
            return None

    def classify_root_cause(self, signals: List[AnomalySignal]) -> Optional[str]:
        """
        Fast classification using haiku — returns root_cause string only.
        Used when we have a partial rule match but want LLM confirmation.
        """
        if not self._enabled:
            return None

        signals_summary = [
            {"type": s.anomaly_type.value, "model": s.model_name, "severity": s.severity.value}
            for s in signals
        ]

        prompt = (
            f"Data pipeline anomaly signals: {json.dumps(signals_summary)}.\n"
            "Respond with ONLY a snake_case root cause string, max 5 words. "
            "Examples: schema_drift, api_pagination_truncation, idempotency_failure, "
            "timezone_mismatch, integer_overflow"
        )

        try:
            import anthropic
            client = anthropic.Anthropic(api_key=self._api_key)

            response = client.messages.create(
                model=MODEL_CLASSIFICATION,
                max_tokens=32,
                messages=[{"role": "user", "content": prompt}],
            )
            return response.content[0].text.strip().lower().replace(" ", "_")

        except Exception as e:
            logger.error("llm_classify_failed | error=%s", str(e))
            return None
