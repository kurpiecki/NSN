from __future__ import annotations

import json
import time
from typing import Any

import requests

try:
    from secrets import PERPLEXITY_API_KEY, PERPLEXITY_BASE_URL
except Exception:
    PERPLEXITY_API_KEY = ""
    PERPLEXITY_BASE_URL = "https://api.perplexity.ai"


class PerplexityAPIError(RuntimeError):
    pass


class PerplexityClient:
    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout_s: int = 120,
        language_preference: str = "pl",
    ) -> None:
        self.api_key = (api_key or PERPLEXITY_API_KEY or "").strip()
        self.base_url = (base_url or PERPLEXITY_BASE_URL or "https://api.perplexity.ai").rstrip("/")
        self.timeout_s = timeout_s
        self.language_preference = language_preference
        if not self.api_key or "WSTAW" in self.api_key:
            raise PerplexityAPIError("Brak PERPLEXITY_API_KEY w pliku secrets.py")

    @staticmethod
    def normalize_model_id(model: str) -> str:
        raw = (model or "").strip()
        lowered = raw.lower()
        aliases = {
            "chatgpt": "openai/gpt-5.2",
            "gpt": "openai/gpt-5.2",
            "sonar": "perplexity/sonar",
            "sonar-pro": "perplexity/sonar-pro",
            "sonar-deep-research": "perplexity/sonar-deep-research",
        }
        return aliases.get(lowered, raw)

    def create_response_text(
        self,
        *,
        model: str,
        input_text: str,
        instructions: str | None = None,
        tools: list[dict[str, Any]] | None = None,
        max_steps: int | None = None,
        max_output_tokens: int | None = None,
        response_format: dict[str, Any] | None = None,
    ) -> str:
        resolved_model = self.normalize_model_id(model)
        payload: dict[str, Any] = {
            "model": resolved_model,
            "input": input_text,
            "language_preference": self.language_preference,
        }
        if instructions:
            payload["instructions"] = instructions
        if tools:
            payload["tools"] = tools
        if max_steps is not None:
            payload["max_steps"] = max_steps
        if max_output_tokens is not None:
            payload["max_output_tokens"] = max_output_tokens
        if response_format is not None:
            payload["response_format"] = response_format

        try:
            resp_json = self._post_json("/v1/responses", payload)
        except PerplexityAPIError as exc:
            msg = str(exc)
            if response_format is not None and ("response_format" in msg or "invalid_parameter" in msg):
                payload.pop("response_format", None)
                resp_json = self._post_json("/v1/responses", payload)
            else:
                raise

        return self._extract_output_text(resp_json)

    def _post_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        last_err: Exception | None = None
        for attempt in range(1, 6):
            try:
                response = requests.post(url, headers=headers, json=payload, timeout=self.timeout_s)
                if response.status_code == 429:
                    time.sleep(min(10, 1.5**attempt))
                    continue
                if response.status_code >= 400:
                    raise PerplexityAPIError(f"HTTP {response.status_code}: {response.text}")
                return response.json()
            except requests.Timeout as exc:
                raise PerplexityAPIError(
                    f"Przekroczono timeout {self.timeout_s}s dla wywołania API; przerwano bez ponawiania."
                ) from exc
            except (requests.RequestException, ValueError) as exc:
                last_err = exc
                time.sleep(min(10, 1.5**attempt))
        raise PerplexityAPIError(f"Nieudane wywołanie API po retry. Ostatni błąd: {last_err}")

    @staticmethod
    def _extract_output_text(resp_json: dict[str, Any]) -> str:
        output = resp_json.get("output", [])
        texts: list[str] = []
        for item in output:
            if item.get("type") != "message":
                continue
            for content in item.get("content", []) or []:
                if content.get("type") == "output_text" and isinstance(content.get("text"), str):
                    texts.append(content["text"])
        if texts:
            return "\n".join(texts).strip()
        return json.dumps(resp_json, ensure_ascii=False, indent=2)
