from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Iterable

import httpx

from .config import settings
from .schemas import LLMResult


@dataclass(frozen=True)
class ModelInfo:
    id: str
    owned_by: str | None = None


def _normalize_model_id(model_id: str) -> str:
    return model_id.strip()


def _default_model_priority() -> list[str]:
    # MVP: pragmatic defaults. We'll later replace this with a ranked list sourced
    # from public benchmarks + local health stats.
    return [
        "glm-4.7-flash",
        "or-llama-3.3-70b-free",
        "or-hermes-3-405b-free",
        "or-deepseek-r1-0528-free",
        "or-mistral-small-3.1-free",
        "or-gpt-oss-120b-free",
        "or-step-3.5-flash-free",
        "or-qwen3-next-80b-free",
        "or-qwen3-coder-free",
        "or-qwen3-4b-free",
    ]


def _is_model_allowed(mi: ModelInfo) -> bool:
    if settings.exclude_openai_owned_models and (mi.owned_by or "").lower() == "openai":
        return False
    # Hard blocklist: user requested never to use models tied to these accounts.
    mid = (mi.id or "").casefold()
    if "icapusta@gmail.com" in mid or "weflyinsky@gmail.com" in mid:
        return False
    return True


def _select_models(available: Iterable[ModelInfo]) -> list[str]:
    avail_map = {m.id: m for m in available}
    picked: list[str] = []

    for mid in _default_model_priority():
        mi = avail_map.get(mid)
        if not mi:
            continue
        if not _is_model_allowed(mi):
            continue
        picked.append(mid)

    # If our priority list doesn't intersect, fallback to anything allowed.
    if not picked:
        for mi in available:
            if _is_model_allowed(mi):
                picked.append(mi.id)
                if len(picked) >= 10:
                    break

    # Dedupe while preserving order
    seen: set[str] = set()
    out: list[str] = []
    for m in picked:
        m = _normalize_model_id(m)
        if m and m not in seen:
            seen.add(m)
            out.append(m)
    return out


async def list_models(client: httpx.AsyncClient) -> list[ModelInfo]:
    url = f"{settings.cliproxy_base_url.rstrip('/')}/models"
    r = await client.get(url, headers=_auth_headers(), timeout=10.0)
    r.raise_for_status()
    data = r.json()
    out: list[ModelInfo] = []
    for item in data.get("data", []):
        out.append(ModelInfo(id=item.get("id", ""), owned_by=item.get("owned_by")))
    return [m for m in out if m.id]


def _auth_headers() -> dict[str, str]:
    if not settings.cliproxy_api_key:
        return {}
    return {"Authorization": f"Bearer {settings.cliproxy_api_key}"}


def _build_prompt(text: str) -> str:
    text = text.strip()
    return (
        "Ты фильтруешь входящие сообщения из Telegram и определяешь: ищет ли автор исполнителя/подрядчика\n"
        "на разработку, внедрение, CRM, интеграции (1C, amoCRM, Bitrix24), автоматизации, сложные API и т.п.\n\n"
        "Верни СТРОГО JSON (без лишнего текста) в формате:\n"
        '{"is_lead": true|false, "summary": "1-2 предложения на русском", "confidence": 0.0-1.0}\n\n'
        "Требования:\n"
        "- summary должен быть только на русском языке (без английских слов; бренды можно).\n"
        "- Без markdown.\n"
        "- Без переводов строк внутри summary.\n\n"
        f"Сообщение:\n{text}\n"
    )


def _try_parse_json(s: str) -> dict | None:
    s = s.strip()
    # Common: model wraps JSON in code fences.
    s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.I)
    s = re.sub(r"\s*```$", "", s)
    try:
        return json.loads(s)
    except Exception:
        # Try to extract first {...}
        m = re.search(r"\{.*\}", s, flags=re.S)
        if not m:
            return None
        try:
            return json.loads(m.group(0))
        except Exception:
            return None


async def classify(text: str) -> tuple[LLMResult, str]:
    if not settings.cliproxy_base_url:
        raise RuntimeError("cliproxy_base_url is not set")
    if not settings.cliproxy_api_key:
        raise RuntimeError("cliproxy_api_key is not set")

    async with httpx.AsyncClient() as client:
        models = await list_models(client)
        candidates = _select_models(models)
        if not candidates:
            raise RuntimeError("no available models")

        prompt = _build_prompt(text)
        last_err: Exception | None = None

        for mid in candidates[: settings.llm_max_attempts]:
            try:
                res = await _chat_completions(client, mid, prompt)
                return res, mid
            except Exception as e:
                last_err = e
                continue

        raise RuntimeError(f"all model attempts failed: {last_err}") from last_err


async def _chat_completions(client: httpx.AsyncClient, model: str, prompt: str) -> LLMResult:
    url = f"{settings.cliproxy_base_url.rstrip('/')}/chat/completions"
    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a strict JSON generator. Output JSON only. All strings in JSON must be Russian."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
    }
    r = await client.post(url, headers=_auth_headers(), json=body, timeout=settings.llm_timeout_s)
    r.raise_for_status()
    data = r.json()
    content = (
        data.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "")
    )
    parsed = _try_parse_json(content)
    if not parsed:
        raise RuntimeError(f"failed to parse model output: {content[:200]}")
    return LLMResult.model_validate(parsed)


async def suggest_stopwords(*, text: str, existing_stopwords_text: str) -> tuple[list[str], str]:
    """
    Suggest additional stopwords/phrases (Russian) after a human marks a message as NOT a lead.
    Returns (stopwords, model_used).
    """
    if not settings.cliproxy_base_url:
        raise RuntimeError("cliproxy_base_url is not set")
    if not settings.cliproxy_api_key:
        raise RuntimeError("cliproxy_api_key is not set")

    existing = [x.strip() for x in (existing_stopwords_text or "").splitlines() if x.strip()]
    existing_preview = "\n".join(existing[:80])

    prompt = (
        "You help maintain a stopword list for filtering irrelevant Telegram messages.\n"
        "Given a message that the user marked as NOT A LEAD, propose up to 6 NEW stopwords/phrases in Russian to block similar messages.\n"
        "Do not propose generic words like 'и', 'это', 'работа'. Prefer short stems/phrases specific to the irrelevant topic.\n"
        "Return STRICT JSON only:\n"
        '{"stopwords": ["..."]}\n\n'
        "Existing stopwords (sample):\n"
        f"{existing_preview}\n\n"
        "Message:\n"
        f"{(text or '').strip()}\n"
    )

    async with httpx.AsyncClient() as client:
        models = await list_models(client)
        candidates = _select_models(models)
        if not candidates:
            raise RuntimeError("no available models")

        last_err: Exception | None = None
        for mid in candidates[: settings.llm_max_attempts]:
            try:
                url = f"{settings.cliproxy_base_url.rstrip('/')}/chat/completions"
                body = {
                    "model": mid,
                    "messages": [
                        {
                            "role": "system",
                            "content": "You are a strict JSON generator. Output JSON only. All strings must be Russian.",
                        },
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.2,
                }
                r = await client.post(url, headers=_auth_headers(), json=body, timeout=settings.llm_timeout_s)
                r.raise_for_status()
                data = r.json()
                content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                parsed = _try_parse_json(content)
                if not parsed or "stopwords" not in parsed:
                    raise RuntimeError(f"failed to parse stopwords: {content[:200]}")
                arr = parsed.get("stopwords") or []
                if not isinstance(arr, list):
                    raise RuntimeError("stopwords is not a list")

                out: list[str] = []
                seen: set[str] = set(x.casefold() for x in existing)
                for item in arr:
                    if not isinstance(item, str):
                        continue
                    w = item.strip()
                    if not w:
                        continue
                    if len(w) < 3:
                        continue
                    if w.casefold() in seen:
                        continue
                    seen.add(w.casefold())
                    out.append(w)
                    if len(out) >= 6:
                        break
                return out, mid
            except Exception as e:
                last_err = e
                continue

        raise RuntimeError(f"all stopword attempts failed: {last_err}") from last_err
