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


@dataclass(frozen=True)
class LLMRoutingOptions:
    model_priority_text: str | None = None
    llm_max_attempts: int | None = None
    llm_timeout_s: float | None = None
    exclude_openai_owned_models: bool | None = None
    models_config_json: str | None = None


def _extract_usage(data: dict) -> dict[str, int]:
    usage = data.get("usage") if isinstance(data, dict) else None
    if not isinstance(usage, dict):
        return {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    return {
        "prompt_tokens": int(usage.get("prompt_tokens") or 0),
        "completion_tokens": int(usage.get("completion_tokens") or 0),
        "total_tokens": int(usage.get("total_tokens") or 0),
    }


def _extract_provider_error(data: dict, fallback: str = "") -> tuple[str, str]:
    # Returns (message, provider_status), if present.
    if not isinstance(data, dict):
        return fallback, ""
    err = data.get("error")
    if isinstance(err, dict):
        msg = str(err.get("message") or fallback or "")
        details = err.get("details")
        provider_status = ""
        if isinstance(details, list):
            for d in details:
                if isinstance(d, dict) and d.get("@type", "").endswith("ErrorInfo"):
                    provider_status = str(d.get("metadata", {}).get("status") or "")
                    break
        provider_status = provider_status or str(err.get("status") or "")
        return msg, provider_status
    if isinstance(err, str):
        return err, ""
    return fallback, ""


def _normalize_model_id(model_id: str) -> str:
    return model_id.strip()


def model_block_reason(*, model_id: str, owned_by: str | None, exclude_openai_owned_models: bool) -> str | None:
    if exclude_openai_owned_models and (owned_by or "").lower() == "openai":
        return "owned_by_openai"
    mid = (model_id or "").casefold()
    if "icapusta@gmail.com" in mid or "weflyinsky@gmail.com" in mid:
        return "blocked_account_model"
    if any(x in mid for x in ("whisper", "guard", "vision", "image", "audio", "tts", "stt")):
        return "non_chat_model"
    return None


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


def default_model_priority_text() -> str:
    return "\n".join(_default_model_priority()) + "\n"


def _parse_model_priority_text(raw: str | None) -> list[str]:
    if not raw:
        return _default_model_priority()
    out = [x.strip() for x in raw.splitlines() if x.strip()]
    return out or _default_model_priority()


def _parse_models_config_json(raw: str | None) -> dict[str, dict]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    out: dict[str, dict] = {}
    for k, v in data.items():
        if not isinstance(k, str) or not k.strip():
            continue
        if not isinstance(v, dict):
            continue
        out[k.strip()] = v
    return out


def _model_cfg(model_id: str, models_cfg: dict[str, dict]) -> dict:
    if not model_id:
        return {}
    # Exact first, casefold fallback.
    if model_id in models_cfg:
        return models_cfg[model_id]
    mid = model_id.casefold()
    for k, v in models_cfg.items():
        if k.casefold() == mid:
            return v
    return {}


def _is_model_allowed(mi: ModelInfo, *, exclude_openai_owned_models: bool) -> bool:
    return model_block_reason(
        model_id=mi.id,
        owned_by=mi.owned_by,
        exclude_openai_owned_models=exclude_openai_owned_models,
    ) is None


def _preferred_match(available_id: str, preferred: str) -> bool:
    aid = (available_id or "").casefold()
    p = (preferred or "").casefold()
    if not aid or not p:
        return False
    if aid == p:
        return True
    # Common in CLIProxy: suffixes like "-openrouter" or provider prefixes like "z.ai-".
    if aid.startswith(p + "-") or aid.endswith("-" + p):
        return True
    if p in aid:
        return True
    return False


def _select_models(
    available: Iterable[ModelInfo],
    *,
    preferred_models: list[str] | None = None,
    exclude_openai_owned_models: bool,
) -> list[str]:
    avail_list = list(available)
    avail_map = {m.id: m for m in avail_list}
    picked: list[str] = []

    for mid in (preferred_models or _default_model_priority()):
        # First exact hit.
        mi = avail_map.get(mid)
        if mi and _is_model_allowed(mi, exclude_openai_owned_models=exclude_openai_owned_models):
            picked.append(mi.id)
            continue
        # Then fuzzy match to tolerate provider suffix/prefix variants.
        for cand in avail_list:
            if not _is_model_allowed(cand, exclude_openai_owned_models=exclude_openai_owned_models):
                continue
            if _preferred_match(cand.id, mid):
                picked.append(cand.id)
                break

    # If our priority list doesn't intersect, fallback to anything allowed.
    if not picked:
        for mi in avail_list:
            if _is_model_allowed(mi, exclude_openai_owned_models=exclude_openai_owned_models):
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
        "You classify incoming Telegram messages.\n"
        "Lead=true only if the author is looking for an executor/contractor/vendor for development/integrations/CRM/API work.\n"
        "Lead=false if the author offers own services, posts a portfolio/resume, sells leads/contacts, or publishes generic advertising.\n\n"
        "Return STRICT JSON only:\n"
        '{"is_lead": true|false, "summary": "1-2 sentences in Russian", "confidence": 0.0-1.0}\n\n'
        "Rules:\n"
        "- summary must be in Russian.\n"
        "- no markdown.\n"
        "- no newlines in summary.\n"
        "- if uncertain between request vs self-promo, choose false.\n\n"
        f"Message:\n{text}\n"
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


def _extract_content_text(data: dict) -> str:
    # ChatCompletions style
    msg_content = (
        data.get("choices", [{}])[0]
        .get("message", {})
        .get("content")
    )
    if isinstance(msg_content, str):
        return msg_content
    if isinstance(msg_content, list):
        parts: list[str] = []
        for part in msg_content:
            if isinstance(part, dict):
                txt = part.get("text")
                if isinstance(txt, str) and txt:
                    parts.append(txt)
        if parts:
            return "\n".join(parts)

    # Responses style
    output_text = data.get("output_text", "")
    if isinstance(output_text, str) and output_text:
        return output_text
    if isinstance(data.get("output"), list):
        parts: list[str] = []
        for out in data.get("output", []):
            for part in out.get("content", []) or []:
                txt = part.get("text")
                if isinstance(txt, str) and txt:
                    parts.append(txt)
        if parts:
            return "\n".join(parts)
    return ""


async def classify(text: str, options: LLMRoutingOptions | None = None) -> tuple[LLMResult, str, dict]:
    if not settings.cliproxy_base_url:
        raise RuntimeError("cliproxy_base_url is not set")
    if not settings.cliproxy_api_key:
        raise RuntimeError("cliproxy_api_key is not set")
    opts = options or LLMRoutingOptions()
    preferred_models = _parse_model_priority_text(opts.model_priority_text)
    max_attempts = max(1, int(opts.llm_max_attempts if opts.llm_max_attempts is not None else settings.llm_max_attempts))
    timeout_s = float(opts.llm_timeout_s if opts.llm_timeout_s is not None else settings.llm_timeout_s)
    models_cfg = _parse_models_config_json(opts.models_config_json)
    exclude_openai_owned_models = (
        bool(opts.exclude_openai_owned_models)
        if opts.exclude_openai_owned_models is not None
        else bool(settings.exclude_openai_owned_models)
    )

    async with httpx.AsyncClient() as client:
        models = await list_models(client)
        candidates = _select_models(
            models,
            preferred_models=preferred_models,
            exclude_openai_owned_models=exclude_openai_owned_models,
        )
        if not candidates:
            raise RuntimeError("no available models")

        prompt = _build_prompt(text)
        last_err: Exception | None = None

        for mid in candidates[:max_attempts]:
            try:
                cfg = _model_cfg(mid, models_cfg)
                if cfg.get("enabled") is False:
                    continue
                timeout_eff = timeout_s
                try:
                    if "timeout_s" in cfg:
                        timeout_eff = max(3.0, min(120.0, float(cfg.get("timeout_s"))))
                except Exception:
                    timeout_eff = timeout_s
                res, meta = await _chat_completions(client, mid, prompt, timeout_s=timeout_eff)
                meta["model_id"] = mid
                return res, mid, meta
            except Exception as e:
                last_err = e
                continue

        raise RuntimeError(f"all model attempts failed: {last_err}") from last_err


async def _chat_completions(
    client: httpx.AsyncClient, model: str, prompt: str, *, timeout_s: float
) -> tuple[LLMResult, dict]:
    base = settings.cliproxy_base_url.rstrip("/")
    headers = _auth_headers()

    chat_body = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a strict JSON generator. Output JSON only. All strings in JSON must be Russian."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
    }

    attempts: list[tuple[str, dict]] = [
        (f"{base}/chat/completions", chat_body),
    ]

    last_err: Exception | None = None
    for url, body in attempts:
        try:
            r = await client.post(url, headers=headers, json=body, timeout=timeout_s)
            try:
                data = r.json()
            except Exception:
                data = {}
            if r.status_code >= 400:
                msg, provider_status = _extract_provider_error(data, fallback=r.text[:240])
                raise RuntimeError(
                    f"http={r.status_code}; provider_status={provider_status or '-'}; error={msg or 'unknown'}"
                )
            content = _extract_content_text(data)
            parsed = _try_parse_json(content or "")
            if not parsed:
                raise RuntimeError(f"failed to parse model output: {(content or '')[:200]}")
            meta = {
                "status_code": int(r.status_code),
                "rate_limits": extract_rate_limit_headers(r.headers),
                "usage": _extract_usage(data),
                "error": "",
                "provider_status": "",
                "reset_in_sec": int((extract_rate_limit_headers(r.headers).get("retry-after") or 0) or 0),
            }
            return LLMResult.model_validate(parsed), meta
        except Exception as e:
            last_err = e
            continue

    raise RuntimeError(f"llm endpoint attempts failed: {last_err}") from last_err


async def suggest_stopwords(
    *,
    text: str,
    existing_stopwords_text: str,
    options: LLMRoutingOptions | None = None,
) -> tuple[list[str], str]:
    """
    Suggest additional stopwords/phrases (Russian) after a human marks a message as NOT a lead.
    Returns (stopwords, model_used).
    """
    if not settings.cliproxy_base_url:
        raise RuntimeError("cliproxy_base_url is not set")
    if not settings.cliproxy_api_key:
        raise RuntimeError("cliproxy_api_key is not set")
    opts = options or LLMRoutingOptions()
    preferred_models = _parse_model_priority_text(opts.model_priority_text)
    max_attempts = max(1, int(opts.llm_max_attempts if opts.llm_max_attempts is not None else settings.llm_max_attempts))
    timeout_s = float(opts.llm_timeout_s if opts.llm_timeout_s is not None else settings.llm_timeout_s)
    models_cfg = _parse_models_config_json(opts.models_config_json)
    exclude_openai_owned_models = (
        bool(opts.exclude_openai_owned_models)
        if opts.exclude_openai_owned_models is not None
        else bool(settings.exclude_openai_owned_models)
    )

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
        candidates = _select_models(
            models,
            preferred_models=preferred_models,
            exclude_openai_owned_models=exclude_openai_owned_models,
        )
        if not candidates:
            raise RuntimeError("no available models")

        last_err: Exception | None = None
        for mid in candidates[:max_attempts]:
            try:
                cfg = _model_cfg(mid, models_cfg)
                if cfg.get("enabled") is False:
                    continue
                timeout_eff = timeout_s
                try:
                    if "timeout_s" in cfg:
                        timeout_eff = max(3.0, min(120.0, float(cfg.get("timeout_s"))))
                except Exception:
                    timeout_eff = timeout_s
                base = settings.cliproxy_base_url.rstrip("/")
                chat_body = {
                    "model": mid,
                    "messages": [
                        {
                            "role": "system",
                            "content": "You are a strict JSON generator. Output JSON only. All strings must be Russian.",
                        },
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.2,
                    "response_format": {"type": "json_object"},
                }
                attempts: list[tuple[str, dict]] = [
                    (f"{base}/chat/completions", chat_body),
                ]

                data = None
                for url, body in attempts:
                    try:
                        r = await client.post(url, headers=_auth_headers(), json=body, timeout=timeout_eff)
                        r.raise_for_status()
                        data = r.json()
                        break
                    except Exception:
                        continue
                if not data:
                    raise RuntimeError("stopwords endpoint attempts failed")

                content = _extract_content_text(data)
                parsed = _try_parse_json(content)
                if not parsed or "stopwords" not in parsed:
                    raise RuntimeError(f"failed to parse stopwords: {content[:200]}")
                arr = parsed.get("stopwords") or []
                if not isinstance(arr, list):
                    raise RuntimeError("stopwords is not a list")

                out: list[str] = []
                protected_substrings = [
                    "битрикс",
                    "bitrix",
                    "битрикс24",
                    "amocrm",
                    "amo crm",
                    "1с",
                    "crm",
                    "api",
                    "n8n",
                    "интеграц",
                    "разработ",
                    "внедрен",
                    "автоматиз",
                    "вебхук",
                    "webhook",
                ]
                seen: set[str] = set(x.casefold() for x in existing)
                for item in arr:
                    if not isinstance(item, str):
                        continue
                    w = item.strip()
                    if not w:
                        continue
                    if len(w) < 3:
                        continue
                    wc = w.casefold()
                    if any(ps in wc for ps in protected_substrings):
                        continue
                    if w.casefold() in seen:
                        continue
                    seen.add(wc)
                    out.append(w)
                    if len(out) >= 6:
                        break
                return out, mid
            except Exception as e:
                last_err = e
                continue

        raise RuntimeError(f"all stopword attempts failed: {last_err}") from last_err


def extract_rate_limit_headers(headers: httpx.Headers) -> dict[str, str]:
    # Common header names across providers/proxies.
    names = [
        "x-ratelimit-limit-requests",
        "x-ratelimit-remaining-requests",
        "x-ratelimit-limit-tokens",
        "x-ratelimit-remaining-tokens",
        "x-ratelimit-limit",
        "x-ratelimit-remaining",
        "retry-after",
    ]
    out: dict[str, str] = {}
    for n in names:
        v = headers.get(n)
        if v:
            out[n] = v
    return out


async def test_model_availability(
    *,
    model_id: str,
    timeout_s: float,
) -> dict:
    """
    Lightweight availability probe for a model.
    Returns status, latency and any exposed rate-limit headers.
    """
    if not settings.cliproxy_base_url:
        raise RuntimeError("cliproxy_base_url is not set")
    if not settings.cliproxy_api_key:
        raise RuntimeError("cliproxy_api_key is not set")

    base = settings.cliproxy_base_url.rstrip("/")
    url = f"{base}/chat/completions"
    body = {
        "model": model_id,
        "messages": [{"role": "user", "content": "Return JSON: {\"ok\":true}"}],
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }

    import time

    started = time.perf_counter()
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(url, headers=_auth_headers(), json=body, timeout=timeout_s)
        latency_ms = int((time.perf_counter() - started) * 1000)
        rate_headers = extract_rate_limit_headers(r.headers)
        payload = None
        try:
            payload = r.json()
        except Exception:
            payload = None
        content = _extract_content_text(payload or {}) if payload else ""
        err_msg = ""
        provider_status = ""
        if r.status_code >= 400:
            msg, ps = _extract_provider_error(payload or {}, fallback=(content or r.text[:240]))
            err_msg = msg
            provider_status = ps
        reset_in_sec = 0
        try:
            reset_in_sec = int(rate_headers.get("retry-after") or 0)
        except Exception:
            reset_in_sec = 0
        usage = _extract_usage(payload or {})
        return {
            "model_id": model_id,
            "ok": bool(r.status_code < 400),
            "status_code": int(r.status_code),
            "latency_ms": latency_ms,
            "error": "" if r.status_code < 400 else (err_msg[:240] if err_msg else (content[:240] if content else r.text[:240])),
            "rate_limits": rate_headers,
            "provider_status": provider_status,
            "reset_in_sec": reset_in_sec,
            "usage": usage,
        }
    except Exception as e:
        latency_ms = int((time.perf_counter() - started) * 1000)
        return {
            "model_id": model_id,
            "ok": False,
            "status_code": 0,
            "latency_ms": latency_ms,
            "error": str(e)[:240],
            "rate_limits": {},
            "provider_status": "",
            "reset_in_sec": 0,
            "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
        }
