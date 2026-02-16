from __future__ import annotations

from dataclasses import dataclass


def _normalize_lines(s: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for line in (s or "").splitlines():
        t = line.strip()
        if not t:
            continue
        t = t.casefold()
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


@dataclass(frozen=True)
class HardFilterResult:
    passed: bool
    matched_keywords: list[str]
    stopword_hit: str | None
    reason: str


def hard_filter(
    *,
    text: str,
    keywords_enabled: bool,
    stopwords_enabled: bool,
    keywords_text: str,
    stopwords_text: str,
) -> HardFilterResult:
    raw = (text or "").strip()
    t = raw.casefold()

    keywords = _normalize_lines(keywords_text)
    stopwords = _normalize_lines(stopwords_text)

    stop_hit: str | None = None
    if stopwords_enabled and stopwords:
        for sw in stopwords:
            if sw and sw in t:
                stop_hit = sw
                break
        if stop_hit:
            return HardFilterResult(
                passed=False,
                matched_keywords=[],
                stopword_hit=stop_hit,
                reason=f"stopword:{stop_hit}",
            )

    matched: list[str] = []
    if keywords_enabled and keywords:
        matched = [kw for kw in keywords if kw and kw in t]
        if not matched:
            return HardFilterResult(
                passed=False,
                matched_keywords=[],
                stopword_hit=None,
                reason="no_keywords",
            )

    return HardFilterResult(
        passed=True,
        matched_keywords=matched,
        stopword_hit=None,
        reason="passed",
    )

