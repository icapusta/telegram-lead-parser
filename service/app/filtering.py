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


@dataclass(frozen=True)
class IntentFilterResult:
    passed: bool
    reason: str


def request_intent_filter(*, text: str) -> IntentFilterResult:
    """
    Reject obvious "author offers own services" / ad-like posts.
    We only want inbound demand where someone is looking for an executor.
    """
    t = (text or "").casefold()

    request_markers = [
        "ищу",
        "нужен",
        "нужна",
        "нужны",
        "требуется",
        "кто может",
        "посоветуйте",
        "нужен подрядчик",
        "ищем исполнителя",
        "ищем разработчика",
        "нужна интеграция",
        "нужна доработка",
    ]
    offer_markers = [
        "предлагаю услуги",
        "оказываю услуги",
        "выполню",
        "сделаю",
        "беру заказы",
        "опыт",
        "стаж",
        "портфолио",
        "кейсы",
        "мой прайс",
        "ставка",
        "руб/час",
        "₽/час",
        "час",
        "пишите в лс",
        "пишите в личку",
        "telegram:",
        "связь:",
        "резюме",
        "ищу работу",
        "лиды с",
        "продам лиды",
        "лиды для",
        "контакты клиентов",
    ]

    req_hits = [m for m in request_markers if m in t]
    off_hits = [m for m in offer_markers if m in t]

    # Strong offer-like clues.
    has_price = ("₽" in t) or ("руб" in t and "час" in t)
    has_contact_cta = ("пишите" in t and ("лс" in t or "лич" in t)) or ("связь:" in t)

    # If message looks like self-promo and there is no clear request intent, reject.
    if len(off_hits) >= 2 and not req_hits:
        return IntentFilterResult(passed=False, reason="intent:self_offer")
    if (has_price and has_contact_cta) and not req_hits:
        return IntentFilterResult(passed=False, reason="intent:self_offer")

    return IntentFilterResult(passed=True, reason="passed")


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
