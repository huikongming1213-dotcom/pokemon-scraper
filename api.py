"""
Pokemon Card Price Scraper
全部 scraper 放同一個檔案，共用 _browser global，避免 circular import 問題
"""
from contextlib import asynccontextmanager
import asyncio
from html import unescape
import json
import os
import re
import time
import unicodedata
from datetime import datetime, timezone, timedelta
from urllib.parse import quote

import httpx
from fastapi import FastAPI, HTTPException, Query
from playwright.async_api import async_playwright, Playwright
from pydantic import BaseModel


# ── Browser lifecycle ──────────────────────────────────────────────────────

_pw: Playwright = None
_browser = None
_stealth_context = None  # browser context with anti-bot headers (for Cloudflare sites)

_STEALTH_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pw, _browser, _stealth_context
    _pw = await async_playwright().start()
    _browser = await _pw.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"],
    )
    _stealth_context = await _browser.new_context(
        user_agent=_STEALTH_UA,
        viewport={"width": 1280, "height": 800},
        locale="ja-JP",
    )
    yield
    await _stealth_context.close()
    await _browser.close()
    await _pw.stop()


app = FastAPI(title="Pokemon Card Price Scraper", lifespan=lifespan)


# ── Exchange rate (1hr cache, no Lock needed in single-threaded asyncio) ───

_rate_cache: dict = {}
RATE_FALLBACK = {"JPY_HKD": 0.052, "USD_HKD": 7.8}


async def _get_rates() -> dict:
    if _rate_cache.get("ts") and time.time() - _rate_cache["ts"] < 3600:
        return _rate_cache["rates"]
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get("https://api.exchangerate-api.com/v4/latest/JPY")
            r.raise_for_status()
            d = r.json()["rates"]
            rates = {
                "JPY_HKD": d.get("HKD", 0.052),
                "USD_HKD": d.get("HKD", 7.8) / d.get("USD", 1),
            }
            _rate_cache.update({"rates": rates, "ts": time.time()})
            return rates
    except Exception:
        return RATE_FALLBACK


def _jpy_to_hkd(jpy: int, rates: dict) -> int:
    return round(jpy * rates["JPY_HKD"])


def _usd_to_jpy(usd_amount: float, rates: dict) -> int:
    jpy_per_usd = rates["USD_HKD"] / max(rates["JPY_HKD"], 0.000001)
    return round(usd_amount * jpy_per_usd)


def _usd_to_twd(usd_amount: float, twd_hkd_rate: float, rates: dict) -> int:
    twd_per_usd = rates["USD_HKD"] / max(twd_hkd_rate, 0.000001)
    return round(usd_amount * twd_per_usd)


# ── Grade normalization ────────────────────────────────────────────────────

def _normalize_grade(grade: str) -> str:
    """
    統一 grade 格式，避免 SNKR Dunk / Card Rush 輸出格式不一致。
    "PSA 10" / "psa10" / "PSA10" → "PSA10"
    其他 → 原樣返回（例如 "raw", "状態B"）
    """
    if not grade:
        return "raw"
    g = re.sub(r"\s+", "", grade).upper()
    m = re.match(r"PSA(\d+)", g)
    if m:
        return f"PSA{m.group(1)}"
    m2 = re.match(r"BGS([\d.]+)", g)
    if m2:
        return f"BGS{m2.group(1)}"
    raw_condition = re.match(r"(?:RAW|状態)?([ABCD])(?:ランク)?$", g)
    if raw_condition:
        return raw_condition.group(1)
    if g in {"RAW", "UNGRADED", "未鑑定"}:
        return "raw"
    return grade  # 保留原格式（無法判斷嘅 grading 等）


# ── Card identity / quote helpers ─────────────────────────────────────────

_JP_NAME_MAP = {
    "pikachu": "ピカチュウ",
    "pikachu v": "ピカチュウV",
    "charizard": "リザードン",
    "charizard ex": "リザードンex",
    "mew": "ミュウ",
    "mewtwo": "ミュウツー",
    "eevee": "イーブイ",
    "umbreon": "ブラッキー",
    "sylveon": "ニンフィア",
    "rayquaza": "レックウザ",
}


def _compact_name_lookup(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", text or "").lower()
    return re.sub(r"[^0-9a-z]", "", normalized)


_JP_COMPACT_NAME_MAP = {_compact_name_lookup(name): jp_name for name, jp_name in _JP_NAME_MAP.items()}

_SET_CODE_MAP = {
    "amazing volt tackle": "S4",
    "仰天のボルテッカー": "S4",
}


def _has_japanese(text: str) -> bool:
    return bool(re.search(r"[\u3040-\u30ff\u3400-\u9fff]", text or ""))


def _normalize_card_identity(req) -> dict:
    card_name = (req.card_name or "").strip()
    card_number = re.sub(r"\s+", "", (req.card_number or "").replace("／", "/").strip())
    rarity = (req.rarity or "").strip().upper()
    set_name = (req.set_name or "").strip()

    lower_name = re.sub(r"\s+", " ", card_name.lower())
    jp_name = (
        card_name
        if _has_japanese(card_name)
        else _JP_NAME_MAP.get(lower_name, "") or _JP_COMPACT_NAME_MAP.get(_compact_name_lookup(card_name), "")
    )
    query_name = jp_name or card_name

    set_code = ""
    m = re.search(r"\b[Ss][A-Za-z0-9-]+\b", set_name)
    if m:
        set_code = m.group(0).upper()
    elif set_name:
        set_code = _SET_CODE_MAP.get(set_name.lower(), "")

    requirements = _quote_requirements(req)

    return {
        "display_name": card_name,
        "query_name": query_name,
        "jp_name": jp_name,
        "card_number": card_number,
        "rarity": rarity,
        "set_name": set_name,
        "set_code": set_code,
        "is_psa": requirements["grading_type"] == "psa",
        "psa_grade": req.psa_grade,
        "target_grade": requirements["label"],
        "quote_requirements": requirements,
        "query": " ".join(filter(None, [query_name, card_number, rarity])),
    }


def _median_int(values: list[int]) -> int:
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    if n % 2:
        return ordered[mid]
    return round((ordered[mid - 1] + ordered[mid]) / 2)


def _quantile_int(values: list[int], q: float) -> int:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    pos = (len(ordered) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(ordered) - 1)
    frac = pos - lo
    return round(ordered[lo] * (1 - frac) + ordered[hi] * frac)


def _summary_confidence(values: list[int], exact_count: int, basis: str) -> str:
    if not values:
        return "manual"
    if exact_count == 1:
        return "low"
    spread = max(values) / max(1, min(values))
    if exact_count >= 3 and spread <= 2.0 and basis.startswith("sold") and len(values) >= 2:
        return "high"
    if exact_count >= 3 and spread <= 3.0:
        return "medium"
    return "low"


def _clean_int(value) -> int | None:
    try:
        parsed = int(value)
        return parsed if 1 <= parsed <= 10 else None
    except (TypeError, ValueError):
        return None


def _raw_condition(value: str | None) -> str | None:
    if not value:
        return None
    normalized = _normalize_grade(value)
    return normalized if normalized in {"A", "B", "C", "D"} else None


def _quote_requirements(req) -> dict:
    intent = (getattr(req, "intent", "") or "").strip().lower()
    if intent not in {"buy", "sell"}:
        intent = "unknown"

    grading_type = (getattr(req, "grading_type", "") or "").strip().lower()
    if grading_type not in {"raw", "psa"}:
        grading_type = "psa" if getattr(req, "is_psa", False) else "raw"

    warnings = []
    if grading_type == "psa":
        exact_grade = _clean_int(getattr(req, "psa_grade", None))
        min_grade = _clean_int(getattr(req, "min_psa_grade", None))
        if intent == "buy" and min_grade:
            grades = [f"PSA{grade}" for grade in range(min_grade, 11)]
            label = f"PSA{min_grade}-PSA10"
        elif exact_grade:
            grades = [f"PSA{exact_grade}"]
            label = grades[0]
        else:
            grades = []
            label = "PSA（分數未提供）"
            warnings.append("missing_psa_grade")
        return {
            "intent": intent,
            "grading_type": "psa",
            "eligible_grades": grades,
            "label": label,
            "is_broad": len(grades) > 1,
            "warnings": warnings,
        }

    exact_condition = _raw_condition(getattr(req, "card_condition", None))
    min_condition = _raw_condition(getattr(req, "min_acceptable_condition", None))
    ordered = ["A", "B", "C", "D"]
    if intent == "buy" and min_condition:
        grades = ordered[:ordered.index(min_condition) + 1]
        label = f"RAW {'/'.join(grades)}"
    elif intent == "sell" and exact_condition:
        grades = [exact_condition]
        label = f"RAW {exact_condition}"
    else:
        grades = ["A", "B", "C"]
        label = "RAW A-C（卡況未確認）"
        warnings.append("missing_raw_condition")
    return {
        "intent": intent,
        "grading_type": "raw",
        "eligible_grades": grades,
        "label": label,
        "is_broad": len(grades) > 1,
        "warnings": warnings,
    }


_NON_CARD_TERMS = (
    "BOX", "ボックス", "パック", "スリーブ", "デッキ", "プレイマット",
    "カードケース", "オリパ", "福袋", "フィギュア", "ぬいぐるみ",
)


_CARD_NAME_SUFFIXES = ("VMAX", "VSTAR", "VUNION", "GX", "EX", "ex", "LVX", "V")


def _compact_card_name(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", text or "")
    return re.sub(r"[^0-9A-Za-z\u3040-\u30ff\u3400-\u9fff]", "", normalized)


def _strip_html(text: str) -> str:
    plain = re.sub(r"<[^>]+>", " ", text or "")
    return re.sub(r"\s+", " ", unescape(plain)).strip()


def _clean_marketplace_title(text: str) -> str:
    cleaned = (text or "").strip()
    cleaned = re.sub(r"\s+のサムネイル$", "", cleaned)
    cleaned = re.sub(r"\s+thumbnail$", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


def _split_card_name_suffix(card_name: str) -> tuple[str, str]:
    normalized = unicodedata.normalize("NFKC", card_name or "").strip()
    compact = _compact_card_name(card_name)
    for suffix in _CARD_NAME_SUFFIXES:
        if compact.endswith(suffix) and len(compact) > len(suffix):
            # Calyrex 等英文寶可夢名本身以 "ex" 結尾；英文 ex/EX 卡後綴應有分隔。
            if suffix in {"ex", "EX"}:
                raw_prefix = normalized[:-len(suffix)]
                if raw_prefix and raw_prefix[-1].isascii() and raw_prefix[-1].isalnum():
                    continue
            return compact[:-len(suffix)], suffix
    return compact, ""


def _card_name_matches(title: str, card_name: str) -> bool:
    """卡名後綴係 identity：V、VMAX、VSTAR、ex、EX、GX 不可互相代替。"""
    title_compact = _compact_card_name(title)
    base, target_suffix = _split_card_name_suffix(card_name)
    if not title_compact or not base:
        return False

    title_folded = title_compact.casefold()
    base_folded = base.casefold()
    start = 0
    while True:
        index = title_folded.find(base_folded, start)
        if index < 0:
            return False
        tail = title_compact[index + len(base):]
        if target_suffix:
            if tail.startswith(target_suffix):
                if target_suffix != "V" or not any(tail.startswith(longer) for longer in ("VMAX", "VSTAR", "VUNION")):
                    return True
        elif not any(tail.startswith(suffix) for suffix in _CARD_NAME_SUFFIXES):
            return True
        start = index + 1


def _listing_match(title: str, identity: dict) -> dict:
    text = (title or "").strip()
    upper = text.upper()
    reasons = []
    score = 0

    card_number = identity.get("card_number", "")
    number_match = bool(card_number and card_number.upper() in upper)
    if number_match:
        score += 4
        reasons.append("card_number")

    # 單卡 title 有時會帶「拡張パック」作為系列名；卡號 exact match 時唔當商品包裝排除。
    excluded_term = next((term for term in _NON_CARD_TERMS if term.upper() in upper), None)
    if excluded_term and not number_match:
        return {"score": 0, "eligible": False, "reasons": [f"non_card:{excluded_term}"]}

    query_name = identity.get("query_name", "")
    name_match = bool(query_name and _card_name_matches(text, query_name))
    if name_match:
        score += 2
        reasons.append("card_name")

    rarity = identity.get("rarity", "")
    if rarity and re.search(rf"(?<![A-Z0-9]){re.escape(rarity)}(?![A-Z0-9])", upper):
        score += 1
        reasons.append("rarity")

    set_code = identity.get("set_code", "")
    set_match = bool(set_code and set_code.upper() in upper)
    if set_match:
        score += 1
        reasons.append("set_code")

    # 保守模式：完整卡名 + 卡號必須同中；已知系列碼時亦必須命中。
    required_identity = number_match and (name_match or not query_name) and (set_match or not set_code)
    eligible = required_identity if card_number else (name_match and score >= 3 and (set_match or not set_code))
    if not eligible:
        reasons.append("identity_not_exact")
    return {"score": score, "eligible": eligible, "reasons": reasons}


def _collect_price_points(sources: dict, req, identity: dict) -> list[dict]:
    points = []
    requirements = _quote_requirements(req)

    if sources.get("yuyu_tei"):
        points.append({
            "source": "yuyu_tei",
            "grade": "raw",
            "raw_condition": None,
            "status": "shop_price",
            "metric": "single",
            "price_hkd": sources["yuyu_tei"]["price_hkd"],
            "count": 1,
            "match_score": 7,
            "match_reasons": ["dedicated_card_shop", "card_number"],
            "quote_eligible": requirements["grading_type"] == "raw" and "missing_raw_condition" in requirements["warnings"],
            "review_reason": "raw_condition_unknown",
        })

    for source_key in ("snkr_dunk", "card_rush", "magi", "yahoo_auctions", "mercari_jp", "mercari_tw"):
        source = sources.get(source_key)
        if not source:
            continue
        for listing in source.get("listings", []):
            match = _listing_match(listing.get("name", ""), identity)
            grade = _normalize_grade(listing.get("grade") or "raw")
            condition = grade if grade in {"A", "B", "C", "D"} else None
            raw_unknown_allowed = (
                grade == "raw"
                and requirements["grading_type"] == "raw"
                and "missing_raw_condition" in requirements["warnings"]
            )
            eligible_grade = grade in requirements["eligible_grades"] or raw_unknown_allowed
            points.append({
                "source": source_key,
                "grade": grade,
                "raw_condition": condition,
                "status": listing.get("status", "active"),
                "metric": "listing",
                "price_hkd": listing["price_hkd"],
                "count": 1,
                "name": listing.get("name", ""),
                "match_score": match["score"],
                "match_reasons": match["reasons"],
                "quote_eligible": match["eligible"] and eligible_grade,
                "review_reason": (
                    None if match["eligible"] and eligible_grade
                    else "raw_condition_unknown" if match["eligible"] and grade == "raw"
                    else "identity_or_grade_mismatch"
                ),
            })
        if not source.get("listings"):
            for grade, stats in source.get("by_grade", {}).items():
                points.append({
                    "source": source_key,
                    "grade": grade,
                    "status": "active",
                    "metric": "avg",
                    "price_hkd": stats["avg_hkd"],
                    "count": stats.get("count", 1),
                    "match_score": 0,
                    "match_reasons": ["aggregate_without_listing_identity"],
                    "quote_eligible": False,
                    "review_reason": "cannot_verify_listing_identity",
                })

    return points


def _build_summary(price_points: list[dict], req) -> dict | None:
    requirements = _quote_requirements(req)
    target_grade = requirements["label"]
    eligible = [p for p in price_points if p.get("quote_eligible") and p.get("price_hkd")]

    if not eligible:
        return {
            "target_grade": target_grade,
            "eligible_grades": requirements["eligible_grades"],
            "confidence": "manual",
            "reason": "no_verified_matching_data",
            "price_point_count": 0,
            "warnings": requirements["warnings"],
        }

    values = [p["price_hkd"] for p in eligible]
    sold_values = [p["price_hkd"] for p in eligible if p["status"] == "sold"]
    active_values = [p["price_hkd"] for p in eligible if p["status"] == "active"]
    sample_count = sum(p.get("count", 1) for p in eligible)
    source_count = len({p["source"] for p in eligible})

    if len(sold_values) >= 2:
        base = _median_int(sold_values)
        basis = "sold_median"
    elif sold_values and active_values:
        base = _median_int(values)
        basis = "verified_mixed_median"
    elif sold_values:
        base = sold_values[0]
        basis = "single_sold"
    elif len(active_values) >= 3:
        base = _quantile_int(active_values, 0.25)
        basis = "active_p25"
    elif active_values:
        base = _median_int(active_values)
        basis = "active_median"
    else:
        base = _median_int(values)
        basis = "exact_grade_median"

    confidence = _summary_confidence(values, sample_count, basis)
    if confidence == "high" and len(sold_values) < 2:
        confidence = "medium"
    if source_count < 2 or requirements["warnings"] or requirements["is_broad"]:
        confidence = "low"

    return {
        "target_grade": target_grade,
        "eligible_grades": requirements["eligible_grades"],
        "basis": basis,
        "market_base_hkd": base,
        "hkd_low": min(values),
        "hkd_high": max(values),
        "recommended_buy": round(base * 0.9),
        "recommended_sell": round(base * 1.05),
        "confidence": confidence,
        "price_point_count": len(eligible),
        "sample_count": sample_count,
        "source_count": source_count,
        "sources_used": sorted({p["source"] for p in eligible}),
        "warnings": requirements["warnings"],
        "boss_review_required": True,
    }


# ── DEBUG endpoint (臨時用，睇真實 HTML 結構) ─────────────────────────────

@app.get("/debug/html")
async def debug_html(url: str = Query(...)):
    """fetch 一個 URL，返回 rendered HTML + 所有含價格嘅文字，幫助 debug scraper selector"""
    page = await _browser.new_page()
    try:
        await page.goto(url, wait_until="networkidle", timeout=30000)

        body_text = await page.evaluate("() => document.body.innerText")
        inner_html = await page.evaluate("() => document.body.innerHTML")

        price_lines = [
            line.strip() for line in body_text.split("\n")
            if re.search(r"[¥￥円\$]|[\d,]{3,}", line) and line.strip()
        ][:50]

        all_classes = await page.evaluate("""() => {
            const els = document.querySelectorAll('[class]');
            const classes = new Set();
            els.forEach(el => el.className.toString().split(' ').forEach(c => c && classes.add(c)));
            return [...classes].filter(c => c.length > 2 && c.length < 50).slice(0, 100);
        }""")

        # 額外：dump 所有 input[name] 幫助識別 form fields
        form_fields = await page.evaluate("""() => {
            return [...document.querySelectorAll('input, select, textarea')]
                .map(el => ({ tag: el.tagName, name: el.name, id: el.id, type: el.type, placeholder: el.placeholder }))
                .filter(f => f.name || f.id);
        }""")

        return {
            "url": url,
            "title": await page.title(),
            "price_lines": price_lines,
            "all_classes": all_classes,
            "form_fields": form_fields,
            "html_snippet": inner_html[:3000],
        }
    except Exception as e:
        return {"error": str(e), "url": url}
    finally:
        await page.close()


# ── Helper: extract JPY prices from raw page text ─────────────────────────

async def _page_prices(page, min_val=200, max_val=5_000_000) -> list[int]:
    try:
        raw = await page.evaluate("""() => {
            const t = document.body.innerText || '';
            const m1 = t.match(/[¥￥][\\s]*([\\d,，]+)/g) || [];
            const m2 = t.match(/([\\d,，]+)[\\s]*円/g) || [];
            return [...m1, ...m2];
        }""")
        prices = []
        for token in (raw or []):
            digits = re.sub(r"[^\d]", "", token)
            if digits:
                val = int(digits)
                if min_val <= val <= max_val:
                    prices.append(val)
        return prices
    except Exception:
        return []


# ── Scraper 1: 遊々亭 (/buy/ = 店舖售價) ─────────────────────────────────
# FIX: 改用 /buy/poc/s/search（市場售價），加 rare= 參數過濾 rarity
# FIX: 搜尋字串同時帶 card_name，精確度更高

async def _scrape_yuyu_tei(card_number: str, card_name: str = "", rarity: str = "") -> dict:
    page = await _browser.new_page()
    try:
        # 組合搜尋字串：卡名 + 卡號（同 SNKR Dunk / Mercari 一樣）
        search_word = f"{card_name} {card_number}".strip() if card_name else card_number

        # 組 URL：rare= 參數直接 filter rarity（遊々亭支援）
        params = f"?search_word={quote(search_word)}"
        if rarity:
            params += f"&rare={quote(rarity)}"

        await page.goto(f"https://yuyu-tei.jp/buy/poc/s/search{params}")
        await page.wait_for_load_state("networkidle", timeout=25000)

        # card_name keywords 做二次 filter（取有意義的詞）
        name_keywords = [k for k in card_name.split() if len(k) > 1] if card_name else []

        cards = await page.query_selector_all(".card-product")
        for card in cards:
            name_el  = await card.query_selector("h4")
            price_el = await card.query_selector("strong.d-block")
            name      = (await name_el.inner_text()).strip()  if name_el  else None
            price_str = (await price_el.inner_text()).strip() if price_el else None
            if not price_str or not name:
                continue
            if name_keywords and not any(kw in name for kw in name_keywords):
                continue
            digits = re.sub(r"[^\d]", "", price_str)
            if digits:
                return {"price_jpy": int(digits), "name": name}

        return {}
    except Exception as e:
        raise RuntimeError(f"yuyu_tei failed: {type(e).__name__}: {e}")
    finally:
        await page.close()


# ── Scraper 2: SNKR Dunk (pure httpx, no Playwright) ──────────────────────
# FIX: search query 加入 rarity，避免出波鞋等無關結果

_SNKR_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Accept":     "application/json",
    "Referer":    "https://snkrdunk.com/",
}


def _grade_stats(listings: list[dict], grade_key: str) -> dict | None:
    items = [l for l in listings if l["grade"] == grade_key]
    if not items:
        return None
    prices = [l["price_jpy"] for l in items]
    return {
        "count":   len(items),
        "min_jpy": min(prices),
        "max_jpy": max(prices),
        "avg_jpy": round(sum(prices) / len(prices)),
    }


async def _scrape_snkr_dunk(card_number: str, card_name: str = "", rarity: str = "") -> dict:
    """
    純 httpx，唔使 Playwright。
    FIX: keyword = 卡名 + 卡號 + rarity，過濾無關商品（波鞋等）。
    返回所有 listing，並按 PSA grade 分組統計 min/max/avg。
    """
    # 組合搜尋字串，包含 rarity
    keyword_parts = " ".join(filter(None, [card_name, card_number, rarity]))
    q = quote(keyword_parts or card_number)

    url = (
        f"https://snkrdunk.com/v3/search"
        f"?func=all&refId=search&cardVersion=2&sortKey=default"
        f"&isDiscounted=false&isFirstHand=false&isUnderRetail=false"
        f"&stock=any&keyword={q}"
    )
    async with httpx.AsyncClient(timeout=15, headers=_SNKR_HEADERS) as client:
        r = await client.get(url)
        r.raise_for_status()
        data = r.json()

    products = data.get("search", {}).get("products", [])
    if not products:
        return {}

    listings = [
        {
            "price_jpy": p["salePrice"],
            # FIX: normalize grade 格式，統一 "PSA 10" → "PSA10" 等
            "grade":     _normalize_grade(p.get("condition") or "raw"),
            "name":      p.get("title", ""),
        }
        for p in products
        if p.get("salePrice", 0) > 0
        and (not card_number or card_number in (p.get("title") or ""))
        and (not card_name or _card_name_matches(p.get("title") or "", card_name))
    ]

    if not listings:
        return {}

    by_grade = {}
    for grade_key in sorted({listing["grade"] for listing in listings}):
        stats = _grade_stats(listings, grade_key)
        if stats:
            by_grade[grade_key] = stats

    all_prices = [l["price_jpy"] for l in listings]
    return {
        "listing_count": len(listings),
        "by_grade":      by_grade,
        "overall": {
            "min_jpy": min(all_prices),
            "max_jpy": max(all_prices),
            "avg_jpy": round(sum(all_prices) / len(all_prices)),
        },
        "listings": listings,
    }


# ── Scraper 3: Card Rush (Apify Cheerio + RESIDENTIAL proxy) ─────────────
# FIX: URL 同時帶 keyword（卡名）同 keyword2（型番/卡號），對應兩個獨立搜尋欄
# ⚠️  keyword2 係根據 Card Rush 表單結構估計，部署前請用 /debug/html 確認 field name

_CR_STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins', {get: () => {
    const arr = [1,2,3,4,5];
    arr.item = (i) => arr[i]; arr.namedItem = () => null; arr.refresh = () => {};
    return arr;
}});
Object.defineProperty(navigator, 'languages', {get: () => ['ja-JP','ja','en-US','en']});
Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 4});
Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});
window.chrome = {app:{isInstalled:false},runtime:{},loadTimes:()=>({}),csi:()=>({})};
const orig = window.Notification;
if (orig) Object.defineProperty(window, 'Notification', {get: () => orig});
"""


def _parse_cr_grade(name: str) -> str:
    """〔PSA10鑑定済〕→ PSA10、〔状態B〕→ 状態B、grade 唔明 → raw"""
    m = re.search(r"〔([^〕]+)〕", name)
    if not m:
        return "raw"
    tag = m.group(1)
    g = re.search(r"(PSA\d+|BGS[\d.]+|ARS\d+|PCG\d+|CGC\d+)", tag)
    if g:
        # FIX: normalize 統一格式
        return _normalize_grade(g.group(1))
    if "状態" in tag:
        return "状態" + tag.replace("状態", "").replace("※", "").strip()
    return tag.replace("鑑定済", "").replace("※状態難/", "").strip() or "raw"


async def _scrape_card_rush(card_number: str, card_name: str = "") -> dict:
    """
    Apify Cheerio Scraper + RESIDENTIAL proxy，bypass Cloudflare。
    FIX: URL 帶兩個 field：
      - keyword  = カード名/商品名（卡名）
      - keyword2 = 型番（卡號）
    ⚠️  請先用 /debug/html?url=https://www.cardrush-pokemon.jp/product-list
        確認 form field name（name 屬性），如唔係 keyword/keyword2 請更新。
    """
    apify_token = os.environ.get("APIFY_API_TOKEN", "")
    if not apify_token:
        raise RuntimeError("APIFY_API_TOKEN not set")

    name_filter   = card_name.split()[0] if card_name else ""
    # card_number 直接用原始值做 row-text filter（唔 quote，因為係 JS string 比較）
    number_filter = card_number  # e.g. "110/080"

    # 只帶卡名入 keyword，card number 靠 pageFunction 過濾 row text
    # （keyword2 field name 唔確定，直接喺 JS 側 check 更可靠）
    q_name = quote(card_name, safe="")
    search_url = (
        f"https://www.cardrush-pokemon.jp/product-list"
        f"?keyword={q_name}&Submit=%E6%A4%9C%E7%B4%A2"
    )

    page_func = f"""async function pageFunction(context) {{
        const {{ $ }} = context;
        const nameFilter   = {json.dumps(name_filter)};
        const numberFilter = {json.dumps(number_filter)};
        const results = [];
        $('.selling_price').each((i, el) => {{
            const li = $(el).closest('li');
            const name  = li.find('.goods_name').text().trim();
            const price = li.find('.figure').text().trim();
            const stock = li.find('.stock').text().trim();
            if (!price) return;
            // 卡名 filter（取第一個 keyword）
            if (nameFilter && !name.includes(nameFilter)) return;
            // 型番 filter：整行 text 必須包含卡號（e.g. "110/080"）
            if (numberFilter) {{
                const rowText = li.text();
                if (!rowText.includes(numberFilter)) return;
            }}
            results.push({{ name, price, stock }});
        }});
        return results;
    }}"""

    payload = {
        "startUrls": [{"url": search_url}],
        "pageFunction": page_func,
        "proxyConfiguration": {
            "useApifyProxy": True,
            "apifyProxyGroups": ["RESIDENTIAL"]
        }
    }

    async with httpx.AsyncClient(timeout=120) as client:
        r = await client.post(
            "https://api.apify.com/v2/acts/apify~cheerio-scraper/run-sync-get-dataset-items",
            params={"token": apify_token},
            json=payload,
        )
        r.raise_for_status()
        items = r.json()

    raw = []
    for page_result in items:
        if isinstance(page_result, list):
            raw.extend(page_result)
        elif isinstance(page_result, dict):
            raw.append(page_result)

    listings = []
    for item in raw:
        digits = re.sub(r"[^\d]", "", item.get("price", ""))
        if not digits:
            continue
        listings.append({
            "price_jpy": int(digits),
            "grade":     _parse_cr_grade(item.get("name", "")),
            "in_stock":  item.get("stock", "") != "×" and "在庫" in item.get("stock", ""),
            "name":      item.get("name", ""),
        })

    if not listings:
        return {}

    by_grade: dict[str, list[int]] = {}
    for l in listings:
        by_grade.setdefault(l["grade"], []).append(l["price_jpy"])

    grade_stats = {
        grade: {
            "count":   len(prices),
            "min_jpy": min(prices),
            "max_jpy": max(prices),
            "avg_jpy": round(sum(prices) / len(prices)),
        }
        for grade, prices in by_grade.items()
    }

    all_prices = [l["price_jpy"] for l in listings]
    return {
        "listing_count": len(listings),
        "by_grade":      grade_stats,
        "overall": {
            "min_jpy": min(all_prices),
            "max_jpy": max(all_prices),
            "avg_jpy": round(sum(all_prices) / len(all_prices)),
        },
        "listings": listings,
    }


# ── Scraper 4: magi marketplace listings ─────────────────────────────────

async def _scrape_magi(card_number: str, card_name: str = "", rarity: str = "") -> dict:
    """magi Pokemon 在售 + 已售 listings；保留逐項 title / URL 供 identity filter。"""
    page = await _stealth_context.new_page()
    try:
        # magi 多關鍵字係 OR search；rarity 會擴闊結果，所以只搜尋卡名 + 卡號，再逐項 AND filter。
        keyword = " ".join(filter(None, [card_name, card_number]))
        q = quote(keyword or card_number, safe="")
        raw_items = []
        for status_param, status in (("presented", "active"), ("sold_out", "sold")):
            url = (
                "https://magi.camp/items/search"
                f"?forms_search_items%5Bkeyword%5D={q}"
                "&forms_search_items%5Bgoods_id%5D=1"
                f"&forms_search_items%5Bstatus%5D={status_param}"
                "&forms_search_items%5Binclude_oripa%5D=false"
            )
            await page.goto(url, wait_until="domcontentloaded", timeout=35000)
            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                await page.wait_for_timeout(1500)
            status_items = await page.evaluate(r"""() => {
                return Array.from(document.querySelectorAll('.item-list__box')).slice(0, 40).map(box => {
                    const link = box.querySelector('a.item-list__link[href*="/items/"]');
                    const name = box.querySelector('.item-list__item-name')?.textContent?.trim() || '';
                    const priceText = box.querySelector('.item-list__price-box--price')?.textContent || '';
                    const price = parseInt(priceText.replace(/[^0-9]/g, ''), 10) || 0;
                    return { name, price_jpy: price, url: link?.href || '' };
                }).filter(item => item.name && item.price_jpy > 0 && item.url);
            }""")
            raw_items.extend({**item, "status": status} for item in status_items)

        listings = []
        for item in raw_items:
            title = item["name"]
            if card_number and card_number not in title:
                continue
            if card_name and not _card_name_matches(title, card_name):
                continue
            psa = re.search(r"PSA\s*(\d+)", item["name"], re.IGNORECASE)
            listings.append({
                **item,
                "grade": f"PSA{psa.group(1)}" if psa else "raw",
            })

        if not listings:
            return {}

        by_grade = {}
        for grade in sorted({listing["grade"] for listing in listings}):
            stats = _grade_stats(listings, grade)
            if stats:
                by_grade[grade] = stats

        prices = [listing["price_jpy"] for listing in listings]
        return {
            "listing_count": len(listings),
            "by_grade": by_grade,
            "overall": {
                "min_jpy": min(prices),
                "max_jpy": max(prices),
                "avg_jpy": round(sum(prices) / len(prices)),
            },
            "listings": listings,
        }
    except Exception as e:
        raise RuntimeError(f"magi failed: {type(e).__name__}: {e}")
    finally:
        await page.close()


# ── Scraper 5: Yahoo Auctions Japan ───────────────────────────────────────

async def _scrape_yahoo_auctions(card_number: str, card_name: str = "", rarity: str = "") -> dict:
    """
    Yahoo!オークション 在售搜尋頁。
    改用 httpx 直接抓 search HTML，避免 Zeabur Playwright timeout。
    """
    try:
        keyword_parts = " ".join(filter(None, [card_name, card_number, rarity]))
        q = quote(keyword_parts or card_number, safe="")
        url = f"https://auctions.yahoo.co.jp/search/search?p={q}&auccat=0"
        headers = {
            "User-Agent": _STEALTH_UA,
            "Accept-Language": "ja-JP,ja;q=0.9,en;q=0.8",
            "Referer": "https://auctions.yahoo.co.jp/",
        }
        async with httpx.AsyncClient(timeout=20, headers=headers, follow_redirects=True) as client:
            response = await client.get(url)
        if response.status_code not in {200, 404}:
            raise RuntimeError(f"http_{response.status_code}")

        html = response.text
        raw_items = []
        seen_urls = set()
        for match in re.finditer(r"<a\b([^>]*href=\"([^\"]*/jp/auction/[^\"]+)\"[^>]*)>(.*?)</a>", html, re.IGNORECASE | re.DOTALL):
            attrs, href, body = match.groups()
            price_match = re.search(r'data-auction-price="([^"]+)"', attrs)
            if not price_match:
                continue
            digits = re.sub(r"[^\d]", "", price_match.group(1))
            if not digits:
                continue
            absolute_url = href if href.startswith("http") else f"https://auctions.yahoo.co.jp{href}"
            if absolute_url in seen_urls:
                continue
            seen_urls.add(absolute_url)

            title = ""
            for candidate in (
                re.search(r'alt="([^"]+)"', body, re.IGNORECASE),
                re.search(r'aria-label="([^"]+)"', attrs, re.IGNORECASE),
            ):
                if candidate:
                    title = unescape(candidate.group(1)).strip()
                    break
            if not title:
                title = _strip_html(body)
            if not title:
                continue

            raw_items.append({
                "url": absolute_url,
                "name": title,
                "price_jpy": int(digits),
                "is_flea": 'data-auction-isflea="' in attrs.lower(),
                "is_free_shipping": 'data-auction-isfreeshipping="' in attrs.lower(),
            })
            if len(raw_items) >= 40:
                break

        listings = []
        for item in raw_items:
            title = item.get("name", "")
            if card_number and card_number.upper() not in title.upper():
                continue
            if card_name and not _card_name_matches(title, card_name):
                continue
            psa = re.search(r"PSA\s*(\d+)", title, re.IGNORECASE)
            listings.append({
                **item,
                "grade": f"PSA{psa.group(1)}" if psa else "raw",
                "status": "active",
                "marketplace": "auction",
            })

        if not listings:
            return {}

        by_grade = {}
        for grade in sorted({listing["grade"] for listing in listings}):
            stats = _grade_stats(listings, grade)
            if stats:
                by_grade[grade] = stats

        prices = [listing["price_jpy"] for listing in listings]
        return {
            "listing_count": len(listings),
            "by_grade": by_grade,
            "overall": {
                "min_jpy": min(prices),
                "max_jpy": max(prices),
                "avg_jpy": round(sum(prices) / len(prices)),
            },
            "listings": listings,
        }
    except Exception as e:
        raise RuntimeError(f"yahoo_auctions failed: {type(e).__name__}: {e}")


# ── Scraper 6: Mercari JP / TW ────────────────────────────────────────────


async def _get_twd_hkd_rate() -> float:
    cache_key = "TWD_HKD"
    if _rate_cache.get(cache_key + "_ts") and time.time() - _rate_cache[cache_key + "_ts"] < 3600:
        return _rate_cache[cache_key]
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.get("https://api.exchangerate-api.com/v4/latest/TWD")
            r.raise_for_status()
            rate = r.json()["rates"].get("HKD", 0.24)
            _rate_cache[cache_key] = rate
            _rate_cache[cache_key + "_ts"] = time.time()
            return rate
    except Exception:
        return 0.24


def _priced_stats(prices: list[int], prefix: str) -> dict:
    return {
        "count": len(prices),
        f"min_{prefix}": min(prices),
        f"max_{prefix}": max(prices),
        f"avg_{prefix}": round(sum(prices) / len(prices)),
    }


def _grouped_market_stats(listings: list[dict], *, price_key: str, prefix: str) -> tuple[dict, dict]:
    by_grade = {}
    for grade in sorted({listing["grade"] for listing in listings}):
        grade_items = [listing[price_key] for listing in listings if listing["grade"] == grade]
        if grade_items:
            by_grade[grade] = _priced_stats(grade_items, prefix)
    overall_prices = [listing[price_key] for listing in listings]
    overall = _priced_stats(overall_prices, prefix)
    return by_grade, overall


async def _scrape_mercari_marketplace(
    *,
    base_url: str,
    currency: str,
    card_number: str,
    card_name: str = "",
    rarity: str = "",
    referer: str,
    sold_tokens: list[str],
) -> dict:
    page = await _stealth_context.new_page()
    try:
        await page.add_init_script(_CR_STEALTH_SCRIPT)
        await page.set_extra_http_headers({"Referer": referer})

        keyword_parts = " ".join(filter(None, [card_name, card_number, rarity]))
        q = quote(keyword_parts or card_number, safe="")

        await page.goto(
            base_url.format(query=q),
            wait_until="domcontentloaded",
            timeout=25000,
        )
        await page.wait_for_timeout(2200)
        await page.mouse.wheel(0, 1400)
        await page.wait_for_timeout(1200)

        payload = await page.evaluate(r"""(config) => {
            const soldTokens = (config.soldTokens || []).map(token => token.toLowerCase());
            const noisePatterns = [
                /^NT\$\s*[\d,]+$/i,
                /^US\$\s*[\d,.]+$/i,
                /^[\u00A5\uFFE5]\s*[\d,]+$/i,
                /^[\d,]+\s*\u5186$/i,
                /^(SOLD|Sold out|\u5df2\u552e\u51fa|\u58f2\u308a\u5207\u308c)$/i,
            ];
            const blockedText = (document.body.innerText || '').toLowerCase();
            if (
                blockedText.includes('access denied') ||
                blockedText.includes('unusual traffic') ||
                blockedText.includes('captcha') ||
                blockedText.includes('verify you are human')
            ) {
                return { status: 'blocked', reason: 'challenge_page' };
            }

            const items = [];
            const seen = new Set();
            for (const link of Array.from(document.querySelectorAll('a[href*="/item/"]'))) {
                const href = link.getAttribute('href') || '';
                const abs = href ? new URL(href, location.origin).href : '';
                if (!abs || seen.has(abs)) continue;
                seen.add(abs);

                let container = link;
                for (let i = 0; i < 4 && container.parentElement; i++) {
                    container = container.parentElement;
                }

                const text = (container.innerText || link.innerText || '').replace(/\u00a0/g, ' ').trim();
                if (!text) continue;

                const prices = [];
                const addPrice = (kind, raw) => {
                    if (!raw) return;
                    const normalized = String(raw).replace(/,/g, '').trim();
                    if (!normalized) return;
                    if (kind === 'USD') {
                        const parsed = parseFloat(normalized);
                        if (!Number.isNaN(parsed) && parsed > 0) prices.push({ kind, value: parsed });
                        return;
                    }
                    if (/^\d+$/.test(normalized)) {
                        const parsed = parseInt(normalized, 10);
                        if (parsed > 0) prices.push({ kind, value: parsed });
                    }
                };

                const linesForPrice = text.split(/\n+/).map(line => line.replace(/\s+/g, ' ').trim()).filter(Boolean);
                for (let i = 0; i < linesForPrice.length; i++) {
                    const line = linesForPrice[i];
                    const next = linesForPrice[i + 1] || '';
                    const twInline = line.match(/NT\$\s*([\d,]+)/i);
                    if (twInline) addPrice('TWD', twInline[1]);
                    const jpInline = line.match(/[\u00A5\uFFE5]\s*([\d,]+)/i) || line.match(/([\d,]+)\s*\u5186/i);
                    if (jpInline) addPrice('JPY', jpInline[1]);
                    const usdInline = line.match(/US\$\s*([\d,.]+)/i);
                    if (usdInline) addPrice('USD', usdInline[1]);
                    if (/^US\$$/i.test(line) && next) addPrice('USD', next);
                    if (/^NT\$$/i.test(line) && next) addPrice('TWD', next);
                    if (/^[\u00A5\uFFE5]$/.test(line) && next) addPrice('JPY', next);
                    if (/^\u5186$/.test(line) && next) addPrice('JPY', next);
                    if (prices.length >= 3) break;
                }

                const chosenPrice = prices[0] || null;
                if (!chosenPrice) continue;

                const lowerText = text.toLowerCase();
                const isSold = soldTokens.some(token => lowerText.includes(token));
                const imgAlt = link.querySelector('img[alt]')?.getAttribute('alt')?.trim() || '';
                const aria = link.getAttribute('aria-label')?.trim() || '';
                const lines = text
                    .split(/\n+/)
                    .map(line => line.replace(/\s+/g, ' ').trim())
                    .filter(line => line)
                    .filter(line => !noisePatterns.some(pattern => pattern.test(line)));
                const title = (imgAlt || aria || lines[0] || '').replace(/\s+\u306e\u30b5\u30e0\u30cd\u30a4\u30eb$/, '').trim();
                if (!title) continue;

                items.push({
                    url: abs,
                    name: title,
                    price: chosenPrice.value,
                    price_currency: chosenPrice.kind,
                    is_sold: isSold,
                });
                if (items.length >= 40) break;
            }

            return { status: 'ok', items };
        }""", {"currency": currency, "soldTokens": sold_tokens})

        if payload.get("status") == "blocked":
            return payload

        raw_items = payload.get("items", [])
        listings = []
        rates = None
        twd_hkd_rate = None
        for item in raw_items:
            title = _clean_marketplace_title(item.get("name", ""))
            if card_number and card_number.upper() not in title.upper():
                continue
            if card_name and not _card_name_matches(title, card_name):
                continue
            psa_match = re.search(r"PSA\s*(\d+)", title, re.IGNORECASE)
            raw_price = item.get("price")
            price_currency = item.get("price_currency", currency)
            if raw_price in (None, ""):
                continue
            if price_currency == "USD":
                rates = rates or await _get_rates()
                if currency == "JPY":
                    normalized_price = _usd_to_jpy(float(raw_price), rates)
                else:
                    twd_hkd_rate = twd_hkd_rate or await _get_twd_hkd_rate()
                    normalized_price = _usd_to_twd(float(raw_price), twd_hkd_rate, rates)
            else:
                normalized_price = int(round(float(raw_price)))
            listing = {
                "name": title,
                "url": item.get("url", ""),
                "grade": f"PSA{psa_match.group(1)}" if psa_match else "raw",
                "status": "sold" if item.get("is_sold") else "active",
            }
            if currency == "TWD":
                listing["price_twd"] = normalized_price
            else:
                listing["price_jpy"] = normalized_price
            listings.append(listing)

        if not listings:
            return {"status": "no_results"}

        if currency == "TWD":
            by_grade, overall = _grouped_market_stats(listings, price_key="price_twd", prefix="twd")
            return {
                "status": "ok",
                "currency": "TWD",
                "listing_count": len(listings),
                "by_grade": by_grade,
                "overall": overall,
                "listings": listings,
            }

        by_grade, overall = _grouped_market_stats(listings, price_key="price_jpy", prefix="jpy")
        return {
            "status": "ok",
            "currency": "JPY",
            "listing_count": len(listings),
            "by_grade": by_grade,
            "overall": overall,
            "listings": listings,
        }

    except Exception as e:
        marketplace = "mercari_tw" if currency == "TWD" else "mercari_jp"
        raise RuntimeError(f"{marketplace} failed: {type(e).__name__}: {e}")
    finally:
        await page.close()


async def _scrape_mercari_tw(card_number: str, card_name: str = "", rarity: str = "") -> dict:
    result = await _scrape_mercari_marketplace(
        base_url="https://tw.mercari.com/zh-hant/search?keyword={query}",
        currency="TWD",
        card_number=card_number,
        card_name=card_name,
        rarity=rarity,
        referer="https://www.google.com.tw/",
        sold_tokens=["已售出", "sold", "sold out"],
    )
    if result.get("status") != "ok":
        return result

    twd_rate = await _get_twd_hkd_rate()
    result["twd_hkd_rate"] = twd_rate
    return result


async def _scrape_mercari_jp(card_number: str, card_name: str = "", rarity: str = "") -> dict:
    return await _scrape_mercari_marketplace(
        base_url="https://jp.mercari.com/search?keyword={query}",
        currency="JPY",
        card_number=card_number,
        card_name=card_name,
        rarity=rarity,
        referer="https://www.google.co.jp/",
        sold_tokens=["売り切れ", "sold", "sold out"],
    )


# ── GET /search  (原有 endpoint，保留唔改) ────────────────────────────────

async def scrape_cards(card_number: str) -> list[dict]:
    page = await _browser.new_page()
    try:
        await page.goto("https://yuyu-tei.jp/sell/poc/s/search")
        inputs = page.locator('input[name="search_word"]')
        for i in range(await inputs.count()):
            inp = inputs.nth(i)
            if await inp.is_visible():
                await inp.fill(card_number)
                await inp.press("Enter")
                break
        await page.wait_for_load_state("networkidle", timeout=15000)
        cards = await page.query_selector_all(".card-product")
        results = []
        for card in cards:
            name_el  = await card.query_selector("h4")
            price_el = await card.query_selector("strong.d-block")
            results.append({
                "name":  (await name_el.inner_text()).strip()  if name_el  else None,
                "price": (await price_el.inner_text()).strip() if price_el else None,
            })
        return results
    finally:
        await page.close()


@app.get("/search")
async def search(
    cardNumber: str = Query(..., example="234/193"),
    rarity:     str = Query(..., example="SAR"),
):
    try:
        return await scrape_cards(cardNumber)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── GET /mercari ──────────────────────────────────────────────────────────

@app.get("/magi")
async def magi_search(
    cardNumber: str = Query(..., example="030/100"),
    cardName:   str = Query(default="", example="ピカチュウV"),
    rarity:     str = Query(default="", example="RR"),
):
    """magi 在售 + 已售單卡，返回逐項 title / URL / grade / JPY + HKD。"""
    try:
        rates, result = await asyncio.gather(_get_rates(), _scrape_magi(cardNumber, cardName, rarity))
        if not result:
            return {"card_number": cardNumber, "message": "no results found"}
        for stats in result.get("by_grade", {}).values():
            stats["min_hkd"] = _jpy_to_hkd(stats["min_jpy"], rates)
            stats["max_hkd"] = _jpy_to_hkd(stats["max_jpy"], rates)
            stats["avg_hkd"] = _jpy_to_hkd(stats["avg_jpy"], rates)
        for listing in result.get("listings", []):
            listing["price_hkd"] = _jpy_to_hkd(listing["price_jpy"], rates)
        result["exchange_rates"] = rates
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── GET /mercari ──────────────────────────────────────────────────────────

@app.get("/mercari")
async def mercari_search(
    cardNumber: str = Query(..., example="288/SM-P"),
    cardName:   str = Query(default="", example="リザードンex"),
    rarity:     str = Query(default="", example="SAR"),
    market:     str = Query(default="tw", example="tw"),
):
    """Mercari 獨立查詢；預設 TW，可用 market=jp 切日本站。"""
    try:
        selected = (market or "tw").strip().lower()
        if selected == "jp":
            rates, result = await asyncio.gather(_get_rates(), _scrape_mercari_jp(cardNumber, cardName, rarity))
            status = result.get("status") if result else None
            if status == "no_results":
                return {"card_number": cardNumber, "message": "no results found"}
            if status == "blocked":
                return {"card_number": cardNumber, "message": "scraper blocked", "reason": result.get("reason")}
            if not result:
                return {"card_number": cardNumber, "message": "no results found"}
            for stats in result.get("by_grade", {}).values():
                stats["min_hkd"] = _jpy_to_hkd(stats["min_jpy"], rates)
                stats["max_hkd"] = _jpy_to_hkd(stats["max_jpy"], rates)
                stats["avg_hkd"] = _jpy_to_hkd(stats["avg_jpy"], rates)
            overall = result["overall"]
            overall["min_hkd"] = _jpy_to_hkd(overall["min_jpy"], rates)
            overall["max_hkd"] = _jpy_to_hkd(overall["max_jpy"], rates)
            overall["avg_hkd"] = _jpy_to_hkd(overall["avg_jpy"], rates)
            result["listings"] = [
                {**listing, "price_hkd": _jpy_to_hkd(listing["price_jpy"], rates), "currency": "JPY"}
                for listing in result.get("listings", [])
            ]
            result["exchange_rates"] = rates
            return result

        result = await _scrape_mercari_tw(cardNumber, cardName, rarity)
        status = result.get("status") if result else None
        if status == "no_results":
            return {"card_number": cardNumber, "message": "no results found"}
        if status == "blocked":
            return {"card_number": cardNumber, "message": "scraper blocked", "reason": result.get("reason")}
        if not result:
            return {"card_number": cardNumber, "message": "no results found"}
        twd_rate = result.get("twd_hkd_rate", 0.24)
        for stats in result.get("by_grade", {}).values():
            stats["min_hkd"] = round(stats["min_twd"] * twd_rate)
            stats["max_hkd"] = round(stats["max_twd"] * twd_rate)
            stats["avg_hkd"] = round(stats["avg_twd"] * twd_rate)
        overall = result["overall"]
        overall["min_hkd"] = round(overall["min_twd"] * twd_rate)
        overall["max_hkd"] = round(overall["max_twd"] * twd_rate)
        overall["avg_hkd"] = round(overall["avg_twd"] * twd_rate)
        result["listings"] = [
            {**listing, "price_hkd": round(listing["price_twd"] * twd_rate), "currency": "TWD"}
            for listing in result.get("listings", [])
        ]
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── GET /card-rush ────────────────────────────────────────────────────────

@app.get("/card-rush")
async def card_rush_search(
    cardNumber: str = Query(..., example="288/SM-P"),
    cardName:   str = Query(default="", example="リザードンex"),
):
    """Card Rush 獨立查詢，返回各 grade min/max/avg（含 HKD 換算）及在庫狀態。"""
    try:
        rates, result = await asyncio.gather(_get_rates(), _scrape_card_rush(cardNumber, cardName))
        if not result:
            return {"card_number": cardNumber, "message": "no results found"}
        for stats in result.get("by_grade", {}).values():
            stats["min_hkd"] = _jpy_to_hkd(stats["min_jpy"], rates)
            stats["max_hkd"] = _jpy_to_hkd(stats["max_jpy"], rates)
            stats["avg_hkd"] = _jpy_to_hkd(stats["avg_jpy"], rates)
        ov = result["overall"]
        ov["min_hkd"] = _jpy_to_hkd(ov["min_jpy"], rates)
        ov["max_hkd"] = _jpy_to_hkd(ov["max_jpy"], rates)
        ov["avg_hkd"] = _jpy_to_hkd(ov["avg_jpy"], rates)
        result["exchange_rates"] = rates
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── GET /yahoo-auctions ────────────────────────────────────────────────────

@app.get("/yahoo-auctions")
async def yahoo_auctions_search(
    cardNumber: str = Query(..., example="030/100"),
    cardName:   str = Query(default="", example="ピカチュウV"),
    rarity:     str = Query(default="", example="RR"),
):
    """Yahoo!オークション獨立查詢，返回 active listings 分級統計（含 HKD 換算）。"""
    try:
        rates, result = await asyncio.gather(_get_rates(), _scrape_yahoo_auctions(cardNumber, cardName, rarity))
        if not result:
            return {"card_number": cardNumber, "message": "no results found"}
        for stats in result.get("by_grade", {}).values():
            stats["min_hkd"] = _jpy_to_hkd(stats["min_jpy"], rates)
            stats["max_hkd"] = _jpy_to_hkd(stats["max_jpy"], rates)
            stats["avg_hkd"] = _jpy_to_hkd(stats["avg_jpy"], rates)
        ov = result["overall"]
        ov["min_hkd"] = _jpy_to_hkd(ov["min_jpy"], rates)
        ov["max_hkd"] = _jpy_to_hkd(ov["max_jpy"], rates)
        ov["avg_hkd"] = _jpy_to_hkd(ov["avg_jpy"], rates)
        result["listings"] = [
            {
                **listing,
                "price_hkd": _jpy_to_hkd(listing["price_jpy"], rates),
                "currency": "JPY",
            }
            for listing in result.get("listings", [])
        ]
        result["exchange_rates"] = rates
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── GET /snkr-dunk ────────────────────────────────────────────────────────

@app.get("/snkr-dunk")
async def snkr_dunk_search(
    cardNumber: str = Query(..., example="288/SM-P"),
    cardName:   str = Query(default="", example="リザードンex"),
    rarity:     str = Query(default="", example="SAR"),
):
    """SNKR Dunk 獨立查詢，返回 PSA8/PSA9/PSA10/raw 分組 min/max/avg（含 HKD 換算）。"""
    try:
        rates, result = await asyncio.gather(_get_rates(), _scrape_snkr_dunk(cardNumber, cardName, rarity))
        if not result:
            return {"card_number": cardNumber, "message": "no results found"}
        for grade, stats in result.get("by_grade", {}).items():
            stats["min_hkd"] = _jpy_to_hkd(stats["min_jpy"], rates)
            stats["max_hkd"] = _jpy_to_hkd(stats["max_jpy"], rates)
            stats["avg_hkd"] = _jpy_to_hkd(stats["avg_jpy"], rates)
        ov = result["overall"]
        ov["min_hkd"] = _jpy_to_hkd(ov["min_jpy"], rates)
        ov["max_hkd"] = _jpy_to_hkd(ov["max_jpy"], rates)
        ov["avg_hkd"] = _jpy_to_hkd(ov["avg_jpy"], rates)
        result["exchange_rates"] = rates
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── GET /yuyu-tei ─────────────────────────────────────────────────────────

@app.get("/yuyu-tei")
async def yuyu_tei_search(
    cardNumber: str = Query(..., example="110/080"),
    cardName:   str = Query(default="", example="メガリザードンXex"),
    rarity:     str = Query(default="", example="SAR"),
):
    """遊々亭獨立查詢（/buy/ 市場售價），支援 rarity filter。"""
    try:
        rates, result = await asyncio.gather(_get_rates(), _scrape_yuyu_tei(cardNumber, cardName, rarity))
        if not result:
            return {"card_number": cardNumber, "message": "no results found"}
        result["price_hkd"] = _jpy_to_hkd(result["price_jpy"], rates)
        result["exchange_rates"] = rates
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── POST /price-report ────────────────────────────────────────────────────

class PriceReportRequest(BaseModel):
    card_name:   str
    card_number: str
    set_name:    str = ""
    rarity:      str = ""
    intent:       str = ""
    grading_type: str = ""
    is_psa:      bool = False
    psa_grade:   int | None = None
    min_psa_grade: int | None = None
    card_condition: str | None = None
    min_acceptable_condition: str | None = None


HKT = timezone(timedelta(hours=8))


@app.post("/price-report")
async def price_report(req: PriceReportRequest):
    errors = {}

    async def _safe(coro, key, timeout=30):
        try:
            return await asyncio.wait_for(coro, timeout=timeout) or {}
        except Exception as e:
            errors[key] = str(e) or type(e).__name__
            return {}

    identity = _normalize_card_identity(req)
    rates = await _get_rates()

    # FIX: 全部 scraper 都傳入 rarity；日站優先用 normalized 日文名查詢
    query_name = identity["query_name"]
    card_number = identity["card_number"]
    rarity = identity["rarity"]
    yuyu, snkr, rush, magi, yahoo, merc_jp, merc_tw = await asyncio.gather(
        _safe(_scrape_yuyu_tei(card_number, query_name, rarity),   "yuyu_tei",  timeout=35),
        _safe(_scrape_snkr_dunk(card_number, query_name, rarity),  "snkr_dunk", timeout=20),
        _safe(_scrape_card_rush(card_number, query_name),          "card_rush", timeout=120),
        _safe(_scrape_magi(card_number, query_name, rarity),       "magi",      timeout=40),
        _safe(_scrape_yahoo_auctions(card_number, query_name, rarity), "yahoo_auctions", timeout=40),
        _safe(_scrape_mercari_jp(card_number, query_name, rarity), "mercari_jp", timeout=45),
        _safe(_scrape_mercari_tw(card_number, query_name, rarity), "mercari_tw", timeout=45),
    )

    # ── 組裝 sources ──
    def _yuyu_out(r):
        if not r.get("price_jpy"): return None
        return {
            "price_jpy": r["price_jpy"],
            "price_hkd": _jpy_to_hkd(r["price_jpy"], rates),
            "name": r.get("name"),
            "currency": "JPY",
            "grading_scope": "raw_only",
            "trust": "verified_card_shop",
        }

    def _jpy_listings_out(r):
        if not r.get("overall"): return None
        by_grade_hkd = {}
        for grade, stats in r.get("by_grade", {}).items():
            by_grade_hkd[grade] = {
                **stats,
                "min_hkd": _jpy_to_hkd(stats["min_jpy"], rates),
                "max_hkd": _jpy_to_hkd(stats["max_jpy"], rates),
                "avg_hkd": _jpy_to_hkd(stats["avg_jpy"], rates),
            }
        ov = r["overall"]
        listings_hkd = [
            {
                **l,
                "price_hkd": _jpy_to_hkd(l["price_jpy"], rates),
                "currency": "JPY",
            }
            for l in r.get("listings", [])
        ]
        return {
            "listing_count": r["listing_count"],
            "by_grade":      by_grade_hkd,
            "overall": {
                **ov,
                "min_hkd": _jpy_to_hkd(ov["min_jpy"], rates),
                "max_hkd": _jpy_to_hkd(ov["max_jpy"], rates),
                "avg_hkd": _jpy_to_hkd(ov["avg_jpy"], rates),
            },
            "listings": listings_hkd,
            "currency": "JPY",
            "trust": "listing_identity_checked",
        }

    def _rush_out(r):
        if not r.get("overall"): return None
        by_grade_hkd = {}
        for grade, stats in r.get("by_grade", {}).items():
            by_grade_hkd[grade] = {
                **stats,
                "min_hkd": _jpy_to_hkd(stats["min_jpy"], rates),
                "max_hkd": _jpy_to_hkd(stats["max_jpy"], rates),
                "avg_hkd": _jpy_to_hkd(stats["avg_jpy"], rates),
            }
        ov = r["overall"]
        listings_hkd = [
            {
                **l,
                "price_hkd": _jpy_to_hkd(l["price_jpy"], rates),
                "currency": "JPY",
            }
            for l in r.get("listings", [])
        ]
        return {
            "listing_count": r["listing_count"],
            "by_grade":      by_grade_hkd,
            "overall": {
                **ov,
                "min_hkd": _jpy_to_hkd(ov["min_jpy"], rates),
                "max_hkd": _jpy_to_hkd(ov["max_jpy"], rates),
                "avg_hkd": _jpy_to_hkd(ov["avg_jpy"], rates),
            },
            "listings": listings_hkd,
            "currency": "JPY",
            "trust": "listing_identity_checked",
        }

    def _twd_listings_out(r):
        if not r.get("overall"): return None
        twd_rate = r.get("twd_hkd_rate", 0.24)
        by_grade_hkd = {}
        for grade, stats in r.get("by_grade", {}).items():
            by_grade_hkd[grade] = {
                **stats,
                "min_hkd": round(stats["min_twd"] * twd_rate),
                "max_hkd": round(stats["max_twd"] * twd_rate),
                "avg_hkd": round(stats["avg_twd"] * twd_rate),
            }
        ov = r["overall"]
        listings_hkd = [
            {
                **l,
                "price_hkd": round(l["price_twd"] * twd_rate),
                "currency": "TWD",
            }
            for l in r.get("listings", [])
        ]
        return {
            "listing_count": r["listing_count"],
            "by_grade": by_grade_hkd,
            "overall": {
                **ov,
                "min_hkd": round(ov["min_twd"] * twd_rate),
                "max_hkd": round(ov["max_twd"] * twd_rate),
                "avg_hkd": round(ov["avg_twd"] * twd_rate),
            },
            "listings": listings_hkd,
            "currency": "TWD",
            "trust": "listing_identity_checked",
        }

    sources = {
        "yuyu_tei":      _yuyu_out(yuyu),
        "snkr_dunk":     _jpy_listings_out(snkr),
        "card_rush":     _rush_out(rush),
        "magi":          _jpy_listings_out(magi),
        "yahoo_auctions": _jpy_listings_out(yahoo),
        "mercari_jp":    _jpy_listings_out(merc_jp),
        "mercari_tw":    _twd_listings_out(merc_tw),
        "mercari":       _twd_listings_out(merc_tw),
        "pricecharting": None,  # TODO: 正式版加入
    }

    price_points = _collect_price_points(sources, req, identity)
    summary = _build_summary(price_points, req)

    card_label = " ".join(filter(None, [query_name, rarity, card_number]))
    now_hkt    = datetime.now(HKT)

    return {
        "card_name":      card_label,
        "identity":       identity,
        "timestamp":      now_hkt.isoformat(),
        "sources":        sources,
        "price_points":   price_points,
        "summary":        summary,
        "exchange_rates": rates,
        "errors":         errors or None,
        "tg_message":     _fmt_tg(card_label, now_hkt, sources, price_points, summary, req, errors),
    }


def _fmt_tg(card_label, ts, sources, price_points, summary, req, errors) -> str:
    import unicodedata as _ud

    def _dw(s: str) -> int:
        return sum(2 if _ud.east_asian_width(c) in ('W', 'F') else 1 for c in s)

    def _rpad(s: str, w: int) -> str:
        return s + ' ' * max(0, w - _dw(s))

    requirements = _quote_requirements(req)
    intent_label = {"buy": "客人想買", "sell": "客人想賣"}.get(requirements["intent"], "買賣方向未確認")
    lines = [
        "🔎 <b>市場價格快照（老細覆核）</b>",
        f"卡：<b>{card_label}</b>",
        f"需求：{intent_label}｜{requirements['label']}",
        f"更新：{ts.strftime('%m-%d %H:%M')} HKT",
    ]

    source_labels = {
        "yuyu_tei": "遊々亭",
        "snkr_dunk": "SNKR",
        "card_rush": "Card Rush",
        "magi": "magi",
        "yahoo_auctions": "Yahoo Auc",
        "mercari_jp": "Mercari JP",
        "mercari_tw": "Mercari TW",
    }
    grouped: dict[tuple[str, str], list[int]] = {}
    for point in price_points:
        if not point.get("quote_eligible") or not point.get("price_hkd"):
            continue
        grade = point.get("grade", "raw")
        grouped.setdefault((point["source"], grade), []).append(point["price_hkd"])

    rows: list[tuple[str, int, int, int]] = []
    for (source, grade), values in sorted(grouped.items()):
        grade_label = "RAW" if grade == "raw" else grade
        rows.append((f"{source_labels.get(source, source)} {grade_label}", min(values), max(values), _median_int(values)))

    if rows:
        C = 12
        hdr = _rpad("來源", C) + _rpad("低", 8) + _rpad("高", 8) + "中位(HKD)"
        sep = "─" * (C + 8 + 8 + 7)
        tbl = [hdr, sep] + [
            _rpad(n, C) + _rpad(str(lo), 8) + _rpad(str(hi), 8) + str(avg)
            for n, lo, hi, avg in rows
        ]
        lines += ["", "✅ 已核對、符合條件", "<pre>" + "\n".join(tbl) + "</pre>"]
    else:
        lines += ["", "⚠️ 暫時冇已核對、符合條件嘅市場樣本"]

    raw_reference: dict[str, list[int]] = {}
    if requirements["grading_type"] == "raw":
        for point in price_points:
            if (
                not point.get("quote_eligible")
                and point.get("grade") == "raw"
                and point.get("match_score", 0) >= 4
                and point.get("price_hkd")
                and point.get("source") in {"yuyu_tei", "magi"}
            ):
                raw_reference.setdefault(point["source"], []).append(point["price_hkd"])
    if raw_reference:
        ref_lines = [
            f"{source_labels.get(source, source)} RAW：HK${min(values):,}-HK${max(values):,}"
            for source, values in sorted(raw_reference.items())
        ]
        lines += ["", "ℹ️ 卡況未標示（唔納入 A/B/C/D 基準）", *ref_lines]

    possible_reference: dict[tuple[str, str], list[int]] = {}
    for point in price_points:
        if (
            not point.get("quote_eligible")
            and point.get("match_score", 0) >= 6
            and point.get("grade") in requirements["eligible_grades"]
            and point.get("price_hkd")
        ):
            key = (point["source"], point.get("grade", "raw"))
            possible_reference.setdefault(key, []).append(point["price_hkd"])
    if possible_reference:
        possible_lines = [
            f"{source_labels.get(source, source)} {grade}：HK${min(values):,}-HK${max(values):,}"
            for (source, grade), values in sorted(possible_reference.items())
        ]
        lines += ["", "ℹ️ 卡名/卡號中，但系列未確認或不符（不入基準）", *possible_lines]

    rejected_count = sum(1 for p in price_points if not p.get("quote_eligible"))
    if rejected_count:
        lines.append(f"🧹 已隔離 {rejected_count} 個不符條件／未能核對樣本")

    if summary and summary.get("recommended_buy") is not None:
        icon = {"high": "🟢", "medium": "🟡", "low": "🟠"}.get(summary.get("confidence"), "⚪")
        lines += [
            "",
            f"📊 市場基準：<b>HK${summary.get('market_base_hkd', 0):,}</b>（{summary.get('basis', 'n/a')}）",
            f"試算買入：HK${summary['recommended_buy']:,}｜試算賣出：HK${summary['recommended_sell']:,}",
            f"來源：{summary.get('source_count', 0)} 個網站｜信心度：{icon} {summary['confidence'].upper()}",
            "⚠️ 試算只供老細參考，最終由老細手動報價俾客。",
        ]
    elif summary and summary.get("confidence") == "manual":
        lines += ["", f"⚠️ 未搵到 {summary.get('target_grade', requirements['label'])} 嘅已核對樣本，需要人手逐網覆核。"]

    if requirements["warnings"]:
        lines.append("⚠️ 客人資料未完整，今次只可當寬鬆市場參考。")
    if errors:
        failed = "、".join(sorted(errors))
        lines.append(f"❌ 未能取得：{failed}")

    return "\n".join(lines)
