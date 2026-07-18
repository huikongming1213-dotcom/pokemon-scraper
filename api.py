"""
Pokemon Card Price Scraper
全部 scraper 放同一個檔案，共用 _browser global，避免 circular import 問題
"""
from contextlib import asynccontextmanager
import asyncio
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

    for source_key in ("snkr_dunk", "card_rush", "magi", "yahoo_auctions"):
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

    mercari = sources.get("mercari")
    if mercari:
        for grade, grade_data in mercari.get("by_grade", {}).items():
            for status_key in ("sold", "on_sale"):
                stats = grade_data.get(status_key)
                if not stats:
                    continue
                points.append({
                    "source": "mercari",
                    "grade": grade,
                    "status": "sold" if status_key == "sold" else "active",
                    "metric": "avg",
                    "price_hkd": stats["avg_hkd"],
                    "count": stats.get("count", 1),
                    "match_score": 0,
                    "match_reasons": ["mercari_page_text_only"],
                    "quote_eligible": False,
                    "review_reason": "mercari_listing_identity_unverified",
                })

        if requirements["grading_type"] == "raw" and not mercari.get("by_grade"):
            for status_key in ("sold", "on_sale"):
                stats = mercari.get(status_key)
                if stats:
                    points.append({
                        "source": "mercari",
                        "grade": "raw",
                        "status": "sold" if status_key == "sold" else "active",
                        "metric": "avg",
                        "price_hkd": stats["avg_hkd"],
                        "count": stats.get("count", 1),
                        "match_score": 0,
                        "match_reasons": ["mercari_page_text_only"],
                        "quote_eligible": False,
                        "review_reason": "mercari_listing_identity_unverified",
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
    先用 search result page 補 PSA active listing 樣本，避免逐 item detail 增加不穩定性。
    """
    page = await _stealth_context.new_page()
    try:
        keyword_parts = " ".join(filter(None, [card_name, card_number, rarity]))
        q = quote(keyword_parts or card_number, safe="")
        await page.goto(
            f"https://auctions.yahoo.co.jp/search/search?p={q}&auccat=0",
            wait_until="domcontentloaded",
            timeout=35000,
        )
        try:
            await page.wait_for_load_state("networkidle", timeout=12000)
        except Exception:
            await page.wait_for_timeout(2000)

        raw_items = await page.evaluate(r"""() => {
            const seen = new Set();
            const rows = [];
            for (const link of Array.from(document.querySelectorAll('a[href*="/jp/auction/"][data-auction-price]'))) {
                const href = link.href || '';
                if (!href || seen.has(href)) continue;
                seen.add(href);

                const priceText = link.getAttribute('data-auction-price') || '';
                const price = parseInt(String(priceText).replace(/[^0-9]/g, ''), 10) || 0;
                if (price <= 0) continue;

                const title =
                    link.querySelector('img[alt]')?.getAttribute('alt')?.trim() ||
                    link.getAttribute('aria-label')?.trim() ||
                    link.textContent?.trim() ||
                    '';
                if (!title) continue;

                rows.push({
                    url: href,
                    name: title,
                    price_jpy: price,
                    is_flea: Boolean(link.getAttribute('data-auction-isflea')),
                    is_free_shipping: Boolean(link.getAttribute('data-auction-isfreeshipping')),
                });
                if (rows.length >= 40) break;
            }
            return rows;
        }""")

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
    finally:
        await page.close()


# ── Scraper 6: Mercari TW ─────────────────────────────────────────────────
# FIX: search keyword 加入 rarity，避免出無關結果

def _mercari_stats(prices: list[int]) -> dict:
    return {
        "count":   len(prices),
        "min_twd": min(prices),
        "max_twd": max(prices),
        "avg_twd": round(sum(prices) / len(prices)),
    }


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


def _mercari_grade_stats(listings: list[dict], grade: str, sold: bool) -> dict | None:
    """Mercari 用 TWD，計算某 grade + 在售/已售 嘅 min/max/avg"""
    items = [l["price"] for l in listings if l["grade"] == grade and l["is_sold"] == sold]
    if not items:
        return None
    return {
        "count":   len(items),
        "min_twd": min(items),
        "max_twd": max(items),
        "avg_twd": round(sum(items) / len(items)),
    }


async def _scrape_mercari_tw(card_number: str, card_name: str = "", rarity: str = "") -> dict:
    """
    Playwright + stealth context，爬 Mercari TW。
    - search keyword 包含 rarity 過濾無關商品
    - JS 從每個 listing 附近文字提取 PSA grade
    - 返回在售/已售 overall + by_grade 分組（TWD + HKD）
    """
    page = await _stealth_context.new_page()
    try:
        await page.add_init_script(_CR_STEALTH_SCRIPT)
        await page.set_extra_http_headers({"Referer": "https://www.google.com.tw/"})

        keyword_parts = " ".join(filter(None, [card_name, card_number, rarity]))
        q = quote(keyword_parts or card_number)

        await page.goto(
            f"https://tw.mercari.com/zh-hant/search?keyword={q}",
            wait_until="domcontentloaded",
            timeout=35000,
        )
        try:
            await page.wait_for_load_state("networkidle", timeout=20000)
        except Exception:
            await page.wait_for_timeout(5000)

        items_data = await page.evaluate(r"""() => {
            const text = document.body.innerText || '';
            const lines = text.split('\n').map(l => l.trim()).filter(l => l);
            const results = [];
            for (let i = 0; i < lines.length; i++) {
                let price = 0;
                if (lines[i] === 'NT$' && i + 1 < lines.length) {
                    const num = lines[i + 1].replace(/,/g, '');
                    if (/^\d+$/.test(num)) { price = parseInt(num); i++; }
                } else {
                    const m = lines[i].match(/^NT\$\s*([\d,]+)$/);
                    if (m) price = parseInt(m[1].replace(/,/g, ''));
                }
                if (price < 50 || price > 50000000) continue;

                // 往前後各 10 行搵 sold 狀態 + PSA grade
                const ctx = lines.slice(Math.max(0, i - 10), i + 3).join(' ');
                const isSold = ctx.includes('已售出') || ctx.includes('SOLD') || ctx.includes('Sold out');

                // 提取 PSA grade：優先抓 PSA + 數字，否則 raw
                const psaMatch = ctx.match(/PSA\s*(\d+)/i);
                const grade = psaMatch ? ('PSA' + psaMatch[1]) : 'raw';

                results.push({ price, is_sold: isSold, grade });
            }
            // 推薦排序頭 20 個 listing 已足夠，避免低質/無關結果污染統計
            return results.slice(0, 20);
        }""")

        if not items_data:
            return {"status": "no_results"}

        twd_rate = await _get_twd_hkd_rate()

        def _with_hkd(stats: dict) -> dict:
            return {
                **stats,
                "min_hkd": round(stats["min_twd"] * twd_rate),
                "max_hkd": round(stats["max_twd"] * twd_rate),
                "avg_hkd": round(stats["avg_twd"] * twd_rate),
            }

        on_sale_prices = [d["price"] for d in items_data if not d["is_sold"]]
        sold_prices    = [d["price"] for d in items_data if     d["is_sold"]]

        if not on_sale_prices and not sold_prices:
            return {"status": "no_results"}

        # ── by_grade 分組：每個 grade 分別統計在售/已售 ──
        all_grades = sorted({d["grade"] for d in items_data})
        by_grade = {}
        for g in all_grades:
            on_s = _mercari_grade_stats(items_data, g, sold=False)
            so   = _mercari_grade_stats(items_data, g, sold=True)
            if on_s or so:
                by_grade[g] = {
                    "on_sale": _with_hkd(on_s) if on_s else None,
                    "sold":    _with_hkd(so)   if so   else None,
                }

        return {
            "status":       "ok",
            "currency":     "TWD",
            "twd_hkd_rate": twd_rate,
            "on_sale":      _with_hkd(_mercari_stats(on_sale_prices)) if on_sale_prices else None,
            "sold":         _with_hkd(_mercari_stats(sold_prices))    if sold_prices    else None,
            "by_grade":     by_grade,
        }

    except Exception as e:
        raise RuntimeError(f"mercari_tw failed: {type(e).__name__}: {e}")
    finally:
        await page.close()


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
):
    """Mercari TW 獨立查詢，返回在售/已售 min/max/avg（TWD + HKD）。"""
    try:
        result = await _scrape_mercari_tw(cardNumber, cardName, rarity)
        status = result.get("status") if result else None
        if status == "no_results":
            return {"card_number": cardNumber, "message": "no results found"}
        if status == "blocked":
            return {"card_number": cardNumber, "message": "scraper blocked", "reason": result.get("reason")}
        if not result:
            return {"card_number": cardNumber, "message": "no results found"}
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
            errors[key] = str(e)
            return {}

    identity = _normalize_card_identity(req)
    rates = await _get_rates()

    # FIX: 全部 scraper 都傳入 rarity；日站優先用 normalized 日文名查詢
    query_name = identity["query_name"]
    card_number = identity["card_number"]
    rarity = identity["rarity"]
    yuyu, snkr, rush, magi, yahoo, merc = await asyncio.gather(
        _safe(_scrape_yuyu_tei(card_number, query_name, rarity),   "yuyu_tei",  timeout=35),
        _safe(_scrape_snkr_dunk(card_number, query_name, rarity),  "snkr_dunk", timeout=20),
        _safe(_scrape_card_rush(card_number, query_name),          "card_rush", timeout=120),
        _safe(_scrape_magi(card_number, query_name, rarity),       "magi",      timeout=40),
        _safe(_scrape_yahoo_auctions(card_number, query_name, rarity), "yahoo_auctions", timeout=40),
        _safe(_scrape_mercari_tw(card_number, query_name, rarity), "mercari_tw", timeout=60),
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

    def _merc_out(r):
        if not r.get("on_sale") and not r.get("sold"): return None
        return {
            **r,
            "trust": "reference_only",
            "quote_eligible": False,
            "warning": "listing title 未逐項核對，可能混入非目標卡或非卡商品",
        }

    sources = {
        "yuyu_tei":      _yuyu_out(yuyu),
        "snkr_dunk":     _jpy_listings_out(snkr),
        "card_rush":     _rush_out(rush),
        "magi":          _jpy_listings_out(magi),
        "yahoo_auctions": _jpy_listings_out(yahoo),
        "mercari":       _merc_out(merc),
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
            and point.get("source") != "mercari"
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

    mercari = sources.get("mercari")
    if mercari:
        ref_rows = []
        for grade in requirements["eligible_grades"]:
            grade_data = mercari.get("by_grade", {}).get(grade, {})
            for status, label in (("sold", "已售"), ("on_sale", "在售")):
                stats = grade_data.get(status)
                if stats:
                    ref_rows.append((f"Mercari {label} {grade}", stats["min_hkd"], stats["max_hkd"], stats["avg_hkd"]))
        if requirements["grading_type"] == "raw" and not ref_rows:
            for status, label in (("sold", "已售"), ("on_sale", "在售")):
                stats = mercari.get(status)
                if stats:
                    ref_rows.append((f"Mercari {label}", stats["min_hkd"], stats["max_hkd"], stats["avg_hkd"]))
        if ref_rows:
            C2 = 18
            hdr2 = _rpad("來源", C2) + _rpad("低", 8) + _rpad("高", 8) + "均(HKD)"
            tbl2 = [hdr2, "─" * (C2 + 8 + 8 + 7)] + [
                _rpad(n, C2) + _rpad(str(lo), 8) + _rpad(str(hi), 8) + str(avg)
                for n, lo, hi, avg in ref_rows
            ]
            lines += ["", "⚠️ 僅供核對（Mercari 未能逐項驗證商品）", "<pre>" + "\n".join(tbl2) + "</pre>"]

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
