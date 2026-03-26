"""
Pokemon Card Price Scraper
全部 scraper 放同一個檔案，共用 _browser global，避免 circular import 問題
"""
from contextlib import asynccontextmanager
import asyncio
import json
import re
import time
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


# ── DEBUG endpoint (臨時用，睇真實 HTML 結構) ─────────────────────────────

@app.get("/debug/html")
async def debug_html(url: str = Query(...)):
    """fetch 一個 URL，返回 rendered HTML + 所有含價格嘅文字，幫助 debug scraper selector"""
    page = await _browser.new_page()
    try:
        await page.goto(url, wait_until="networkidle", timeout=30000)

        # 返回 body text（唔係完整 HTML，避免太大）
        body_text = await page.evaluate("() => document.body.innerText")
        inner_html = await page.evaluate("() => document.body.innerHTML")

        # 搵含價格嘅行
        price_lines = [
            line.strip() for line in body_text.split("\n")
            if re.search(r"[¥￥円\$]|[\d,]{3,}", line) and line.strip()
        ][:50]

        # 搵所有 class 名（幫助識別 selector）
        all_classes = await page.evaluate("""() => {
            const els = document.querySelectorAll('[class]');
            const classes = new Set();
            els.forEach(el => el.className.toString().split(' ').forEach(c => c && classes.add(c)));
            return [...classes].filter(c => c.length > 2 && c.length < 50).slice(0, 100);
        }""")

        return {
            "url": url,
            "title": await page.title(),
            "price_lines": price_lines,
            "all_classes": all_classes,
            "html_snippet": inner_html[:3000],  # 頭3000字
        }
    except Exception as e:
        return {"error": str(e), "url": url}
    finally:
        await page.close()


# ── Helper: extract JPY prices from raw page text ─────────────────────────

async def _page_prices(page, min_val=200, max_val=5_000_000) -> list[int]:
    """JS evaluation: 搵頁面內所有合理嘅日圓價格"""
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


# ── Scraper 1: 遊々亭 ─────────────────────────────────────────────────────

async def _scrape_yuyu_tei(card_number: str) -> dict:
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
        await page.wait_for_load_state("networkidle", timeout=25000)

        cards = await page.query_selector_all(".card-product")
        for card in cards:
            name_el  = await card.query_selector("h4")
            price_el = await card.query_selector("strong.d-block")
            name      = (await name_el.inner_text()).strip()  if name_el  else None
            price_str = (await price_el.inner_text()).strip() if price_el else None
            if price_str:
                digits = re.sub(r"[^\d]", "", price_str)
                if digits:
                    return {"price_jpy": int(digits), "name": name}

        return {}  # 搵唔到但唔係 error
    except Exception as e:
        raise RuntimeError(f"yuyu_tei failed: {type(e).__name__}: {e}")
    finally:
        await page.close()


# ── Scraper 2: SNKR Dunk (pure httpx, no Playwright) ──────────────────────

_SNKR_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Accept":     "application/json",
    "Referer":    "https://snkrdunk.com/",
}


def _grade_stats(listings: list[dict], grade_key: str) -> dict | None:
    """計算某 grade 的 min/max/avg/count"""
    items = [l for l in listings if l["grade"] == grade_key]
    if not items:
        return None
    prices = [l["price_jpy"] for l in items]
    return {
        "count":       len(items),
        "min_jpy":     min(prices),
        "max_jpy":     max(prices),
        "avg_jpy":     round(sum(prices) / len(prices)),
    }


async def _scrape_snkr_dunk(card_number: str) -> dict:
    """
    純 httpx，唔使 Playwright。
    返回所有 listing，並按 PSA8/PSA9/PSA10/raw 分組統計 min/max/avg。
    """
    q = quote(card_number)
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
            "grade":     p.get("condition") or "raw",
            "name":      p.get("title", ""),
        }
        for p in products
        if p.get("salePrice", 0) > 0
    ]

    if not listings:
        return {}

    # 分組統計
    by_grade = {}
    for grade_key in ("PSA10", "PSA9", "PSA8", "PSA8以下", "raw"):
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


# ── Scraper 3: Card Rush ──────────────────────────────────────────────────

_CR_STEALTH_SCRIPT = "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"


def _parse_cr_grade(name: str) -> str:
    """〔PSA10鑑定済〕→ PSA10、〔状態B〕→ 状態B、grade 唔明 → raw"""
    m = re.search(r"〔([^〕]+)〕", name)
    if not m:
        return "raw"
    tag = m.group(1)
    g = re.search(r"(PSA\d+|BGS[\d.]+|ARS\d+|PCG\d+|CGC\d+)", tag)
    if g:
        return g.group(1)
    if "状態" in tag:
        return "状態" + tag.replace("状態", "").replace("※", "").strip()
    return tag.replace("鑑定済", "").replace("※状態難/", "").strip() or "raw"


async def _scrape_card_rush(card_number: str) -> dict:
    """
    Playwright + Cloudflare stealth。
    返回所有 listing，按 grade 分組 min/max/avg，並標記在庫狀態。
    """
    page = await _stealth_context.new_page()
    try:
        await page.add_init_script(_CR_STEALTH_SCRIPT)
        q = quote(card_number)
        await page.goto(
            f"https://www.cardrush-pokemon.jp/product-list?keyword={q}&Submit=%E6%A4%9C%E7%B4%A2",
            wait_until="domcontentloaded",
            timeout=35000,
        )
        await page.wait_for_timeout(5000)

        raw = await page.evaluate("""() => {
            const results = [];
            document.querySelectorAll(".selling_price").forEach(priceEl => {
                const li      = priceEl.closest("li") || priceEl.parentElement;
                const nameEl  = li.querySelector(".goods_name");
                const stockEl = li.querySelector(".stock");
                const figEl   = priceEl.querySelector(".figure");
                results.push({
                    name:  nameEl ? nameEl.innerText.trim() : "",
                    price: figEl  ? figEl.innerText.trim()  : "",
                    stock: stockEl ? stockEl.innerText.trim() : "",
                });
            });
            return results;
        }""")

        listings = []
        for item in raw:
            digits = re.sub(r"[^\d]", "", item["price"])
            if not digits:
                continue
            listings.append({
                "price_jpy": int(digits),
                "grade":     _parse_cr_grade(item["name"]),
                "in_stock":  item["stock"] != "×" and "在庫" in item["stock"],
                "name":      item["name"],
            })

        if not listings:
            return {}

        # 分組統計
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

    except Exception as e:
        raise RuntimeError(f"card_rush failed: {type(e).__name__}: {e}")
    finally:
        await page.close()


# ── Scraper 4: Mercari TW ────────────────────────────────────────────────

def _mercari_stats(prices: list[int]) -> dict:
    return {
        "count":   len(prices),
        "min_twd": min(prices),
        "max_twd": max(prices),
        "avg_twd": round(sum(prices) / len(prices)),
    }


async def _get_twd_hkd_rate() -> float:
    """TWD → HKD 匯率（1 TWD = ? HKD），1hr cache"""
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
        return 0.24  # fallback


async def _scrape_mercari_tw(card_number: str) -> dict:
    """
    Playwright + stealth context，爬 Mercari TW。
    從 script tag 提取 initialItems JSON。
    返回在售 / 已售各自 min/max/avg (NTD + HKD)。
    """
    page = await _stealth_context.new_page()
    try:
        await page.add_init_script(_CR_STEALTH_SCRIPT)
        q = quote(card_number)
        await page.goto(
            f"https://tw.mercari.com/zh-hant/search?keyword={q}",
            wait_until="domcontentloaded",
            timeout=35000,
        )
        await page.wait_for_timeout(4000)

        # 提取 initialItems — Python 端 parse，避免 JS regex escape 問題
        html = await page.evaluate("() => document.body.innerHTML")
        scripts = re.findall(r"<script[^>]*>(.*?)</script>", html, re.DOTALL)
        target = next((s for s in scripts if "initialItems" in s), None)
        if not target:
            return {}

        unescaped = target.replace('\\\\"', '"')
        m = re.search(r'"initialItems":\[(.+?)\],"', unescaped)
        if not m:
            return {}

        raw = json.loads("[" + m.group(1) + "]")

        on_sale, sold = [], []
        for item in raw:
            price_str = item.get("price", {}).get("formattedAmount", "")
            digits = re.sub(r"[^\d]", "", price_str)
            if not digits:
                continue
            price = int(digits)
            if item.get("availability") == 1:
                on_sale.append(price)
            else:
                sold.append(price)

        if not on_sale and not sold:
            return {}

        twd_rate = await _get_twd_hkd_rate()

        def _with_hkd(stats: dict) -> dict:
            return {
                **stats,
                "min_hkd": round(stats["min_twd"] * twd_rate),
                "max_hkd": round(stats["max_twd"] * twd_rate),
                "avg_hkd": round(stats["avg_twd"] * twd_rate),
            }

        return {
            "currency":    "TWD",
            "twd_hkd_rate": twd_rate,
            "on_sale":     _with_hkd(_mercari_stats(on_sale))  if on_sale else None,
            "sold":        _with_hkd(_mercari_stats(sold))     if sold    else None,
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


# ── GET /snkr-dunk ────────────────────────────────────────────────────────

@app.get("/mercari")
async def mercari_search(
    cardNumber: str = Query(..., example="288/SM-P"),
):
    """Mercari TW 獨立查詢，返回在售/已售 min/max/avg（TWD + HKD）。"""
    try:
        result = await _scrape_mercari_tw(cardNumber)
        if not result:
            return {"card_number": cardNumber, "message": "no results found"}
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/card-rush")
async def card_rush_search(
    cardNumber: str = Query(..., example="288/SM-P"),
):
    """Card Rush 獨立查詢，返回各 grade min/max/avg（含 HKD 換算）及在庫狀態。"""
    try:
        rates, result = await asyncio.gather(_get_rates(), _scrape_card_rush(cardNumber))
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


@app.get("/snkr-dunk")
async def snkr_dunk_search(
    cardNumber: str = Query(..., example="288/SM-P"),
):
    """SNKR Dunk 獨立查詢，返回 PSA8/PSA9/PSA10/raw 分組 min/max/avg（含 HKD 換算）。"""
    try:
        rates, result = await asyncio.gather(_get_rates(), _scrape_snkr_dunk(cardNumber))
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


# ── POST /price-report ────────────────────────────────────────────────────

class PriceReportRequest(BaseModel):
    card_name:   str
    card_number: str
    set_name:    str = ""
    rarity:      str = ""
    is_psa:      bool = False
    psa_grade:   int | None = None


HKT = timezone(timedelta(hours=8))


@app.post("/price-report")
async def price_report(req: PriceReportRequest):
    # 順序執行，避免並行開多個 Playwright page 導致 RAM 爆
    errors = {}

    async def _safe(coro, key):
        try:
            return await asyncio.wait_for(coro, timeout=25) or {}
        except Exception as e:
            errors[key] = str(e)
            return {}

    rates = await _get_rates()
    yuyu  = await _safe(_scrape_yuyu_tei(req.card_number), "yuyu_tei")
    snkr  = await _safe(_scrape_snkr_dunk(req.card_number), "snkr_dunk")
    rush  = await _safe(_scrape_card_rush(req.card_number), "card_rush")
    merc  = await _safe(_scrape_mercari_tw(req.card_number), "mercari_tw")

    # ── 組裝 sources ──
    def _yuyu_out(r):
        if not r.get("price_jpy"): return None
        return {"price_jpy": r["price_jpy"], "price_hkd": _jpy_to_hkd(r["price_jpy"], rates), "name": r.get("name"), "currency": "JPY"}

    def _snkr_out(r):
        if not r.get("overall"): return None
        # 加 HKD 換算到每個 grade
        by_grade_hkd = {}
        for grade, stats in r.get("by_grade", {}).items():
            by_grade_hkd[grade] = {
                **stats,
                "min_hkd": _jpy_to_hkd(stats["min_jpy"], rates),
                "max_hkd": _jpy_to_hkd(stats["max_jpy"], rates),
                "avg_hkd": _jpy_to_hkd(stats["avg_jpy"], rates),
            }
        ov = r["overall"]
        return {
            "listing_count": r["listing_count"],
            "by_grade":      by_grade_hkd,
            "overall": {
                **ov,
                "min_hkd": _jpy_to_hkd(ov["min_jpy"], rates),
                "max_hkd": _jpy_to_hkd(ov["max_jpy"], rates),
                "avg_hkd": _jpy_to_hkd(ov["avg_jpy"], rates),
            },
            "currency": "JPY",
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
        return {
            "listing_count": r["listing_count"],
            "by_grade":      by_grade_hkd,
            "overall": {
                **ov,
                "min_hkd": _jpy_to_hkd(ov["min_jpy"], rates),
                "max_hkd": _jpy_to_hkd(ov["max_jpy"], rates),
                "avg_hkd": _jpy_to_hkd(ov["avg_jpy"], rates),
            },
            "currency": "JPY",
        }

    def _merc_out(r):
        if not r.get("on_sale") and not r.get("sold"): return None
        return r  # already has HKD inside

    sources = {
        "yuyu_tei":    _yuyu_out(yuyu),
        "snkr_dunk":   _snkr_out(snkr),
        "card_rush":   _rush_out(rush),
        "mercari":     _merc_out(merc),
        "pricecharting": None,  # TODO: 正式版加入
    }

    # ── Summary ──
    hkd_prices = []
    if sources["yuyu_tei"]:
        hkd_prices.append(sources["yuyu_tei"]["price_hkd"])
    if sources["snkr_dunk"] and sources["snkr_dunk"].get("overall"):
        hkd_prices.append(sources["snkr_dunk"]["overall"]["avg_hkd"])
    if sources["card_rush"] and sources["card_rush"].get("overall"):
        hkd_prices.append(sources["card_rush"]["overall"]["avg_hkd"])
    if sources["mercari"]:
        m = sources["mercari"]
        if m.get("on_sale"):
            hkd_prices.append(m["on_sale"]["avg_hkd"])
        elif m.get("sold"):
            hkd_prices.append(m["sold"]["avg_hkd"])

    summary = None
    if hkd_prices:
        low  = min(hkd_prices)
        high = max(hkd_prices)
        summary = {
            "hkd_low":          low,
            "hkd_high":         high,
            "recommended_buy":  round(low  * 0.9),
            "recommended_sell": round(high * 1.05),
            "confidence":       "high" if len(hkd_prices) >= 2 else "low",
        }

    card_label = f"{req.card_name} {req.rarity} {req.card_number}".strip()
    now_hkt    = datetime.now(HKT)

    return {
        "card_name":      card_label,
        "timestamp":      now_hkt.isoformat(),
        "sources":        sources,
        "summary":        summary,
        "exchange_rates": rates,
        "errors":         errors or None,
        "tg_message":     _fmt_tg(card_label, now_hkt, sources, summary, req),
    }


def _fmt_tg(card_label, ts, sources, summary, req) -> str:
    lines = [
        "📊 *價格報告*",
        f"卡名：{card_label}",
        f"查詢時間：{ts.strftime('%Y-%m-%d %H:%M')} (HKT)",
        "",
    ]

    jp = []
    if sources["yuyu_tei"]:
        s = sources["yuyu_tei"]
        jp.append(f"遊々亭：¥{s['price_jpy']:,} (HK${s['price_hkd']:,})")
    if sources["snkr_dunk"]:
        s = sources["snkr_dunk"]
        ov = s.get("overall", {})
        jp.append(f"SNKR Dunk：¥{ov.get('min_jpy',0):,}～¥{ov.get('max_jpy',0):,} 平均¥{ov.get('avg_jpy',0):,} (HK${ov.get('min_hkd',0):,}～{ov.get('max_hkd',0):,}) [{s.get('listing_count','?')}個]")
    if sources["card_rush"]:
        s = sources["card_rush"]
        ov = s.get("overall", {})
        jp.append(f"Card Rush：¥{ov.get('min_jpy',0):,}～¥{ov.get('max_jpy',0):,} 平均¥{ov.get('avg_jpy',0):,} (HK${ov.get('min_hkd',0):,}～{ov.get('max_hkd',0):,}) [{s.get('listing_count','?')}個]")
    if sources["mercari"]:
        s = sources["mercari"]
        if s.get("on_sale"):
            ov = s["on_sale"]
            jp.append(f"Mercari TW 在售：NT${ov['min_twd']:,}～NT${ov['max_twd']:,} 平均NT${ov['avg_twd']:,} (HK${ov['min_hkd']:,}～{ov['max_hkd']:,}) [{ov['count']}個]")
        if s.get("sold"):
            ov = s["sold"]
            jp.append(f"Mercari TW 已售：NT${ov['min_twd']:,}～NT${ov['max_twd']:,} 平均NT${ov['avg_twd']:,} (HK${ov['min_hkd']:,}～{ov['max_hkd']:,}) [{ov['count']}個]")

    if jp:
        lines += ["💴 *日本市場*"] + jp + [""]
    else:
        lines += ["⚠️ 暫時搵唔到日本市場價格", ""]

    if req.is_psa and req.psa_grade:
        lines += [f"🏅 *PSA {req.psa_grade}*", ""]

    if summary:
        lines += [
            "💰 *建議*",
            f"建議買入：HK${summary['recommended_buy']:,}",
            f"建議賣出：HK${summary['recommended_sell']:,}",
            f"信心度：{'🟢' if summary['confidence'] == 'high' else '🟡'} {summary['confidence'].upper()}",
            "",
        ]

    lines.append("⚠️ 需要人手確認後再報價")
    return "\n".join(lines)
