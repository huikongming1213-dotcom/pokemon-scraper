"""
Pokemon Card Price Scraper
全部 scraper 放同一個檔案，共用 _browser global，避免 circular import 問題
"""
from contextlib import asynccontextmanager
import asyncio
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pw, _browser
    _pw = await async_playwright().start()
    _browser = await _pw.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--disable-dev-shm-usage"],
    )
    yield
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


# ── Scraper 2: SNKR Dunk ──────────────────────────────────────────────────

async def _scrape_snkr_dunk(card_name: str, card_number: str) -> dict:
    page = await _browser.new_page()
    try:
        q = quote(f"{card_name} {card_number}".strip())
        await page.goto(f"https://snkrdunk.com/en/pokemon-cards?q={q}")
        await page.wait_for_load_state("networkidle", timeout=25000)

        # 試 specific selectors
        selectors = [
            "[class*='CardPrice']",
            "[class*='card-price']",
            "[class*='product-price']",
            "[class*='item-price']",
            ".price",
        ]
        for sel in selectors:
            el = await page.query_selector(sel)
            if el:
                digits = re.sub(r"[^\d]", "", (await el.inner_text()).strip())
                if digits and int(digits) > 200:
                    return {"price_jpy": int(digits)}

        # Fallback: extract from page text
        prices = await _page_prices(page)
        if prices:
            return {"price_jpy": prices[0]}

        return {}
    except Exception as e:
        raise RuntimeError(f"snkr_dunk failed: {type(e).__name__}: {e}")
    finally:
        await page.close()


# ── Scraper 3: Card Rush ──────────────────────────────────────────────────

async def _scrape_card_rush(card_name: str, card_number: str) -> dict:
    page = await _browser.new_page()
    try:
        q = quote(f"{card_name} {card_number}".strip())
        await page.goto(f"https://www.cardrush-pokemon.jp/product-list?search_word={q}")
        await page.wait_for_load_state("networkidle", timeout=25000)

        selectors = [
            ".buy-price",
            "[class*='buy']",
            "[class*='price']",
            ".product-price",
        ]
        for sel in selectors:
            el = await page.query_selector(sel)
            if el:
                digits = re.sub(r"[^\d]", "", (await el.inner_text()).strip())
                if digits and int(digits) > 200:
                    return {"buy_price_jpy": int(digits)}

        prices = await _page_prices(page)
        if prices:
            return {"buy_price_jpy": prices[0]}

        return {}
    except Exception as e:
        raise RuntimeError(f"card_rush failed: {type(e).__name__}: {e}")
    finally:
        await page.close()


# ── Scraper 4: Mercari Japan ──────────────────────────────────────────────

async def _scrape_mercari(card_name: str, card_number: str) -> dict:
    page = await _browser.new_page()
    try:
        q = quote(f"{card_name} {card_number}".strip())
        await page.goto(f"https://jp.mercari.com/search?keyword={q}&status=on_sale")
        await page.wait_for_load_state("networkidle", timeout=30000)

        # Mercari item price selectors
        price_els = await page.query_selector_all(
            "[data-testid='price'], [class*='merPrice'], [class*='item-price'], [class*='ItemPrice']"
        )
        prices = []
        for el in price_els[:15]:
            digits = re.sub(r"[^\d]", "", (await el.inner_text()).strip())
            if digits and 200 <= int(digits) <= 5_000_000:
                prices.append(int(digits))

        if not prices:
            prices = await _page_prices(page)

        if prices:
            return {
                "avg_price_jpy":     round(sum(prices) / len(prices)),
                "min_price_jpy":     min(prices),
                "listing_count":     len(prices),
            }
        return {}
    except Exception as e:
        raise RuntimeError(f"mercari failed: {type(e).__name__}: {e}")
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
    snkr  = {}   # TODO: selector 未驗證，暫停
    rush  = {}   # TODO: selector 未驗證，暫停
    merc  = {}   # TODO: selector 未驗證，暫停

    # ── 組裝 sources ──
    def _yuyu_out(r):
        if not r.get("price_jpy"): return None
        return {"price_jpy": r["price_jpy"], "price_hkd": _jpy_to_hkd(r["price_jpy"], rates), "name": r.get("name"), "currency": "JPY"}

    def _snkr_out(r):
        if not r.get("price_jpy"): return None
        return {"price_jpy": r["price_jpy"], "price_hkd": _jpy_to_hkd(r["price_jpy"], rates), "currency": "JPY"}

    def _rush_out(r):
        if not r.get("buy_price_jpy"): return None
        return {"buy_price_jpy": r["buy_price_jpy"], "buy_price_hkd": _jpy_to_hkd(r["buy_price_jpy"], rates), "currency": "JPY"}

    def _merc_out(r):
        if not r.get("avg_price_jpy"): return None
        return {
            "avg_price_jpy":  r["avg_price_jpy"],
            "avg_price_hkd":  _jpy_to_hkd(r["avg_price_jpy"], rates),
            "min_price_jpy":  r.get("min_price_jpy"),
            "min_price_hkd":  _jpy_to_hkd(r["min_price_jpy"], rates) if r.get("min_price_jpy") else None,
            "listing_count":  r.get("listing_count"),
            "currency":       "JPY",
        }

    sources = {
        "yuyu_tei":    _yuyu_out(yuyu),
        "snkr_dunk":   _snkr_out(snkr),
        "card_rush":   _rush_out(rush),
        "mercari":     _merc_out(merc),
        "pricecharting": None,  # TODO: 正式版加入
    }

    # ── Summary ──
    hkd_prices = [
        v for k, v in [
            ("yuyu_tei",  sources["yuyu_tei"]["price_hkd"]     if sources["yuyu_tei"]  else None),
            ("snkr_dunk", sources["snkr_dunk"]["price_hkd"]    if sources["snkr_dunk"] else None),
            ("mercari",   sources["mercari"]["avg_price_hkd"]  if sources["mercari"]   else None),
        ] if v
    ]

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
        jp.append(f"SNKR Dunk：¥{s['price_jpy']:,} (HK${s['price_hkd']:,})")
    if sources["card_rush"]:
        s = sources["card_rush"]
        jp.append(f"Card Rush 收購：¥{s['buy_price_jpy']:,} (HK${s['buy_price_hkd']:,})")
    if sources["mercari"]:
        s = sources["mercari"]
        jp.append(f"Mercari 均價：¥{s['avg_price_jpy']:,} (HK${s['avg_price_hkd']:,}) [{s.get('listing_count','?')}個]")

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
