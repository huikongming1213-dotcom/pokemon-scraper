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
    return grade  # 保留原格式（状態B, raw 等）


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

    name_keywords = [k for k in card_name.split() if len(k) > 1] if card_name else []

    listings = [
        {
            "price_jpy": p["salePrice"],
            # FIX: normalize grade 格式，統一 "PSA 10" → "PSA10" 等
            "grade":     _normalize_grade(p.get("condition") or "raw"),
            "name":      p.get("title", ""),
        }
        for p in products
        if p.get("salePrice", 0) > 0
        and (not name_keywords or any(kw in (p.get("title") or "") for kw in name_keywords))
    ]

    if not listings:
        return {}

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


# ── Scraper 4: Mercari TW ─────────────────────────────────────────────────
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
    is_psa:      bool = False
    psa_grade:   int | None = None


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

    rates = await _get_rates()

    # FIX: 全部 scraper 都傳入 rarity
    yuyu  = await _safe(_scrape_yuyu_tei(req.card_number, req.card_name, req.rarity),        "yuyu_tei",  timeout=35)
    snkr  = await _safe(_scrape_snkr_dunk(req.card_number, req.card_name, req.rarity),       "snkr_dunk", timeout=20)
    rush  = await _safe(_scrape_card_rush(req.card_number, req.card_name),                   "card_rush", timeout=120)
    merc  = await _safe(_scrape_mercari_tw(req.card_number, req.card_name, req.rarity),      "mercari_tw", timeout=60)

    # ── 組裝 sources ──
    def _yuyu_out(r):
        if not r.get("price_jpy"): return None
        return {"price_jpy": r["price_jpy"], "price_hkd": _jpy_to_hkd(r["price_jpy"], rates), "name": r.get("name"), "currency": "JPY"}

    def _snkr_out(r):
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
        return r

    sources = {
        "yuyu_tei":      _yuyu_out(yuyu),
        "snkr_dunk":     _snkr_out(snkr),
        "card_rush":     _rush_out(rush),
        "mercari":       _merc_out(merc),
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
    import unicodedata as _ud

    def _dw(s: str) -> int:
        return sum(2 if _ud.east_asian_width(c) in ('W', 'F') else 1 for c in s)

    def _rpad(s: str, w: int) -> str:
        return s + ' ' * max(0, w - _dw(s))

    lines = [
        "🔔 <b>報價審批</b>",
        f"卡名：<b>{card_label}</b>",
        f"時間：{ts.strftime('%m-%d %H:%M')} HKT",
    ]

    rows: list[tuple[str, int, int, int]] = []

    if sources.get("yuyu_tei"):
        h = sources["yuyu_tei"]["price_hkd"]
        rows.append(("遊々亭", h, h, h))

    if sources.get("snkr_dunk"):
        ov = sources["snkr_dunk"].get("overall", {})
        if ov:
            rows.append(("SNKR Dunk", ov["min_hkd"], ov["max_hkd"], ov["avg_hkd"]))

    if sources.get("card_rush"):
        ov = sources["card_rush"].get("overall", {})
        if ov:
            rows.append(("Card Rush", ov["min_hkd"], ov["max_hkd"], ov["avg_hkd"]))

    if sources.get("mercari"):
        m = sources["mercari"]
        if m.get("on_sale"):
            ov = m["on_sale"]
            rows.append(("Mercari在售", ov["min_hkd"], ov["max_hkd"], ov["avg_hkd"]))
        if m.get("sold"):
            ov = m["sold"]
            rows.append(("Mercari已售", ov["min_hkd"], ov["max_hkd"], ov["avg_hkd"]))

    if rows:
        C = 12
        hdr = _rpad("來源", C) + _rpad("低", 8) + _rpad("高", 8) + "均(HKD)"
        sep = "─" * (C + 8 + 8 + 7)
        tbl = [hdr, sep] + [
            _rpad(n, C) + _rpad(str(lo), 8) + _rpad(str(hi), 8) + str(avg)
            for n, lo, hi, avg in rows
        ]
        lines += ["", "<pre>" + "\n".join(tbl) + "</pre>"]
    else:
        lines += ["", "⚠️ 暫時無市場數據"]

    psa_rows = []

    # Card Rush + SNKR Dunk：JPY sources，by_grade 直接有 avg_hkd
    for src_name, src_key in [("Card Rush", "card_rush"), ("SNKR Dunk", "snkr_dunk")]:
        src = sources.get(src_key)
        if not src or not src.get("by_grade"):
            continue
        bg = src["by_grade"]
        p8  = bg.get("PSA8",  {}).get("avg_hkd")
        p9  = bg.get("PSA9",  {}).get("avg_hkd")
        p10 = bg.get("PSA10", {}).get("avg_hkd")
        if p8 or p9 or p10:
            psa_rows.append((src_name, p8, p9, p10))

    # Mercari TW：by_grade 每個 grade 有 on_sale / sold，優先用 on_sale avg_hkd
    merc_src = sources.get("mercari")
    if merc_src and merc_src.get("by_grade"):
        def _merc_grade_hkd(grade_data: dict) -> int | None:
            s = grade_data.get("on_sale") or grade_data.get("sold")
            return s.get("avg_hkd") if s else None
        bg = merc_src["by_grade"]
        p8  = _merc_grade_hkd(bg.get("PSA8",  {}))
        p9  = _merc_grade_hkd(bg.get("PSA9",  {}))
        p10 = _merc_grade_hkd(bg.get("PSA10", {}))
        if p8 or p9 or p10:
            psa_rows.append(("Mercari TW", p8, p9, p10))

    if psa_rows:
        C2 = 12
        hdr2 = _rpad("來源", C2) + _rpad("PSA8", 6) + _rpad("PSA9", 6) + "PSA10"
        sep2 = "─" * (C2 + 6 + 6 + 5)
        tbl2 = [hdr2, sep2] + [
            _rpad(n, C2)
            + _rpad(str(p8)  if p8  else "—", 6)
            + _rpad(str(p9)  if p9  else "—", 6)
            + (str(p10) if p10 else "—")
            for n, p8, p9, p10 in psa_rows
        ]
        lines += ["", "🏅 PSA 分級均價（HKD）", "<pre>" + "\n".join(tbl2) + "</pre>"]

    if req.is_psa and req.psa_grade:
        lines.append(f"🏅 PSA {req.psa_grade}")

    if summary:
        lines += [
            "",
            f"💰 建議買入：<b>HK${summary['recommended_buy']:,}</b>　建議賣出：<b>HK${summary['recommended_sell']:,}</b>",
            f"信心度：{'🟢' if summary['confidence'] == 'high' else '🟡'} {summary['confidence'].upper()}",
        ]

    return "\n".join(lines)
