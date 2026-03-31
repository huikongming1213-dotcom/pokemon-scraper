"""
Standalone scraper tester -- 逐個測試，唔需要起 FastAPI server
用法: python test_scrapers.py
"""
import asyncio
import re
import sys
from urllib.parse import quote

# Windows UTF-8 output fix
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from playwright.async_api import async_playwright

CARD_NAME   = "ピカチュウ"
CARD_NUMBER = "288/SM-P"
RARITY      = "promo"


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


def _sep(label):
    print(f"\n{'='*50}")
    print(f"  {label}")
    print('='*50)


async def test_yuyu_tei(browser):
    _sep("1. 遊々亭 (YuyuTei)")
    page = await browser.new_page()
    try:
        await page.goto("https://yuyu-tei.jp/sell/poc/s/search")
        inputs = page.locator('input[name="search_word"]')
        for i in range(await inputs.count()):
            inp = inputs.nth(i)
            if await inp.is_visible():
                await inp.fill(CARD_NUMBER)
                await inp.press("Enter")
                break
        await page.wait_for_load_state("networkidle", timeout=25000)

        title = await page.title()
        print(f"Page title: {title}")

        cards = await page.query_selector_all(".card-product")
        print(f"Cards found: {len(cards)}")

        results = []
        for card in cards[:5]:
            name_el  = await card.query_selector("h4")
            price_el = await card.query_selector("strong.d-block")
            name      = (await name_el.inner_text()).strip()  if name_el  else None
            price_str = (await price_el.inner_text()).strip() if price_el else None
            results.append({"name": name, "price": price_str})

        if results:
            print("✅ OK —", results[:3])
        else:
            prices = await _page_prices(page)
            print("⚠️  EMPTY — no .card-product found")
            print(f"   Fallback prices from text: {prices[:5]}")
            # 印出部分頁面文字幫助 debug
            body = await page.evaluate("() => document.body.innerText")
            print(f"   Page text snippet: {body[:300]!r}")
    except Exception as e:
        print(f"❌ ERROR — {type(e).__name__}: {e}")
    finally:
        await page.close()


async def test_snkr_dunk(browser):
    _sep("2. SNKR Dunk")
    page = await browser.new_page()
    try:
        q = quote(CARD_NUMBER)
        url = f"https://snkrdunk.com/search?keywords={q}"
        print(f"URL: {url}")
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        # 等多 3 秒俾 JS render
        await page.wait_for_timeout(3000)

        title = await page.title()
        print(f"Page title: {title}")

        # 印出完整 body text，睇結構
        body = await page.evaluate("() => document.body.innerText")
        print(f"--- Body text (first 1500 chars) ---")
        print(body[:1500])
        print(f"--- End ---")

        # 搵所有含 price/價格 class 的 elements
        price_els = await page.evaluate("""() => {
            const results = [];
            document.querySelectorAll('[class*="price"], [class*="Price"], [class*="amount"], [class*="Amount"]').forEach(el => {
                const t = el.innerText.trim();
                if (t) results.push({ class: el.className.slice(0,60), text: t.slice(0,80) });
            });
            return results.slice(0, 15);
        }""")
        print(f"\nPrice-class elements ({len(price_els)}):")
        for el in price_els:
            print(f"  {el}")

        # 搵所有 list item / card 的結構
        items = await page.evaluate("""() => {
            const results = [];
            const sels = ['li', '[class*="item"]', '[class*="Item"]', '[class*="card"]', '[class*="Card"]', '[class*="product"]'];
            for (const sel of sels) {
                const els = document.querySelectorAll(sel);
                if (els.length > 0 && els.length < 50) {
                    results.push({ sel, count: els.length, sample: els[0]?.innerText?.trim()?.slice(0,100) });
                }
            }
            return results;
        }""")
        print(f"\nList-item structures:")
        for it in items:
            print(f"  {it}")

    except Exception as e:
        print(f"❌ ERROR — {type(e).__name__}: {e}")
    finally:
        await page.close()


async def test_card_rush(browser):
    _sep("3. Card Rush")
    page = await browser.new_page()
    try:
        q = quote(f"{CARD_NAME} {CARD_NUMBER}")
        url = f"https://www.cardrush-pokemon.jp/product-list?search_word={q}"
        print(f"URL: {url}")
        await page.goto(url, wait_until="networkidle", timeout=30000)

        title = await page.title()
        print(f"Page title: {title}")

        price_texts = await page.evaluate("""() => {
            const results = [];
            document.querySelectorAll('[class*="price"], [class*="Price"], [class*="buy"], [class*="Buy"]').forEach(el => {
                const t = el.innerText.trim();
                if (t) results.push({ class: el.className, text: t.slice(0,80) });
            });
            return results.slice(0, 10);
        }""")
        print(f"Price/Buy elements: {price_texts}")

        prices = await _page_prices(page)
        if prices:
            print(f"✅ Fallback prices from text: {prices[:5]}")
        else:
            print("⚠️  EMPTY — 搵唔到任何價格")
            body = await page.evaluate("() => document.body.innerText")
            print(f"   Page text snippet: {body[:400]!r}")
    except Exception as e:
        print(f"❌ ERROR — {type(e).__name__}: {e}")
    finally:
        await page.close()


async def test_mercari(browser):
    _sep("4. Mercari Japan")
    page = await browser.new_page()
    try:
        q = quote(f"{CARD_NAME} {CARD_NUMBER}")
        url = f"https://jp.mercari.com/search?keyword={q}&status=on_sale"
        print(f"URL: {url}")
        await page.goto(url, wait_until="networkidle", timeout=35000)

        title = await page.title()
        print(f"Page title: {title}")

        price_texts = await page.evaluate("""() => {
            const results = [];
            const sels = ['[data-testid="price"]', '[class*="merPrice"]', '[class*="item-price"]', '[class*="ItemPrice"]', '[class*="Price"]'];
            sels.forEach(sel => {
                document.querySelectorAll(sel).forEach(el => {
                    const t = el.innerText.trim();
                    if (t) results.push({ sel, text: t.slice(0, 60) });
                });
            });
            return results.slice(0, 15);
        }""")
        print(f"Price elements ({len(price_texts)} found): {price_texts[:5]}")

        prices = await _page_prices(page)
        if prices:
            print(f"✅ Fallback prices from text ({len(prices)} found): {prices[:8]}")
        else:
            print("⚠️  EMPTY — 搵唔到任何價格")
            body = await page.evaluate("() => document.body.innerText")
            print(f"   Page text snippet: {body[:500]!r}")
    except Exception as e:
        print(f"❌ ERROR — {type(e).__name__}: {e}")
    finally:
        await page.close()


async def main():
    print(f"🃏 Testing scrapers for: {CARD_NAME} | {CARD_NUMBER} | {RARITY}")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        try:
            await test_yuyu_tei(browser)
            await test_snkr_dunk(browser)
            await test_card_rush(browser)
            await test_mercari(browser)
        finally:
            await browser.close()

    print(f"\n{'='*50}")
    print("  Done")
    print('='*50)


if __name__ == "__main__":
    asyncio.run(main())
