from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from playwright.async_api import async_playwright, Playwright

from routers.price_report import router as price_report_router


# ── Browser lifecycle (shared across requests) ────────────────────────────────

_pw: Playwright = None
_browser = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pw, _browser
    _pw = await async_playwright().start()
    _browser = await _pw.chromium.launch(headless=True)
    app.state.browser = _browser   # 讓 routers 可以 access
    yield
    await _browser.close()
    await _pw.stop()


app = FastAPI(title="Pokemon Card Price Scraper", lifespan=lifespan)
app.include_router(price_report_router)


# ── 現有 endpoint（保留，唔改）────────────────────────────────────────────────

async def scrape_cards(card_number: str) -> list[dict]:
    page = await _browser.new_page()
    try:
        await page.goto("https://yuyu-tei.jp/sell/poc/s/search")

        inputs = page.locator('input[name="search_word"]')
        count = await inputs.count()
        filled = False
        for i in range(count):
            inp = inputs.nth(i)
            if await inp.is_visible():
                await inp.fill(card_number)
                await inp.press("Enter")
                filled = True
                break

        if not filled:
            raise RuntimeError("No visible search input found")

        await page.wait_for_load_state("networkidle", timeout=15000)

        cards = await page.query_selector_all(".card-product")
        results = []
        for card in cards:
            name_el  = await card.query_selector("h4")
            price_el = await card.query_selector("strong.d-block")
            name  = (await name_el.inner_text()).strip()  if name_el  else None
            price = (await price_el.inner_text()).strip() if price_el else None
            results.append({"name": name, "price": price})

        return results
    finally:
        await page.close()


@app.get("/search")
async def search(
    cardNumber: str = Query(..., example="234/193"),
    rarity: str    = Query(..., example="SAR"),
):
    try:
        cards = await scrape_cards(cardNumber)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return cards
