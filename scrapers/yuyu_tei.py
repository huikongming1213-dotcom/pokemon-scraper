"""
遊々亭爬蟲 — 搜尋賣出價
reuse 現有 api.py 邏輯，接受共用 browser instance
"""
import re


async def scrape(browser, card_number: str) -> dict | None:
    """
    搜尋遊々亭，返回第一個匹配卡片嘅價格（JPY integer）
    失敗返回 None
    """
    page = await browser.new_page()
    try:
        await page.goto("https://yuyu-tei.jp/sell/poc/s/search")  # default 30s timeout

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
            return None

        await page.wait_for_load_state("networkidle", timeout=25000)

        cards = await page.query_selector_all(".card-product")
        if not cards:
            return None

        results = []
        for card in cards:
            name_el  = await card.query_selector("h4")
            price_el = await card.query_selector("strong.d-block")
            name  = (await name_el.inner_text()).strip()  if name_el  else None
            price_str = (await price_el.inner_text()).strip() if price_el else None

            # 轉成 integer（去掉 ¥ 同逗號）
            price_jpy = None
            if price_str:
                digits = re.sub(r"[^\d]", "", price_str)
                price_jpy = int(digits) if digits else None

            results.append({"name": name, "price_jpy": price_jpy, "price_raw": price_str})

        # 返回第一個有價嘅結果
        for r in results:
            if r["price_jpy"]:
                return {"price_jpy": r["price_jpy"], "name": r["name"]}

        return None

    except Exception as e:
        # 唔 swallow exception，讓 caller 知道係咩錯
        raise RuntimeError(f"yuyu_tei scrape failed: {type(e).__name__}: {e}") from e
    finally:
        await page.close()
