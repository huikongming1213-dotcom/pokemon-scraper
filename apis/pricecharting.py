"""
PriceCharting API
Step 1: search by card name → get product ID
Step 2: fetch product by ID → get prices (unit: cents, ÷100)
"""
import os
import httpx

BASE = "https://www.pricecharting.com/api"


async def fetch(card_name: str, card_number: str = "") -> dict | None:
    """
    返回:
    {
        "raw_ungraded": float,  # loose-price ÷ 100
        "psa_9": float,         # graded-price ÷ 100
        "psa_10": float,        # manual-only-price ÷ 100
        "currency": "USD"
    }
    失敗返回 None
    """
    api_key = os.getenv("PRICECHARTING_API_KEY", "")
    if not api_key:
        return None

    query = f"{card_name} {card_number}".strip()

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            # Step 1: search
            search_resp = await client.get(
                f"{BASE}/products",
                params={"t": api_key, "q": query, "id": "pokemon-cards"}
            )
            search_resp.raise_for_status()
            search_data = search_resp.json()

            products = search_data.get("products", [])
            if not products:
                return None

            # 取第一個結果
            product_id = products[0].get("id")
            if not product_id:
                return None

            # Step 2: fetch prices
            price_resp = await client.get(
                f"{BASE}/product",
                params={"t": api_key, "id": product_id}
            )
            price_resp.raise_for_status()
            p = price_resp.json()

            def cents(key):
                val = p.get(key)
                if val is None:
                    return None
                try:
                    return round(int(val) / 100, 2)
                except (ValueError, TypeError):
                    return None

            raw        = cents("loose-price")
            psa9       = cents("graded-price")
            psa10      = cents("manual-only-price")

            # 全部都係 None 就當 source 失敗
            if raw is None and psa9 is None and psa10 is None:
                return None

            return {
                "raw_ungraded": raw,
                "psa_9": psa9,
                "psa_10": psa10,
                "currency": "USD",
            }

    except Exception:
        return None
