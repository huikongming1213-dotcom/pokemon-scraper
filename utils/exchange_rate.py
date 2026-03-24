"""
匯率 utility — 帶 1小時 cache，thread-safe
Fallback: JPY→HKD 0.052, USD→HKD 7.8
"""
import asyncio
import time
import httpx

_cache: dict = {}
_lock = asyncio.Lock()

FALLBACK = {"JPY_HKD": 0.052, "USD_HKD": 7.8}
CACHE_SECONDS = 3600  # 1小時


async def get_rates() -> dict:
    """返回 {"JPY_HKD": float, "USD_HKD": float}"""
    async with _lock:
        now = time.time()
        if _cache.get("ts") and now - _cache["ts"] < CACHE_SECONDS:
            return _cache["rates"]

        try:
            async with httpx.AsyncClient(timeout=8) as client:
                # 以 JPY 為 base，拎 HKD 同 USD
                resp = await client.get("https://api.exchangerate-api.com/v4/latest/JPY")
                resp.raise_for_status()
                data = resp.json()
                rates_raw = data.get("rates", {})

                rates = {
                    "JPY_HKD": rates_raw.get("HKD", FALLBACK["JPY_HKD"]),
                    "USD_HKD": rates_raw.get("HKD", FALLBACK["USD_HKD"]) / rates_raw.get("USD", 1),
                }
                _cache["rates"] = rates
                _cache["ts"] = now
                return rates

        except Exception:
            # 匯率 fetch 失敗，用 fallback
            return FALLBACK


def jpy_to_hkd(jpy: int | float, rates: dict) -> int:
    """JPY → HKD，返回 integer"""
    return round(jpy * rates["JPY_HKD"])


def usd_to_hkd(usd: float, rates: dict) -> int:
    """USD → HKD，返回 integer"""
    return round(usd * rates["USD_HKD"])
