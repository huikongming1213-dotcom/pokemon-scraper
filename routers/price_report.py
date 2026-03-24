"""
POST /price-report
並行爬所有 source，組合成 report + TG 消息
"""
import asyncio
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from scrapers import yuyu_tei
from apis import pricecharting
from utils.exchange_rate import get_rates, jpy_to_hkd, usd_to_hkd

router = APIRouter()

HKT = timezone(timedelta(hours=8))


# ── Input schema ──────────────────────────────────────────────────────────────

class PriceReportRequest(BaseModel):
    card_name:   str
    card_number: str
    set_name:    str = ""
    rarity:      str = ""
    is_psa:      bool = False
    psa_grade:   int | None = None


# ── Endpoint ──────────────────────────────────────────────────────────────────

@router.post("/price-report")
async def price_report(req: PriceReportRequest, request: Request):
    browser = request.app.state.browser

    # 並行跑所有 source + 匯率
    yuyu_task   = yuyu_tei.scrape(browser, req.card_number)
    pc_task     = pricecharting.fetch(req.card_name, req.card_number)
    rates_task  = get_rates()

    yuyu_result, pc_result, rates = await asyncio.gather(
        yuyu_task, pc_task, rates_task,
        return_exceptions=True
    )

    # 如果 gather 返回 exception object，記錄但繼續
    yuyu_error = str(yuyu_result) if isinstance(yuyu_result, Exception) else None
    pc_error   = str(pc_result)   if isinstance(pc_result,   Exception) else None

    if isinstance(yuyu_result, Exception): yuyu_result = None
    if isinstance(pc_result,   Exception): pc_result   = None
    if isinstance(rates,       Exception):
        from utils.exchange_rate import FALLBACK
        rates = FALLBACK

    # 加 HKD 到各 source
    yuyu_out = None
    if yuyu_result:
        yuyu_out = {
            "price_jpy": yuyu_result["price_jpy"],
            "price_hkd": jpy_to_hkd(yuyu_result["price_jpy"], rates),
            "currency": "JPY",
        }

    pc_out = None
    if pc_result:
        pc_out = {
            **pc_result,
            "raw_ungraded_hkd": usd_to_hkd(pc_result["raw_ungraded"], rates) if pc_result.get("raw_ungraded") else None,
            "psa_9_hkd":        usd_to_hkd(pc_result["psa_9"], rates)        if pc_result.get("psa_9")        else None,
            "psa_10_hkd":       usd_to_hkd(pc_result["psa_10"], rates)       if pc_result.get("psa_10")       else None,
        }

    # Summary（有幾多 source 就用幾多）
    hkd_prices = []
    if yuyu_out:
        hkd_prices.append(yuyu_out["price_hkd"])
    if pc_out and req.is_psa and req.psa_grade == 10 and pc_out.get("psa_10_hkd"):
        hkd_prices.append(pc_out["psa_10_hkd"])
    elif pc_out and req.is_psa and req.psa_grade == 9 and pc_out.get("psa_9_hkd"):
        hkd_prices.append(pc_out["psa_9_hkd"])
    elif pc_out and pc_out.get("raw_ungraded_hkd"):
        hkd_prices.append(pc_out["raw_ungraded_hkd"])

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

    errors = {}
    if yuyu_error:  errors["yuyu_tei"]      = yuyu_error
    if pc_error:    errors["pricecharting"] = pc_error

    report = {
        "card_name":      card_label,
        "timestamp":      now_hkt.isoformat(),
        "sources": {
            "pricecharting":         pc_out,
            "pokemon_price_tracker": None,   # TODO: 下版本加入
            "snkr_dunk":             None,   # TODO: 下版本加入
            "card_rush":             None,   # TODO: 下版本加入
            "yuyu_tei":              yuyu_out,
        },
        "summary":        summary,
        "exchange_rates": rates,
        "errors":         errors if errors else None,
        "tg_message":     _format_tg(card_label, now_hkt, yuyu_out, pc_out, summary, req),
    }

    return report


# ── TG 消息 formatter ─────────────────────────────────────────────────────────

def _format_tg(
    card_label: str,
    ts: datetime,
    yuyu: dict | None,
    pc: dict | None,
    summary: dict | None,
    req: PriceReportRequest,
) -> str:
    lines = [
        "📊 *價格報告*",
        f"卡名：{card_label}",
        f"查詢時間：{ts.strftime('%Y-%m-%d %H:%M')} (HKT)",
        "",
    ]

    # 日本市場
    jp_lines = []
    if yuyu:
        jp_lines.append(f"遊々亭：¥{yuyu['price_jpy']:,} (HK${yuyu['price_hkd']:,})")
    if jp_lines:
        lines.append("💴 *日本市場*")
        lines.extend(jp_lines)
        lines.append("")

    # 美國市場
    us_lines = []
    if pc:
        if pc.get("raw_ungraded"):
            us_lines.append(f"Raw 未評級：${pc['raw_ungraded']} (HK${pc.get('raw_ungraded_hkd', '—')})")
        if pc.get("psa_9"):
            us_lines.append(f"PSA 9 均價：${pc['psa_9']} (HK${pc.get('psa_9_hkd', '—')})")
        if pc.get("psa_10"):
            us_lines.append(f"PSA 10 均價：${pc['psa_10']} (HK${pc.get('psa_10_hkd', '—')})")
    if us_lines:
        lines.append("💵 *美國市場（PriceCharting）*")
        lines.extend(us_lines)
        lines.append("")

    # PSA 資訊
    if req.is_psa and req.psa_grade:
        lines.append(f"🏅 *PSA {req.psa_grade}*")
        lines.append("")

    # 建議
    if summary:
        lines.append("💰 *建議*")
        lines.append(f"建議買入：HK${summary['recommended_buy']:,}")
        lines.append(f"建議賣出：HK${summary['recommended_sell']:,}")
        conf_emoji = "🟢" if summary["confidence"] == "high" else "🟡"
        lines.append(f"信心度：{conf_emoji} {summary['confidence'].upper()}")
        lines.append("")

    lines.append("⚠️ 需要人手確認後再報價")

    return "\n".join(lines)
