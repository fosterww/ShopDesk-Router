import os
import time
import httpx
from typing import Any, Dict, Optional, Tuple

_CACHE_TTL_SECONDS = 600
_cache: dict[str, Tuple[float, Optional[Dict[str, Any]]]] = {}


def _now() -> float:
    return time.time()


async def _fetch_order(order_id: str) -> Optional[Dict[str, Any]]:
    sandbox_env = os.getenv("SHOPIFY_SANDBOX", "1").lower()
    if sandbox_env in ("1", "true", "yes", "y"):
        return {
            "order_id": order_id,
            "line_items": [{"title": "Sandbox Widget", "quantity": 1}],
            "ship_status": "in_transit",
            "payment_status": "paid",
            "source": "sandbox",
        }

    api_key = os.getenv("SHOPIFY_API_KEY")
    password = os.getenv("SHOPIFY_PASSWORD")
    domain = os.getenv("SHOPIFY_DOMAIN")

    if not (api_key and password and domain):
        return None

    clean_id = order_id.replace("#", "").strip()
    url = f"https://{api_key}:{password}@{domain}/admin/api/2023-10/orders.json"

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, params={"name": clean_id, "status": "any"})
            resp.raise_for_status()

            data = resp.json()
            orders = data.get("orders", [])
            if not orders:
                return None

            order = orders[0]
            return {
                "id": order.get("id"),
                "order_number": order.get("order_number"),
                "email": order.get("email"),
                "total_price": order.get("total_price"),
                "currency": order.get("currency"),
                "financial_status": order.get("financial_status"),
                "fulfillment_status": order.get("fulfillment_status") or "unfulfilled",
                "line_items": [
                    {
                        "title": item.get("title"),
                        "quantity": item.get("quantity"),
                        "sku": item.get("sku"),
                    }
                    for item in order.get("line_items", [])
                ],
                "tracking_urls": [
                    f.get("tracking_url")
                    for f in order.get("fulfillments", [])
                    if f.get("tracking_url")
                ],
            }
    except Exception:
        return None


async def get_order(order_id: str) -> Optional[Dict[str, Any]]:
    now = _now()
    cached = _cache.get(order_id)
    if cached and now - cached[0] < _CACHE_TTL_SECONDS:
        return cached[1]

    res = await _fetch_order(order_id)
    _cache[order_id] = (now, res)
    return res
