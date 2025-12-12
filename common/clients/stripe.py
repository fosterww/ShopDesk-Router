import os
import time
import stripe
import asyncio
from typing import Any, Dict, Optional, Tuple

_CACHE_TTL_SECONDS = 600
_cache: dict[str, Tuple[float, Optional[Dict[str, Any]]]] = {}


def _now() -> float:
    return time.time()


def _fetch_charge_sync(order_id: Optional[str], email: Optional[str], amount: Optional[float]) -> Optional[Dict[str, Any]]:
    sandbox_env = os.getenv("STRIPE_SANDBOX", "1").lower()
    if sandbox_env in ("1", "true", "yes", "y"):
        return {
            "charge_id": "ch_stub_123",
            "status": "succeeded",
            "payment_status": "succeeded",
            "amount": amount,
            "currency": "USD",
            "source": "sandbox",
        }
    
    api_key = os.getenv("STRIPE_API_KEY")
    if not api_key:
        return None
    
    stripe.api_key = api_key

    query_parts = []

    if email:
        query_parts.append(f"email:'{email}'")

    if amount:
        try:
            amount_cents = int(amount * 100)
            query_parts.append(f"amount={amount_cents}")
        except Exception:
            pass

    if order_id:
        clean_id = order_id.replace("#", "")
        query_parts.append(f"metadata['order_id']:'{clean_id}'")

    if not query_parts:
        return None
    
    query = " AND ".join(query_parts)

    try:
        resp = stripe.Charge.search(query=query, limit=1)
        if resp.data:
            charge = resp.data[0]
            pm_details = getattr(charge, "payment_method_details", None)
            card_brand = getattr(pm_details.card, "brand", None) if pm_details and getattr(pm_details, "card", None) else None
            outcome = getattr(charge, "outcome", None)
            risk_score = getattr(outcome, "risk_score", None) if outcome else None
            return {
                "charge_id": charge.id,
                "amount": charge.amount / 100.0,
                "status": charge.status,
                "payment_status": charge.status,
                "receipt_url": getattr(charge, "receipt_url", None),
                "card_brand": card_brand,
                "risk_score": risk_score,
                "currency": getattr(charge, "currency", None),
            }
    except Exception:
        return None

    return None



async def find_charge(order_id: Optional[str] = None, email: Optional[str] = None, amount: Optional[float] = None) -> Optional[Dict[str, Any]]:
    now = _now()
    cache_key = f"{order_id or ''}:{email or ''}:{amount or ''}"
    cached = _cache.get(cache_key)
    if cached and now - cached[0] < _CACHE_TTL_SECONDS:
        return cached[1]

    res = await asyncio.to_thread(_fetch_charge_sync, order_id, email, amount)
    _cache[cache_key] = (now, res)
    return res
