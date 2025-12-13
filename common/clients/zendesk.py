import os
from typing import Optional, Dict, Any

import httpx


def _sandbox_enabled() -> bool:
    return os.getenv("ZENDESK_SANDBOX", "1").lower() in ("1", "true", "yes", "y")


def _base_url() -> Optional[str]:
    subdomain = os.getenv("ZENDESK_SUBDOMAIN")
    if not subdomain:
        return None
    return f"https://{subdomain}.zendesk.com/api/v2"


async def create_ticket(ticket: Dict[str, Any]) -> Optional[str]:
    if _sandbox_enabled():
        return f"zd_stub_{ticket.get('subject','ticket')}"

    base = _base_url()
    email = os.getenv("ZENDESK_EMAIL")
    token = os.getenv("ZENDESK_API_TOKEN")
    if not (base and email and token):
        return None

    auth = (f"{email}/token", token)
    try:
        async with httpx.AsyncClient(auth=auth) as client:
            resp = await client.post(f"{base}/tickets.json", json={"ticket": ticket})
            resp.raise_for_status()
            data = resp.json()
            return str(data.get("ticket", {}).get("id"))
    except Exception:
        return None


async def add_public_comment(ticket_id: str, body: str) -> bool:
    if _sandbox_enabled():
        return True

    base = _base_url()
    email = os.getenv("ZENDESK_EMAIL")
    token = os.getenv("ZENDESK_API_TOKEN")
    if not (base and email and token):
        return False

    auth = (f"{email}/token", token)
    try:
        async with httpx.AsyncClient(auth=auth) as client:
            resp = await client.put(
                f"{base}/tickets/{ticket_id}.json",
                json={"ticket": {"comment": {"body": body, "public": True}}},
            )
            resp.raise_for_status()
            return True
    except Exception:
        return False
