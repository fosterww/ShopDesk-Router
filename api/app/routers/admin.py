from typing import Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from api.app.db import get_db
from common.clients import zendesk
from common.clients import admin

router = APIRouter(prefix="/admin", tags=["admin"])
templates = Jinja2Templates(directory="api/app/templates")

@router.get("/", response_class=HTMLResponse)
async def admin_list(
    request: Request, 
    route: Optional[str] = None, 
    status: Optional[str] = None, 
    db: AsyncSession = Depends(get_db)
):
    rows = await admin.fetch_tickets(db, route, status)
    return templates.TemplateResponse(
        "admin_list.html",
        {
            "request": request,
            "tickets": rows,
            "filter_route": route or "",
            "filter_status": status or "",
        },
    )

@router.get("/tickets/{ticket_id}", response_class=HTMLResponse)
async def admin_ticket(
    request: Request, 
    ticket_id: str, 
    db: AsyncSession = Depends(get_db)
):
    row = await admin.get_ticket_details(db, ticket_id)
    if not row:
        raise HTTPException(status_code=404, detail="Ticket not found")

    message_id = row.message_id

    summary_event = await admin.get_latest_event_payload(db, message_id, "SUMMARY_DONE")
    normalized_event = await admin.get_latest_event_payload(db, message_id, "NORMALIZE_DONE")
    docqa_event = await admin.get_latest_event_payload(db, message_id, "DOCQA_DONE")
    body_text = await admin.get_message_body(db, message_id)

    def first_two_sentences(text_in: str) -> str:
        parts = [p.strip() for p in text_in.replace("\n", " ").split(".") if p.strip()]
        return ". ".join(parts[:2]) + ("." if parts[:2] else "")

    customer_says = first_two_sentences(
        body_text or summary_event.get("summary") or row.summary or ""
    )
    
    fields = normalized_event.get("normalized") or docqa_event.get("fields") or {}
    confidence = fields.get("confidence", {})

    return templates.TemplateResponse(
        "admin_ticket.html",
        {
            "request": request,
            "ticket": row,
            "customer_says": customer_says,
            "fields": fields,
            "confidence": confidence,
        },
    )

@router.patch("/tickets/{ticket_id}/fields")
async def edit_fields(
    ticket_id: str, 
    payload: Dict[str, Any], 
    db: AsyncSession = Depends(get_db)
):
    fields_to_update = {
        k: v for k, v in payload.items() 
        if k in {"order_id", "amount", "order_date", "sku"}
    }
    if not fields_to_update:
        raise HTTPException(status_code=400, detail="No editable fields provided")

    row = await admin.get_ticket_details(db, ticket_id)
    if not row:
        raise HTTPException(status_code=404, detail="Ticket not found")

    summary_text = await admin.update_ticket_fields(
        db, ticket_id, row.message_id, fields_to_update
    )
    
    return {"status": "ok", "summary": summary_text}

@router.post("/tickets/{ticket_id}/approve")
async def approve_send(
    ticket_id: str, 
    payload: Dict[str, Any], 
    db: AsyncSession = Depends(get_db)
):
    reply = payload.get("reply") or ""
    
    row = await admin.get_ticket_details(db, ticket_id)
    if not row or not row.external_id:
        raise HTTPException(status_code=404, detail="Ticket not found or not synced")

    ok = await zendesk.add_public_comment(row.external_id, reply)
    if not ok:
        raise HTTPException(status_code=502, detail="Failed to post to Zendesk")

    await admin.log_approval_event(db, ticket_id, row.message_id, reply)
    
    return {"status": "ok"}