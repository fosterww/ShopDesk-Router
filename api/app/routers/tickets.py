from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from api.app.db import get_db
from common.db.dao import MessageRepository
from common.db.schemas import tickets
from common.clients import zendesk


router = APIRouter(prefix="/tickets", tags=["tickets"])


class ApproveReplyPayload(BaseModel):
    reply: str


@router.post("/{ticket_id}/approve-reply")
async def approve_reply(ticket_id: str, payload: ApproveReplyPayload, db: AsyncSession = Depends(get_db)):
    stmt = select(
        tickets.c.id, 
        tickets.c.external_id, 
        tickets.c.message_id
    ).where(tickets.c.id == ticket_id)

    result = await db.execute(stmt)
    row = result.first()
    if not row:
        raise HTTPException(status_code=404, detail="Ticket not found")
    
    external_id = row.external_id
    message_id = row.message_id
    if not external_id:
        raise HTTPException(status_code=400, detail="Ticket has no external_id")
    
    ok = await zendesk.add_public_comment(external_id, payload.reply)
    if not ok:
        raise HTTPException(status_code=502, detail="Failed to post to Zendesk")
    
    repo = MessageRepository(db)
    await repo.insert_event(
        ticket_id=str(ticket_id),
        message_id=str(message_id) if message_id else None,
        type_="REPLY_APPROVED",
        payload={
            "ticket_id": str(ticket_id),
            "message_id": str(message_id) if message_id else None,
            "reply": payload.reply,
        },
    )
    await db.commit()
    return {"status": "ok"}
