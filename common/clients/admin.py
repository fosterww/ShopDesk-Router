from typing import Optional, List, Dict, Any
from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession
from common.db.schemas import tickets, events, messages
from common.db.dao import MessageRepository 

async def fetch_tickets(
    db: AsyncSession, 
    route: Optional[str] = None, 
    status: Optional[str] = None, 
    limit: int = 200
) -> List[Dict[str, Any]]:
    stmt = select(
        tickets.c.id, 
        tickets.c.message_id, 
        tickets.c.external_id, 
        tickets.c.status, 
        tickets.c.route, 
        tickets.c.summary, 
        tickets.c.updated_at
    )

    if route:
        stmt = stmt.where(tickets.c.route == route)
    if status:
        stmt = stmt.where(tickets.c.status == status)

    stmt = stmt.order_by(tickets.c.updated_at.desc().nulls_last()).limit(limit)
    
    result = await db.execute(stmt)
    return [dict(row) for row in result.mappings()]

async def get_latest_event_payload(
    db: AsyncSession, 
    message_id: str, 
    event_type: str
) -> Dict[str, Any]:
    stmt = (
        select(events.c.payload)
        .where(events.c.type == event_type)
        .where(events.c.payload["message_id"].astext == str(message_id))
        .order_by(events.c.ts.desc())
        .limit(1)
    )
    result = await db.execute(stmt)
    row = result.first()
    return row[0] if row else {}

async def get_ticket_details(db: AsyncSession, ticket_id: str):
    stmt = select(
        tickets.c.id, 
        tickets.c.message_id, 
        tickets.c.external_id, 
        tickets.c.status, 
        tickets.c.route, 
        tickets.c.summary
    ).where(tickets.c.id == ticket_id)
    
    result = await db.execute(stmt)
    return result.first()

async def get_message_body(db: AsyncSession, message_id: str) -> str:
    stmt = select(messages.c.body_text).where(messages.c.id == message_id)
    result = await db.execute(stmt)
    row = result.first()
    return row[0] if row else ""

async def update_ticket_fields(
    db: AsyncSession, 
    ticket_id: str, 
    message_id: str, 
    fields: Dict[str, Any]
) -> str:
    repo = MessageRepository(db)
    await repo.insert_event(
        ticket_id=str(ticket_id),
        message_id=str(message_id),
        type_="FIELD_EDITED",
        payload={
            "message_id": str(message_id), 
            "ticket_id": str(ticket_id), 
            "fields": fields
        },
    )

    summary_text = f"Order {fields.get('order_id') or 'N/A'} amount {fields.get('amount') or 'N/A'}"
    
    stmt = (
        update(tickets)
        .where(tickets.c.id == ticket_id)
        .values(
            summary=summary_text,
            updated_at=func.now()
        )
    )
    await db.execute(stmt)
    await db.commit()
    
    return summary_text

async def log_approval_event(
    db: AsyncSession, 
    ticket_id: str, 
    message_id: Optional[str], 
    reply: str
):
    repo = MessageRepository(db)
    await repo.insert_event(
        ticket_id=str(ticket_id),
        message_id=str(message_id) if message_id else None,
        type_="REPLY_APPROVED",
        payload={"ticket_id": str(ticket_id), "reply": reply},
    )
    await db.commit()