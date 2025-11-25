from __future__ import annotations
from typing import List, Dict, Optional, Any
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def upsert_message(
    session: AsyncSession,
    *,
    source: str,
    external_id: str,
    subject: str,
    from_addr: str,
    ts: datetime,
    body_text: str,
) -> str:
    result = await session.execute(
        text(
            """
            insert into messages(source, external_id, subject, from_addr, ts, body_text)
            values (:source, :external_id, :subject, :from_addr, :ts, :body_text)
            on conflict (source, external_id)
            do update set subject = excluded.subject
            returning id
            """
        ),
        {
            "source": source,
            "external_id": external_id,
            "subject": subject,
            "from_addr": from_addr,
            "ts": ts,
            "body_text": body_text,
        },
    )
    return str(result.scalar_one())


async def insert_event(
    session: AsyncSession,
    *,
    ticket_id: Optional[str],
    message_id: Optional[str],
    type_: str,
    payload: dict,
) -> None:
    await session.execute(
        text(
            """
            insert into events(ticket_id, type, payload, ts)
            values (:ticket_id, :type, :payload::jsonb, now())
            """
        ),
        {
            "ticket_id": ticket_id,
            "type": type_,
            "payload": payload,
        },
    )


async def insert_attachments(
    session: AsyncSession, message_id: str, atts: List[Dict[str, Any]]
) -> List[str]:
    rows = [
        {
            "message_id": message_id,
            "s3_key": a["s3_key"],
            "mime": a["mime"],
            "filename": a["filename"],
            "size_bytes": a["size_bytes"],
        }
        for a in atts
    ]
    result = await session.execute(
        text(
            """
            insert into attachments(message_id, s3_key, mime, filename, size_bytes)
            values (:message_id, :s3_key, :mime, :filename, :size_bytes)
            returning id
            """
        ),
        rows,
    )
    return [str(row[0]) for row in result.fetchall()]
