from __future__ import annotations

import asyncio
import email.utils
import logging
from datetime import datetime, timezone

from google.oauth2.credentials import Credentials
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from worker.celery_app import app
from common.clients.gmail_client import GmailClient
from common.ingest.email_parser import parse_email
from common.storage.s3 import AttachmentStorage
from api.app.config import settings
from api.app.db import SessionLocal
from common.db.dao import MessageRepository

LOG = logging.getLogger(__name__)


def _load_gmail_creds() -> Credentials:
    token_path = settings.gmail_service_account_file or settings.gmail_user
    return Credentials.from_authorized_user_file(
        token_path, ["https://www.googleapis.com/auth/gmail.readonly"]
    )


@app.task(
    name="worker.jobs.gmail_poll.poll_gmail",
    bind=True,
    max_retries=3,
    default_retry_delay=10,
)

def poll_gmail(self, newest_n: int = 25):
    try:
        asyncio.run(_poll_gmail_async(newest_n=newest_n))
    except Exception as e:
        LOG.exception("gmail poll failed")
        raise self.retry(exc=e)


async def _poll_gmail_async(newest_n: int = 25):
    creds = _load_gmail_creds()
    client = GmailClient(creds)
    query = settings.gmail_query or "newer_than:1d"
    message_ids = client.list_message_ids(query=query, max_results=newest_n)
    LOG.info("Gmail poll: %s messages", len(message_ids))

    async with SessionLocal() as session:
        repo = MessageRepository(session)
        for mid in message_ids:
            await _process_message(mid, client, repo, session)


async def _process_message(mid: str, client: GmailClient, repo: MessageRepository, session: AsyncSession):
    s3 = AttachmentStorage()
    headers = client.get_headers(mid)
    raw = client.get_raw_message(mid)

    body_text, atts = parse_email(raw)

    try:
        ts = datetime.fromtimestamp(
            email.utils.mktime_tz(email.utils.parsedate_tz(headers.get("date"))),
            tz=timezone.utc,
        )
    except Exception:
        ts = datetime.now(timezone.utc)

    subject = headers.get("subject", "")
    from_addr = headers.get("from", "")
    external_id = headers.get("message_id") or mid

    existing = await session.execute(
        text(
            "select id from messages where source = :source and external_id = :external_id limit 1"
        ),
        {"source": "gmail", "external_id": external_id},
    )
    row = existing.scalar_one_or_none()
    if row:
        LOG.info("Skipping existing gmail message %s", external_id)
        return

    message_id = await repo.upsert_message(
        session,
        source="gmail",
        external_id=external_id,
        subject=subject,
        from_addr=from_addr,
        ts=ts,
        body_text=body_text,
    )

    uploaded = []
    for a in atts:
        s3_key = await s3.put(
            data=a["bytes"],
            mime=a["mime"],
            filename=a["filename"],
            bucket=settings.s3_bucket,
        )
        uploaded.append(
            {
                "filename": a["filename"],
                "mime": a["mime"],
                "size_bytes": len(a["bytes"]),
                "s3_key": s3_key,
            }
        )
    if uploaded:
        await repo.insert_attachments(session, message_id, uploaded)

    await repo.insert_event(
        session,
        ticket_id=None,
        message_id=message_id,
        type_="INGESTED",
        payload={
            "source": "gmail",
            "external_id": external_id,
            "message_id": str(message_id),
            "attachments": [u["filename"] for u in uploaded],
        },
    )

    await session.commit()
