import asyncio
import os

from celery import Celery
from sqlalchemy import text

from api.app.db import SessionLocal
from common.db.dao import MessageRepository
from common.ml.asr import transcribe
from common.ml.docqa import extract_fields
from common.ml.zeroshot import classify
from common.ml.types import Classification
from common.storage.s3 import AttachmentStorage
from common.ml.vqa import is_damaged


broker_url = os.environ.get("REDIS_URL", "redis://redis:6379/0")
app = Celery(
    "shopdesk",
    broker=broker_url,
    backend=broker_url,
    include=[
        "worker.celery_app",
        "worker.jobs.gmail_poll",
    ],
)

_loop = asyncio.new_event_loop()
asyncio.set_event_loop(_loop)

def run_coro(coro):
    return _loop.run_until_complete(coro)

async def _get_existing(repo: MessageRepository, message_id: str, type_: str):
    return await repo.get_last_event(message_id=str(message_id), type_=type_)

@app.task(name="ping")
def ping():
    return "pong"


@app.task(name="pipeline.asr", bind=True, max_retries=3, default_retry_delay=10)
def asr_task(self, attachment_id: str) -> str | None:
    try:
        return run_coro(_asr_task(attachment_id))
    except Exception as exc:
        raise self.retry(exc=exc)


async def _asr_task(attachment_id: str) -> str | None:
    async with SessionLocal() as session:
        repo = MessageRepository(session)
        row = (
            await session.execute(
                text("select message_id, s3_key, mime from attachments where id = :id"),
                {"id": attachment_id},
            )
        ).first()
        if not row:
            return None

        existing = await _get_existing(repo, row.message_id, "ASR_DONE")
        if existing:
            return existing

        storage = AttachmentStorage()
        obj = await storage.get(row.s3_key)
        mime = row.mime or obj.get("mime") or "application/octet-stream"
        if not mime.startswith("audio/"):
            return None

        transc = await transcribe(obj["data"], mime)

        await repo.insert_event(
            ticket_id=None,
            message_id=str(row.message_id),
            type_="ASR_DONE",
            payload={
                "attachment_id": str(attachment_id),
                "message_id": str(row.message_id),
                "text": transc.text,
                "confidence": transc.confidence,
            },
        )
        await session.commit()
        return transc.text


@app.task(name="pipeline.docqa", bind=True, max_retries=3, default_retry_delay=10)
def docqa_task(self, attachment_id: str) -> dict | None:
    try:
        return run_coro(_docqa_task(attachment_id))
    except Exception as exc:
        raise self.retry(exc=exc)


async def _docqa_task(attachment_id: str) -> dict | None:
    async with SessionLocal() as session:
        repo = MessageRepository(session)
        row = (
            await session.execute(
                text("select message_id, s3_key, mime from attachments where id = :id"),
                {"id": attachment_id},
            )
        ).first()
        if not row:
            return None

        existing = await _get_existing(repo, row.message_id, "DOCQA_DONE")
        if existing:
            return existing

        storage = AttachmentStorage()
        obj = await storage.get(row.s3_key)
        mime = row.mime or obj.get("mime") or "application/octet-stream"
        if not (mime.startswith("application/pdf") or mime.startswith("image/")):
            return None

        fields = await extract_fields(obj["data"], mime)
        payload = {
            "attachment_id": str(attachment_id),
            "message_id": str(row.message_id),
            "fields": fields.model_dump(),
        }

        await repo.insert_event(
            ticket_id=None,
            message_id=str(row.message_id),
            type_="DOCQA_DONE",
            payload=payload,
        )
        await session.commit()
        return payload


@app.task(name="pipeline.zeroshot", bind=True, max_retries=3, default_retry_delay=10)
def classify_task(self, message_id: str) -> dict | None:
    try:
        return run_coro(_classify_task(message_id))
    except Exception as exc:
        raise self.retry(exc=exc)


async def _classify_task(message_id: str) -> dict | None:
    async with SessionLocal() as session:
        repo = MessageRepository(session)
        row = (
            await session.execute(
                text("select id, body_text from messages where id = :id"),
                {"id": message_id},
            )
        ).first()
        if not row:
            return None

        existing = await _get_existing(repo, row.id, "CLASSIFY_DONE")
        if existing:
            return existing

        text_body = row.body_text or ""
        classification: Classification = await classify(text_body)
        payload = {
            "message_id": str(row.id),
            "label": classification.label,
            "scores": classification.scores,
        }

        await repo.insert_event(
            ticket_id=None,
            message_id=str(row.id),
            type_="CLASSIFY_DONE",
            payload=payload,
        )
        await session.commit()
        return payload


@app.task(name="pipeline.summarize", bind=True, max_retries=3, default_retry_delay=10)
def summarize_task(self, message_id: str) -> dict | None:
    try:
        return run_coro(_summarize_task(message_id))
    except Exception as exc:
        raise self.retry(exc=exc)


async def _summarize_task(message_id: str) -> dict | None:
    async with SessionLocal() as session:
        repo = MessageRepository(session)
        row = (
            await session.execute(
                text("select id, body_text from messages where id = :id"),
                {"id": message_id},
            )
        ).first()
        if not row:
            return None

        existing = await _get_existing(repo, row.id, "SUMMARY_DONE")
        if existing:
            return existing
        
        summary_text = (row.body_text or "")[:500]
        payload = {"message_id": str(row.id), "summary": summary_text}
        await repo.insert_event(
            ticket_id=None,
            message_id=str(row.id),
            type_="SUMMARY_DONE",
            payload=payload,
        )
        await session.commit()
        return payload


@app.task(name="pipeline.vqa", bind=True, max_retries=3, default_retry_delay=10)
def is_damaged_task(self, attachment_id: str) -> bool:
    try:
        return run_coro(_is_damaged_task(attachment_id))
    except Exception as exc:
        raise self.retry(exc=exc)


async def _is_damaged_task(attachment_id: str) -> bool:
    async with SessionLocal() as session:
        repo = MessageRepository(session)
        row = (
            await session.execute(
                text("select message_id, s3_key, mime from attachments where id = :id"),
                {"id": attachment_id},
            )
        ).first()
        if not row:
            return None

        existing = await _get_existing(repo, row.message_id, "VQA_DONE")
        if existing:
            return existing
        
        storage = AttachmentStorage()
        obj = await storage.get(row.s3_key)
        mime = row.mime or obj.get("mime") or "application/octet-stream"
        if not (mime.startswith("application/pdf") or mime.startswith("image/")):
            return None
        
        damaged = await is_damaged(obj["data"])
        payload = {
            "attachment_id": str(attachment_id),
            "message_id": str(row.message_id),
            "is_damaged": damaged,
        }

        await repo.insert_event(
            ticket_id=None,
            message_id=str(row.message_id),
            type_="VQA_DONE",
            payload=payload,
        )
        await session.commit()
        return damaged


@app.task(name="pipeline.normalized", bind=True, max_retries=3, default_retry_delay=10)
def normalized_task(self, message_id: str) -> dict | None:
    try:
        return run_coro(_normalize_task(message_id))
    except Exception as exc:
        raise self.retry(exc=exc)


async def _normalize_task(message_id: str) -> dict | None:
    async with SessionLocal() as session:
        repo = MessageRepository(session)
        row = (
            await session.execute(
                text("select id from messages where id = :id"),
                {"id": message_id},
            )
        ).first()
        if not row:
            return None

        existing = await _get_existing(repo, row.id, "NORMALIZE_DONE")
        if existing:
            return existing

        payload = {"message_id": str(row.id), "normalized": True}
        await repo.insert_event(
            ticket_id=None,
            message_id=str(row.id),
            type_="NORMALIZE_DONE",
            payload=payload,
        )
        await session.commit()
        return payload


app.conf.beat_schedule = {
    "gmail-poll-every-60s": {
        "task": "worker.jobs.gmail_poll.poll_gmail",
        "schedule": 60.0,
        "args": (25,),
    }
}
app.conf.timezone = "UTC"

if __name__ == "__main__":
    res = ping.delay()
    print("Celery app delay:", res.get(timeout=5))
