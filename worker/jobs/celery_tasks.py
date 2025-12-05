from sqlalchemy import text

from api.app.db import SessionLocal
from common.db.dao import MessageRepository
from common.ml.asr import transcribe
from common.ml.docqa import extract_fields
from common.ml.zeroshot import classify
from common.ml.types import Classification, DocFields
from common.storage.s3 import AttachmentStorage
from common.ml.vqa import is_damaged
from common.norm.merger import merge_fields
from common.ml.types import DocFields


async def _get_existing(repo: MessageRepository, message_id: str, type_: str):
    return await repo.get_last_event(message_id=str(message_id), type_=type_)


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
        asr_event = await _get_existing(repo, row.id, "ASR_DONE")
        if asr_event and isinstance(asr_event, dict):
            text_body = f"{text_body}\n{asr_event.get('text','')}".strip()

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
        if mime.startswith("application/pdf"):
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


async def _normalize_task(message_id: str) -> dict | None:
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

        existing = await _get_existing(repo, row.id, "NORMALIZE_DONE")
        if existing:
            return existing

        docqa_event = await _get_existing(repo, row.id, "DOCQA_DONE") or {}
        asr_event = await _get_existing(repo, row.id, "ASR_DONE") or {}

        doc_fields = DocFields(
            **(docqa_event.get("fields") or {}),
        ) if docqa_event.get("fields") else DocFields(
            order_id=None,
            amount=None,
            currency=None,
            order_date=None,
            sku=None,
            confidence={},
        )

        body_text = row.body_text or ""
        transcript = ""
        if isinstance(asr_event, dict):
            transcript = asr_event.get("text") or ""

        normalized = merge_fields(doc_fields, body_text, transcript)
        payload = {
            "message_id": str(row.id),
            "normalized": normalized.model_dump(),
        }
        await repo.insert_event(
            ticket_id=None,
            message_id=str(row.id),
            type_="NORMALIZE_DONE",
            payload=payload,
        )
        await session.commit()
        return payload
