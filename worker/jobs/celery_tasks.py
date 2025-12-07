from sqlalchemy import text

from datetime import datetime

from api.app.db import SessionLocal
from common.db.dao import MessageRepository
from common.ml.asr import transcribe
from common.ml.docqa import extract_fields
from common.ml.zeroshot import classify
from common.ml.types import Classification, DocFields
from common.storage.s3 import AttachmentStorage
from common.ml.vqa import is_damaged
from common.norm.merger import merge_fields


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



async def _is_damaged_task(attachment_id: str) -> bool | None:
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

        unsupported_reason = None
        if not (mime.startswith("application/pdf") or mime.startswith("image/")):
            unsupported_reason = "unsupported_mime"
        elif mime.startswith("application/pdf"):
            unsupported_reason = "pdf_not_supported"

        if unsupported_reason:
            payload = {
                "attachment_id": str(attachment_id),
                "message_id": str(row.message_id),
                "is_damaged": None,
                "reason": unsupported_reason,
                "mime": mime,
            }
            await repo.insert_event(
                ticket_id=None,
                message_id=str(row.message_id),
                type_="VQA_DONE",
                payload=payload,
            )
            await session.commit()
            return None

        damaged = await is_damaged(obj["data"])
        payload = {
            "attachment_id": str(attachment_id),
            "message_id": str(row.message_id),
            "is_damaged": damaged,
            "mime": mime,
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


async def _get_attachments_for_fanout(message_id: str) -> list[dict]:
    async with SessionLocal() as session:
        result = await session.execute(
            text("select id, mime, s3_key from attachments where message_id = :mid"),
            {"mid": message_id},
        )
        rows = result.mappings().all()
        return [
            {"id": str(r["id"]), "mime": r["mime"], "s3_key": r["s3_key"]}
            for r in rows
        ]


async def _fanout_ingested(message_id: str) -> dict:
    from worker.celery_app import app

    async with SessionLocal() as session:
        repo = MessageRepository(session)
        existing = await _get_existing(repo, message_id, "INGESTED_FANOUT")
        if existing:
            return existing

    attachments = await _get_attachments_for_fanout(message_id)
    dispatched: list[dict] = []
    for att in attachments:
        att_id = att["id"]
        mime = att.get("mime") or ""
        if mime.startswith("audio/"):
            tid = f"{message_id}:asr:{att_id}"
            app.send_task("pipeline.asr", args=[att_id], task_id=tid)
            dispatched.append({"task": "asr", "attachment_id": att_id, "task_id": tid})
        elif mime.startswith("application/pdf") or mime.startswith("image/"):
            tid = f"{message_id}:docqa:{att_id}"
            app.send_task("pipeline.docqa", args=[att_id], task_id=tid)
            dispatched.append({"task": "docqa", "attachment_id": att_id, "task_id": tid})

    payload = {"message_id": str(message_id), "dispatched": dispatched}
    async with SessionLocal() as session:
        repo = MessageRepository(session)
        await repo.insert_event(
            ticket_id=None,
            message_id=str(message_id),
            type_="INGESTED_FANOUT",
            payload=payload,
        )
        await session.commit()
    return payload


async def _choose_best_docqa(message_id: str) -> dict | None:
    async with SessionLocal() as session:
        repo = MessageRepository(session)
        existing = await _get_existing(repo, message_id, "DOCQA_SELECTED")
        if existing:
            return existing

        result = await session.execute(
            text(
                """
                select payload
                from events
                where type = 'DOCQA_DONE'
                  and payload->>'message_id' = :mid
                order by ts desc
                """
            ),
            {"mid": message_id},
        )
        rows = result.mappings().all()
        if not rows:
            return None

        def score(payload: dict):
            fields = payload.get("fields") or {}
            conf = fields.get("confidence") or {}
            has_order = 1 if fields.get("order_id") else 0
            return (has_order, conf.get("order_id", 0.0), conf.get("amount", 0.0))

        best_payload = max((r["payload"] for r in rows if r.get("payload")), key=score, default=None)
        if not best_payload:
            return None

        payload = {
            "message_id": str(message_id),
            "attachment_id": best_payload.get("attachment_id"),
            "fields": best_payload.get("fields"),
        }
        await repo.insert_event(
            ticket_id=None,
            message_id=str(message_id),
            type_="DOCQA_SELECTED",
            payload=payload,
        )
        await session.commit()
        return payload


async def _create_ticket(message_id: str) -> dict | None:
    async with SessionLocal() as session:
        repo = MessageRepository(session)
        existing = await _get_existing(repo, message_id, "TICKET_CREATED")
        if existing:
            return existing

        ticket_row = (
            await session.execute(
                text("select id from tickets where message_id = :mid"),
                {"mid": message_id},
            )
        ).first()
        if ticket_row:
            payload = {"message_id": str(message_id), "ticket_id": str(ticket_row.id)}
            await repo.insert_event(
                ticket_id=str(ticket_row.id),
                message_id=str(message_id),
                type_="TICKET_CREATED",
                payload=payload,
            )
            await session.commit()
            return payload

        classify_event = await _get_existing(repo, message_id, "CLASSIFY_DONE") or {}
        summary_event = await _get_existing(repo, message_id, "SUMMARY_DONE") or {}
        normalize_event = await _get_existing(repo, message_id, "NORMALIZE_DONE") or {}
        selected_docqa = await _get_existing(repo, message_id, "DOCQA_SELECTED") or {}

        summary_text = summary_event.get("summary") if isinstance(summary_event, dict) else ""
        route = classify_event.get("label") if isinstance(classify_event, dict) else None
        normalized = normalize_event.get("normalized") if isinstance(normalize_event, dict) else None
        doc_fields = selected_docqa.get("fields") if isinstance(selected_docqa, dict) else None

        result = await session.execute(
            text(
                """
                insert into tickets(message_id, status, route, summary, created_at, updated_at)
                values (:message_id, :status, :route, :summary, :now, :now)
                returning id
                """
            ),
            {
                "message_id": message_id,
                "status": "new",
                "route": route,
                "summary": summary_text,
                "now": datetime.utcnow(),
            },
        )
        ticket_id = str(result.scalar_one())

        payload = {
            "message_id": str(message_id),
            "ticket_id": ticket_id,
            "route": route,
            "summary": summary_text,
            "normalized": normalized,
            "doc_fields": doc_fields,
        }
        await repo.insert_event(
            ticket_id=ticket_id,
            message_id=str(message_id),
            type_="TICKET_CREATED",
            payload=payload,
        )
        await session.commit()
        return payload
