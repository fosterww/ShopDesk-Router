from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from common.storage.s3 import put_bytes
from api.app.db import get_db

router = APIRouter(prefix="/ingest", tags=["ingest"])

@router.post("/upload")
async def ingest_upload(
    body: str | None = Form(None), 
    files: list[UploadFile] = File(...), 
    db: AsyncSession = Depends(get_db)
    ):
    if not files:
        raise HTTPException(status_code=400, detail="At least one file is required")

    result = await db.execute(
        text("""
            INSERT INTO messages (source, subject, from_addr, body_text)
            VALUES ('upload', NULL, NULL, :body_text)
            RETURNING id
        """),
        {"body_text": body},
    )
    message_id = result.scalar_one()

    attachments_out = []
    
    for file in files:
        file_bytes = await file.read()
        size_bytes = len(file_bytes)

        s3_key = await put_bytes(
            data=file_bytes,
            mime=file.content_type,
            filename=file.filename,
        )

        result = await db.execute(
            text("""
                 INSERT INTO attachments(
                 message_id, s3_key, mime, filename, size_bytes
                 )
                 VALUES (
                 :message_id, :s3_key, :mime, :filename, :size_bytes
                 )
                 RETURNING ID
            """),
            {
                "message_id": message_id,
                "s3_key": s3_key,
                "mime": file.content_type,
                "filename": file.filename,
                "size_bytes": size_bytes,
            }
        )
        attachment_id = result.scalar_one()

        attachments_out.append({
            "id": str(attachment_id),
            "filename": file.filename,
            "mime": file.content_type,
            "size_bytes": size_bytes,
            "s3_key": s3_key,
        })

    await db.execute(
        text("""
            INSERT INTO events (ticket_id, type, payload)
            VALUES (NULL, 'INGESTED', '{}' ::jsonb)
        """),
        {
            "payload": f'{{"message_id": "{message_id}"}}',
        }
    )

    await db.commit()

    return {
        "message_id": str(message_id),
        "attachments": attachments_out,
    }
