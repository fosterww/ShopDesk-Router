import json
from typing import List, Dict, Any

from fastapi import UploadFile, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from common.storage.s3 import AttachmentStorage


class IngestUploadService:
    def __init__(self, storage: AttachmentStorage) -> None:
        self.storage = storage

    async def __call__(
        self, *, body: str | None, files: List[UploadFile], session: AsyncSession
    ) -> Dict[str, Any]:
        if not files:
            raise HTTPException(status_code=400, detail="At least one file is required")

        result = await session.execute(
            text(
                """
                INSERT INTO messages (source, subject, from_addr, body_text)
                VALUES ('upload', NULL, NULL, :body_text)
                RETURNING id
            """
            ),
            {"body_text": body},
        )
        message_id = result.scalar_one()

        attachments_out = []

        for file in files:
            file_bytes = await file.read()
            size_bytes = len(file_bytes)

            s3_key = await self.storage.put(
                data=file_bytes,
                mime=file.content_type,
                filename=file.filename or "upload.bin",
            )

            result = await session.execute(
                text(
                    """
                     INSERT INTO attachments(
                     message_id, s3_key, mime, filename, size_bytes
                     )
                     VALUES (
                     :message_id, :s3_key, :mime, :filename, :size_bytes
                     )
                     RETURNING ID
                """
                ),
                {
                    "message_id": message_id,
                    "s3_key": s3_key,
                    "mime": file.content_type,
                    "filename": file.filename,
                    "size_bytes": size_bytes,
                },
            )
            attachment_id = result.scalar_one()

            attachments_out.append(
                {
                    "id": str(attachment_id),
                    "filename": file.filename,
                    "mime": file.content_type,
                    "size_bytes": size_bytes,
                    "s3_key": s3_key,
                }
            )

        await session.execute(
            text(
                """
                INSERT INTO events (ticket_id, type, payload)
                VALUES (NULL, 'INGESTED', :payload ::jsonb)
            """
            ),
            {
                "payload": json.dumps({"message_id": str(message_id)}),
            },
        )

        await session.commit()

        return {
            "message_id": str(message_id),
            "attachments": attachments_out,
        }


service = IngestUploadService(storage=AttachmentStorage())
