from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from common.storage.s3 import presign
from api.app.db import get_db

router = APIRouter(prefix="/attachments", tags=["attachments"])

@router.get("/{attachment_id}/presing")
async def presign_attachment(attachment_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        text("select s3_key from attachments where id = :id"),
        {"id": attachment_id},
    )
    s3_key = result.scalar_one_or_none()
    if not s3_key:
        raise HTTPException(status_code=404, detail="Attachment not found")
    return {"url": await presign(s3_key), "s3_key": s3_key, "expires_in": 600}
