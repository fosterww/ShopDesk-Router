from fastapi import APIRouter, UploadFile, File, Form, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from api.app.db import get_db
from common.ingest.upload_service import service

router = APIRouter(prefix="/ingest", tags=["ingest"])


@router.post("/upload")
async def ingest_upload(
    body: str | None = Form(None),
    files: list[UploadFile] = File(...),
    db: AsyncSession = Depends(get_db),
):
    return await service(body=body, files=files, session=db)
