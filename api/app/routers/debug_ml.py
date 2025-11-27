from fastapi import APIRouter, UploadFile, File
from common.ml.docqa import extract_fields
from common.ml.zeroshot import classify


router = APIRouter(prefix="/debug", tags=["debug"])

@router.post("/docqa")
async def debug_docqa(file: UploadFile = File(...)):
    content = await file.read()
    fields = await extract_fields(content, file.content_type)
    return fields

@router.post("/classify")
async def debug_classify(body: str):
    result = await classify(body)
    return result

