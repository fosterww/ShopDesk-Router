import io
from pdf2image import convert_from_bytes
from PIL import Image
from transformers import pipeline

from typing import Any

from . import use_stub
from .types import DocFields

import anyio

_qa_pipeline = None


def _get_pipeline():
    global _qa_pipeline
    if _qa_pipeline is None:
        _qa_pipeline = pipeline(
            "document-question-answering",
            model="impira/layoutlm-document-qa",
        )
    return _qa_pipeline


def _load_pages(doc_bytes, mime):
    if mime.startswith("application/pdf"):
        return convert_from_bytes(doc_bytes)
    img = Image.open(io.BytesIO(doc_bytes)).convert("RGB")
    return [img]


def extract_fields_sync(doc_bytes: bytes, mime: str) -> DocFields:
    if use_stub():
        ...

    qa = _get_pipeline()
    pages = _load_pages(doc_bytes, mime)
    if not pages:
        return DocFields(
            order_id=None,
            amount=None,
            currency=None,
            order_date=None,
            sku=None,
            confidence={},
        )
    page = pages[0]
    questions = {
        "order_id": "What is the order number?",
        "amount": "What is the total number?",
        "currency": "What is the currency?",
        "order_date": "What is the date of the order?",
        "sku": "What is the SKU or item code?",
    }

    answers: dict[str, Any] = {}
    for field, q in questions.items():
        out = qa(question=q, image=page)[0]
        answers[field] = out

    return DocFields(
        order_id=answers["order_id"]["answer"],
        amount=None,
        currency=None,
        order_date=None,
        sku=answers["sku"]["answer"],
        confidence={k: float(v.get("score", 0.0)) for k, v in answers.items()},
    )


async def extract_fields(doc_bytes: bytes, mime: str) -> DocFields:
    return await anyio.to_thread.run_sync(
        extract_fields_sync,
        doc_bytes,
        mime,
    )
