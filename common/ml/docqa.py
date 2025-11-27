from datetime import date
from decimal import Decimal
from typing import Any

from transformers import pipeline

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

def extract_fields_sync(doc_bytes: bytes, mime: str) -> DocFields:
    if use_stub():
        return DocFields(
            order_id="A10023",
            amount=Decimal("59.99"),
            currency="USD",
            order_date=date(2025, 11, 10),
            sku="SKU-123",
            confidence={"order_id": 0.98, "amount": 0.93},
        )
    
    qa = _get_pipeline()
    questions = {
        "order_id": "What is the order number?",
        "amount": "What is the total number?",
        "currency": "What is the currency?",
        "order_date": "What is the date of the order?",
        "sku": "What is the SKU or item code?",
    }

    answers = dict[str, Any] = {}
    for field, q in questions.items():
        out = qa(question=q, image=doc_bytes)[0]
        answers[field] = out

    return DocFields(
        order_id=answers["order_id"]["answer"],
        amount=None,
        currency=None,
        order_date=None,
        sku=answers["sku"]["answer"],
        confidence={
            field: float(val.get("score", 0.0)) for field, val in answers.items()
        },
    )

async def extract_fields(doc_bytes: bytes, mime: str) -> DocFields:
    return await anyio.to_thread.run_sync(
        extract_fields_sync,
        doc_bytes,
        mime,
    )