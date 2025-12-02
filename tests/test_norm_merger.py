from datetime import date
from decimal import Decimal

from common.ml.types import DocFields
from common.norm.merger import merge_fields


def make_docfields(
    order_id=None,
    amount=None,
    currency=None,
    order_date=None,
    sku=None,
    confidence=None,
) -> DocFields:
    return DocFields(
        order_id=order_id,
        amount=amount,
        currency=currency,
        order_date=order_date,
        sku=sku,
        confidence=confidence or {},
    )


def test_merge_prefers_high_conf_docqa_order_id():
    doc = make_docfields(
        order_id="DOCQA-123",
        confidence={"order_id": 0.9},
    )
    body = "order #BODY-456"
    normalized = merge_fields(doc, body, None)

    assert normalized.order_id == "DOCQA-123"
    assert normalized.source["order_id"] == "docqa"
    assert normalized.confidence["order_id"] == 0.9


def test_merge_uses_regex_when_docqa_low_conf_order_id():
    doc = make_docfields(
        order_id="DOCQA-123",
        confidence={"order_id": 0.3},
    )
    body = "Hello, I have an issue with order #BODY-456, please help."
    normalized = merge_fields(doc, body, None)

    assert normalized.order_id == "BODY-456"
    assert normalized.source["order_id"] == "regex"
    assert normalized.confidence["order_id"] >= 0.8


def test_merge_amount_and_currency_from_regex():
    doc = make_docfields(
        amount=None,
        currency=None,
        confidence={"amount": 0.1, "currency": 0.1},
    )
    body = "Total: 1,234.56 EUR for this order."
    normalized = merge_fields(doc, body, None)

    assert normalized.amount == Decimal("1234.56")
    assert normalized.currency == "EUR"
    assert normalized.source["amount"] == "regex"
    assert normalized.source["currency"] == "regex"



def test_merge_keeps_docqa_when_confident():
    doc = make_docfields(
        order_id="DOCQA-KEEP",
        amount=Decimal("59.99"),
        currency="USD",
        order_date=date(2025, 1, 2),
        sku="SKU-123",
        confidence={
            "order_id": 0.95,
            "amount": 0.9,
            "currency": 0.9,
            "order_date": 0.9,
            "sku": 0.9,
        },
    )
    body = "order #BODY-456; total 100.00 EUR; date 01/01/2025; SKU ABC-999"
    normalized = merge_fields(doc, body, None)

    assert normalized.order_id == "DOCQA-KEEP"
    assert normalized.amount == Decimal("59.99")
    assert normalized.currency == "USD"
    assert normalized.order_date == date(2025, 1, 2)
    assert normalized.sku == "SKU-123"
    assert normalized.source["order_id"] == "docqa"
    assert normalized.source["amount"] == "docqa"
    assert normalized.source["currency"] == "docqa"
    assert normalized.source["order_date"] == "docqa"
    assert normalized.source["sku"] == "docqa"
