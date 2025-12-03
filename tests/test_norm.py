from decimal import Decimal
from datetime import date

from common.ml.types import DocFields
from common.norm.merger import merge_fields


def test_merge_end_to_end_with_transcript():
    doc = DocFields(
        order_id="A10023",
        amount=None,
        currency=None,
        order_date=None,
        sku=None,
        confidence={"order_id": 0.5},
    )

    body = "Hi, my package never arrived. See attached receipt."
    transcript = (
        "Hello, I need a refund for order #WEB-999, "
        "it was 59.99 dollars on 10/05/2025."
    )

    normalized = merge_fields(doc, body, transcript)

    assert normalized.order_id == "WEB-999"
    assert normalized.amount == Decimal("59.99")
    assert normalized.currency == "USD"
    assert normalized.order_date == date(2025, 5, 10)
    assert normalized.source["order_id"] == "regex"
    assert normalized.source["amount"] == "regex"
    assert normalized.source["currency"] == "regex"
    assert normalized.source["order_date"] == "regex"
