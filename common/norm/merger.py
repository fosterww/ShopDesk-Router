from .regexes import extract_amount_currency, extract_order_id, extract_sku
from .amounts import normalize_amount, normalize_currency
from .dates import parse_date_eu
from ..ml.types import DocFields
from . import NormalizedFields


def merge_fields(
    doc_fields: DocFields,
    body_text: str | None,
    transcript: str | None,
) -> NormalizedFields:
    source: dict[str, str] = {}
    conf: dict[str, float] = {}

    order_id = doc_fields.order_id
    order_conf = doc_fields.confidence.get("order_id", 0.0)
    if not order_id or order_conf < 0.7:
        text = (body_text or "") + " " + (transcript or "")
        regex_id = extract_order_id(text)
        if regex_id:
            order_id = regex_id
            source["order_id"] = "regex"
            conf["order_id"] = max(order_conf, 0.8)
    if order_id and "order_id" not in source:
        source["order_id"] = "docqa"
        conf["order_id"] = order_conf

    amount = doc_fields.amount
    amount_conf = doc_fields.confidence.get("amount", 0.0)
    currency = doc_fields.currency

    text_for_money = (body_text or "") + (transcript or "")
    amt_raw, curr_hint = extract_amount_currency(text_for_money)

    if (not amount or amount_conf < 0.7) and amt_raw:
        norm_amt = normalize_amount(amt_raw)
        if norm_amt is not None:
            amount = norm_amt
            source["amount"] = "regex"
            conf["amount"] = max(amount_conf, 0.8)

    else:
        if amount is not None:
            source["amount"] = "docqa"
            conf["amount"] = amount_conf

    if not currency and curr_hint:
        currency = normalize_currency(curr_hint)
        source["currency"] = "regex"
        conf["currency"] = 0.8
    elif currency:
        currency = normalize_currency(currency)
        source["currency"] = "docqa"
        conf["currency"] = doc_fields.confidence.get("currency", 0.7)

    order_date = doc_fields.order_date
    date_conf = doc_fields.confidence.get("order_date", 0.0)
    if not order_date or date_conf < 0.7:
        text_for_date = (body_text or "") + " " + (transcript or "")
        parsed = parse_date_eu(text_for_date)
        if parsed:
            order_date = parsed
            source["order_date"] = "regex"
            conf["order_date"] = max(order_conf, 0.8)
    else:
        source["order_date"] = "docqa"
        conf["order_date"] = date_conf

    sku = doc_fields.sku
    sku_conf = doc_fields.confidence.get("sku", 0.0)
    if not sku or sku_conf < 0.7:
        text_for_sku = (body_text or "") + " " + (transcript or "")
        sku_regex = extract_sku(text_for_sku)
        if sku_regex:
            sku = sku_regex
            source["sku"] = "regex"
            conf["sku"] = max(sku_conf, 0.8)
    else:
        source["sku"] = "docqa"
        conf["sku"] = sku_conf

    return NormalizedFields(
        order_id=order_id,
        amount=amount,
        currency=currency,
        order_date=order_date,
        sku=sku,
        source=source,
        confidence=conf,
    )
