import re
from typing import Optional, Tuple


ORDER_ID_RE = re.compile(
    r"\b(?:order\s*[:#]?\s*)?(?:#)?((?=[A-Z0-9-]{4,}\b)[A-Z0-9-]*\d[A-Z0-9-]*)\b",
    re.IGNORECASE,
)


AMOUNT_RE = re.compile(
    r"""
    (?P<currency_symbol_prefix>[$€£₴])?
    \s*
    (?P<amount>\d{1,3}(?:[ ,]\d{3})*(?:[.,]\d{2})?)
    \s*
    (?P<currency_code>USD|EUR|GBP|UAH|PLN)?
    \s*
    (?P<currency_symbol_suffix>[$€£₴])?
    """,
    re.IGNORECASE | re.VERBOSE,
)

SKU_RE = re.compile(r"(?:sku|item|product)\s*[:#]\s*([A-Z0-9\-]{3,})", re.IGNORECASE)


def extract_order_id(text: str) -> Optional[str]:
    match = ORDER_ID_RE.search(text)
    if not match:
        return None
    return match.group(1).strip()


def extract_amount_currency(text: str) -> Tuple[Optional[str], Optional[str]]:
    m = AMOUNT_RE.search(text)
    if not m:
        return None, None

    amount_raw = m.group("amount")
    curr_sym = m.group("currency_symbol_prefix") or m.group("currency_symbol_suffix")
    curr_code = m.group("currency_code")

    currency_hint = curr_code or {
        "$": "USD",
        "€": "EUR",
        "£": "GBP",
        "₴": "UAH",
    }.get(curr_sym or "")

    return amount_raw, currency_hint


def extract_sku(text: str) -> Optional[str]:
    m = SKU_RE.search(text)
    if not m:
        return None
    return m.group(1).strip()
