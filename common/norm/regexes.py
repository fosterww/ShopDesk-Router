import re
from typing import Optional, Tuple


CURRENCY_SYMBOL_MAP = {
    "$": "USD",
    "\u20ac": "EUR",
    "\u00a3": "GBP",
    "\u20b4": "UAH",
}

CURRENCY_WORD_MAP = {
    "dollar": "USD",
    "dollars": "USD",
    "usd": "USD",
    "eur": "EUR",
    "euro": "EUR",
    "gbp": "GBP",
    "pound": "GBP",
    "pounds": "GBP",
    "uah": "UAH",
    "hryvnia": "UAH",
    "pln": "PLN",
    "zloty": "PLN",
}


ORDER_ID_RE = re.compile(
    r"\b(?:order\s*[:#]?\s*)?(?:#)?((?=[A-Z0-9-]{4,}\b)[A-Z0-9-]*\d[A-Z0-9-]*)\b",
    re.IGNORECASE,
)


AMOUNT_RE = re.compile(
    r"""
    (?P<currency_symbol_prefix>[\$\u20ac\u00a3\u20b4])?
    \s*
    (?P<amount>\d{1,3}(?:[ ,]\d{3})*(?:[.,]\d{2})?)
    \s*
    (?P<currency_code>USD|EUR|GBP|UAH|PLN)?
    \s*
    (?P<currency_symbol_suffix>[\$\u20ac\u00a3\u20b4])?
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
    best_amount = None
    best_currency = None
    best_score: tuple[int, int, int] = (-1, -1, -1)

    for m in AMOUNT_RE.finditer(text):
        amount_start, amount_end = m.span("amount")
        if amount_start > 0 and (text[amount_start - 1].isalnum() or text[amount_start - 1] == "-"):
            continue
        if amount_end < len(text) and text[amount_end].isdigit():
            continue

        amount_raw = m.group("amount")
        curr_sym = m.group("currency_symbol_prefix") or m.group("currency_symbol_suffix")
        curr_code = m.group("currency_code")

        currency_hint = curr_code or CURRENCY_SYMBOL_MAP.get(curr_sym or "")

        if not currency_hint:
            window_start = max(0, amount_start - 12)
            window_end = min(len(text), amount_end + 12)
            window = text[window_start:window_end].lower()
            for word, code in CURRENCY_WORD_MAP.items():
                if word in window:
                    currency_hint = code
                    break

        has_decimal = len(amount_raw) >= 3 and amount_raw[-3] in {".", ","}
        score = (1 if currency_hint else 0, 1 if has_decimal else 0, len(amount_raw))

        if score > best_score:
            best_amount = amount_raw
            best_currency = currency_hint
            best_score = score

    return best_amount, best_currency


def extract_sku(text: str) -> Optional[str]:
    m = SKU_RE.search(text)
    if not m:
        return None
    return m.group(1).strip()
