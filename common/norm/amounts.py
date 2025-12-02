from decimal import Decimal, InvalidOperation
from typing import Optional

def normalize_amount(raw: str) -> Optional[Decimal]:
    if not raw:
        return None
    s = raw.strip()
    if s.count(",") == 1 and "." not in s:
        s = s.replace(" ", "").replace(",", ".")
    else:
        s = s.replace(",", "")
    lower = s.lower()
    for word in ("dollars", "usd"):
        if lower.endswith(word):
            s = s[: -len(word)].strip()
            lower = s.lower()
    try:
        return Decimal(s)
    except InvalidOperation:
        return None
    

def normalize_currency(curr: str | None) -> Optional[str]:
    if not curr:
        return None
    c = curr.strip().upper()
    mapping = {
        "USD": "USD",
        "EUR": "EUR",
        "GBP": "GBP",
        "UAH": "UAH",
        "PLN": "PLN",
    }
    return mapping.get(c, c)
