import pytest
from decimal import Decimal

from common.norm.amounts import normalize_amount, normalize_currency


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("59.99", Decimal("59.99")),
        ("1,234.56", Decimal("1234.56")),
        ("1 234,56", Decimal("1234.56")),
        ("  99,00 ", Decimal("99.00")),
        ("bad", None),
        ("", None),
        (None, None),
    ],
)
def test_normalize_amount(raw, expected):
    assert normalize_amount(raw) == expected


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("usd", "USD"),
        ("EUR", "EUR"),
        ("uah", "UAH"),
        (None, None),
        ("UNK", "UNK"),
    ],
)
def test_normalize_currency(raw, expected):
    assert normalize_currency(raw) == expected
