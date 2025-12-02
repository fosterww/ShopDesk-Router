import pytest

from common.norm.regexes import extract_amount_currency, extract_order_id, extract_sku


@pytest.mark.parametrize(
    "text,expected",
    [
        ("Order #A12345 is delayed", "A12345"),
        ("order: ORDER-2025-000123", "ORDER-2025-000123"),
        ("Your order 1234-5678 has shipped", "1234-5678"),
        ("No order id here", None),
    ],
)
def test_extract_order_id(text, expected):
    assert extract_order_id(text) == expected

@pytest.mark.parametrize(
    "text,amount,currency",
    [
        ("Total: $59.99", "59.99", "USD"),
        ("you paid 1,234.56 EUR", "1,234.56", "EUR"),
        ("сума: 1 234,56 ₴", "1 234,56", "UAH"),
        ("no money here", None, None),
    ],
)
def test_extract_amount_and_currency(text, amount, currency):
    res_amount, res_currency = extract_amount_currency(text)
    assert res_amount == amount
    assert res_currency == currency


@pytest.mark.parametrize(
    "text,expected",
    [
        ("SKU: ABC-123", "ABC-123"),
        ("product: XYZ999", "XYZ999"),
        ("item # QWE-2025", "QWE-2025"),
        ("no sku here", None),
    ],
)
def test_extract_sku(text, expected):
    assert extract_sku(text) == expected