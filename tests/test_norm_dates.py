from datetime import date

from common.norm.dates import parse_date_eu


def test_parse_date_eu_basic():
    assert parse_date_eu("Order date: 01/02/2025") == date(2025, 2, 1)



def test_parse_date_eu_two_digit_year():
    assert parse_date_eu("date: 31-12-24") == date(2024, 12, 31)


def test_parse_date_eu_invalid():
    assert parse_date_eu("31/13/2025") is None
    assert parse_date_eu("no date here") is None
