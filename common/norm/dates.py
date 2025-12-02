from datetime import date
from typing import Optional
import re

DATE_RE = re.compile(
    r"""
    (?P<day>\d{1,2})[./-](?P<month>\d{1,2})[./-](?P<year>\d{2,4})
    """,
    re.VERBOSE,
)


def parse_date_eu(raw: str) -> Optional[date]:
    m = DATE_RE.search(raw)
    if not m:
        return None

    day = int(m.group("day"))
    month = int(m.group("month"))
    year = int(m.group("year"))
    if year < 100:
        year += 2000
    try:
        return date(year, month, day)
    except ValueError:
        return None
