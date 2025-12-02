from pydantic import BaseModel
from datetime import date
from decimal import Decimal
from typing import Optional


class NormalizedFields(BaseModel):
    order_id: Optional[str]
    amount: Optional[Decimal]
    currency: Optional[str]
    order_date: Optional[date]
    sku: Optional[str]
    source: dict[str, str] = {}
    confidence: dict[str, float] = {}
