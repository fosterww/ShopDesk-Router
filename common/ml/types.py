from pydantic import BaseModel
from typing import Literal, Optional
from datetime import date
from decimal import Decimal


class DocFields(BaseModel):
    order_id: Optional[str]
    amount: Optional[Decimal]
    currency: Optional[str]
    order_date: Optional[date]
    sku: Optional[str]
    confidence: dict[str, float] = {}


class Classification(BaseModel):
    label: Literal["refund", "not_received", "warranty", "address_change", "how_to", "other"]
    scores: dict[str, float]


class Transcript(BaseModel):
    text: str
    confidence: float


class Summary(BaseModel):
    text: str
    tokens: int
