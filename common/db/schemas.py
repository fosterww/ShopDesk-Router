from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Text,
    Integer,
    BigInteger,
    Numeric,
    Date,
    ForeignKey,
    TIMESTAMP,
    text
)
from sqlalchemy.dialects.postgresql import UUID, JSONB

metadata = MetaData()

messages = Table(
    "messages",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column("source", Text, nullable=False),
    Column("external_id", Text, nullable=True),
    Column("subject", Text, nullable=True),
    Column("from_addr", Text, nullable=True),
    Column("ts", TIMESTAMP(timezone=True), nullable=False, server_default=text("NOW()")),
    Column("body_text", Text, nullable=True),
    Column("raw", JSONB(astext_type=Text), nullable=True),
    Column("source_meta", JSONB(astext_type=Text), nullable=True),
)

extractions = Table(
    "extractions",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column("message_id", UUID(as_uuid=True), ForeignKey("messages.id", ondelete="CASCADE"), nullable=False),
    Column("order_id", Text, nullable=True),
    Column("sku", Text, nullable=True),
    Column("amount", Numeric(12, 2), nullable=True),
    Column("currency", Text, nullable=True),
    Column("order_date", Date, nullable=True),
    Column("confidence_json", JSONB(astext_type=Text), nullable=True),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("NOW()")),
)

tickets = Table(
    "tickets",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column("message_id", UUID(as_uuid=True), ForeignKey("messages.id", ondelete="CASCADE"), nullable=False),
    Column("external_id", Text, nullable=True),
    Column("status", Text, nullable=True),
    Column("route", Text, nullable=True),
    Column("priority", Text, nullable=True),
    Column("summary", Text, nullable=True),
    Column("draft_reply", Text, nullable=True),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("NOW()")),
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("NOW()")),
)

events = Table(
    "events",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column("ticket_id", UUID(as_uuid=True), ForeignKey("tickets.id", ondelete="CASCADE"), nullable=True),
    Column("type", Text, nullable=False),  # e.g., 'INGESTED', 'ASR_DONE'
    Column("payload", JSONB(astext_type=Text), nullable=True),
    Column("ts", TIMESTAMP(timezone=True), nullable=False, server_default=text("NOW()")),
)

attachments = Table(
    "attachments",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")),
    Column("message_id", UUID(as_uuid=True), ForeignKey("messages.id", ondelete="CASCADE"), nullable=False),
    Column("s3_key", Text, nullable=False),
    Column("mime", Text, nullable=False),
    Column("filename", Text, nullable=True),
    Column("size_bytes", BigInteger, nullable=True),
    Column("page_count", Integer, nullable=True),
    Column("hash_sha256", Text, nullable=True),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, server_default=text("NOW()")),
)