"""init_schema

Revision ID: 78c9531d3cbe
Revises: 
Create Date: 2025-11-15 22:53:37.308901

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as psql

# revision identifiers, used by Alembic.
revision: str = '78c9531d3cbe'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
    op.execute("CREATE EXTENSION IF NOT EXISTS citext;")

    op.create_table(
        "messages",
        sa.Column("id", psql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("external_id", sa.Text(), nullable=True),
        sa.Column("subject", sa.Text(), nullable=True),
        sa.Column("from_addr", sa.Text(), nullable=True),
        sa.Column("ts", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("body_text", sa.Text(), nullable=True),
        sa.Column("raw", psql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.create_index("ix_messages_external_id", "messages", ["external_id"])
    op.create_index("ix_messages_ts", "messages", ["ts"])

    op.create_table(
        "extractions",
        sa.Column("id", psql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("message_id", psql.UUID(as_uuid=True), nullable=False),
        sa.Column("order_id", sa.Text(), nullable=True),
        sa.Column("sku", sa.Text(), nullable=True),
        sa.Column("amount", sa.Numeric(12, 2), nullable=True),
        sa.Column("currency", sa.Text(), nullable=True),
        sa.Column("order_date", sa.Date(), nullable=True),
        sa.Column("confidence_json", psql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.ForeignKeyConstraint(["message_id"], ["messages.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_extractions_message_id", "extractions", ["message_id"])

    op.create_table(
        "tickets",
        sa.Column("id", psql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("message_id", psql.UUID(as_uuid=True), nullable=False),
        sa.Column("external_id", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=True),
        sa.Column("route", sa.Text(), nullable=True),
        sa.Column("priority", sa.Text(), nullable=True),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("draft_reply", sa.Text(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.ForeignKeyConstraint(["message_id"], ["messages.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_tickets_route_status", "tickets", ["route", "status"])
    op.create_index("ix_tickets_message_id", "tickets", ["message_id"])

    op.create_table(
        "events",
        sa.Column("id", psql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("ticket_id", psql.UUID(as_uuid=True), nullable=True),
        sa.Column("type", sa.Text(), nullable=False),        # 'INGESTED','ASR_DONE',...
        sa.Column("payload", psql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("ts", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.ForeignKeyConstraint(["ticket_id"], ["tickets.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_events_ticket_ts", "events", ["ticket_id", "ts"])

    op.create_table(
        "attachments",
        sa.Column("id", psql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("message_id", psql.UUID(as_uuid=True), nullable=False),
        sa.Column("s3_key", sa.Text(), nullable=False),
        sa.Column("mime", sa.Text(), nullable=False),
        sa.Column("filename", sa.Text(), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("page_count", sa.Integer(), nullable=True),
        sa.Column("hash_sha256", sa.Text(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.ForeignKeyConstraint(["message_id"], ["messages.id"], ondelete="CASCADE"),
    )

    op.create_index(
        "ix_attachments_message_id",
        "attachments",
        ["message_id"],
    )
    op.create_index(
        "ix_attachments_hash_sha256",
        "attachments",
        ["hash_sha256"],
    )

def downgrade() -> None:
    op.drop_index("ix_events_ticket_ts", table_name="events")
    op.drop_table("events")

    op.drop_index("ix_tickets_message_id", table_name="tickets")
    op.drop_index("ix_tickets_route_status", table_name="tickets")
    op.drop_table("tickets")

    op.drop_index("ix_extractions_message_id", table_name="extractions")
    op.drop_table("extractions")

    op.drop_index("ix_messages_ts", table_name="messages")
    op.drop_index("ix_messages_external_id", table_name="messages")
    op.drop_table("messages")

    op.drop_index("ix_attachments_hash_sha256", table_name="attachments")
    op.drop_index("ix_attachments_message_id", table_name="attachments")
    op.drop_table("attachments")
