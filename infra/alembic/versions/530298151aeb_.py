"""empty message

Revision ID: 530298151aeb
Revises: 78c9531d3cbe
Create Date: 2025-11-22 20:39:37.173545

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = '530298151aeb'
down_revision: Union[str, None] = '78c9531d3cbe'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    with op.batch_alter_table("messages") as batch:
        batch.add_column(sa.Column("source_meta", postgresql.JSONB(astext_type=sa.Text()), nullable=True))


    bind = op.get_bind()
    is_pg = bind.dialect.name == "postgresql"

    if is_pg:
        op.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_messages_external_id_gmail
            ON messages (external_id)
            WHERE source = 'gmail' AND external_id IS NOT NULL
            """
        )
    else:
        op.create_index(
            "ix_messages_external_id",
            "messages",
            ["external_id"],
            unique=False,
            if_not_exists=True,
        )

    op.create_index(
        "ix_attachments_message_id",
        "attachments",
        ["message_id"],
        unique=False,
        if_not_exists=True,
    )

    if is_pg:
        op.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_attachments_msg_hash
            ON attachments (message_id, hash_sha256)
            """
        )
    else:
        op.create_index(
            "uq_attachments_msg_hash",
            "attachments",
            ["message_id", "hash_sha256"],
            unique=True,
            if_not_exists=True,
        )


def downgrade():
    bind = op.get_bind()
    is_pg = bind.dialect.name == "postgresql"

    if is_pg:
        op.execute("DROP INDEX IF EXISTS uq_attachments_msg_hash")
    else:
        op.drop_index("uq_attachments_msg_hash", table_name="attachments")

    op.drop_index("ix_attachments_message_id", table_name="attachments")

    if is_pg:
        op.execute("DROP INDEX IF EXISTS uq_messages_external_id_gmail")
    else:
        op.drop_index("ix_messages_external_id", table_name="messages")

    with op.batch_alter_table("attachments") as batch:
        batch.drop_column("hash_sha256")

    with op.batch_alter_table("messages") as batch:
        batch.drop_column("source_meta")
