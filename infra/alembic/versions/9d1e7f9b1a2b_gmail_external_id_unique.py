"""Add unique index for Gmail external_id"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9d1e7f9b1a2b"
down_revision: Union[str, None] = "530298151aeb"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Enforce idempotency for Gmail messages: one row per external_id per source.
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_messages_source_external_id
        ON messages (source, external_id)
        WHERE external_id IS NOT NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_messages_source_external_id")
