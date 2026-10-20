"""Record the previous quote time and move legacy early closes to 13:00.

Revision ID: 20261020_0002
Revises: 20261010_0001
Create Date: 2026-10-20

Does not drop a 16:00 row unless a 13:00 row for the same session already
exists. In that case the 16:00 values are copied into market_quote_revisions
first, in the same transaction.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "20261020_0002"
down_revision: Union[str, None] = "20261010_0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "market_quote_revisions",
        sa.Column("previous_quote_time", sa.TIMESTAMP(timezone=True), nullable=True),
    )
    from worker.clock_migration import migrate_legacy_early_closes

    migrate_legacy_early_closes(op.get_bind())


def downgrade() -> None:
    # Put a moved row back on its recorded 16:00 stamp when that key is free.
    # Rows that were merged stay represented by the revision until this column
    # is dropped; they are not deleted here.
    op.execute(
        """
        UPDATE market_quotes AS quote
        SET quote_time = revision.previous_quote_time
        FROM market_quote_revisions AS revision
        WHERE revision.quote_id = quote.id
          AND revision.previous_quote_time IS NOT NULL
          AND revision.quote_time = quote.quote_time
          AND revision.previous_quote_time <> quote.quote_time
          AND NOT EXISTS (
              SELECT 1
              FROM market_quotes AS other
              WHERE other.provider = quote.provider
                AND other.symbol = quote.symbol
                AND other.exchange = quote.exchange
                AND other.interval = quote.interval
                AND other.adjustment_mode = quote.adjustment_mode
                AND other.quote_time = revision.previous_quote_time
                AND other.id <> quote.id
          )
        """
    )
    op.drop_column("market_quote_revisions", "previous_quote_time")
