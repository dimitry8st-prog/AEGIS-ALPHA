"""Store every observed Finam news version once.

Revision ID: 20261127_0003
Revises: 20261020_0002
Create Date: 2026-11-27

(source, news_id, version) is unique. observed_at is the first time AEGIS
received that version and is never updated by later reads.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20261127_0003"
down_revision: Union[str, None] = "20261020_0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "market_news",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("provider", sa.String(16), nullable=False, server_default="finam"),
        sa.Column("source", sa.String(64), nullable=False),
        sa.Column("news_id", sa.String(128), nullable=False),
        sa.Column("version", sa.String(64), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=True),
        sa.Column("symbols", postgresql.ARRAY(sa.String(16)), nullable=False),
        sa.Column("published_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("provider_received_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("observed_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("deleted", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("source_version", sa.String(64), nullable=True),
        sa.Column("language", sa.String(16), nullable=True),
        sa.UniqueConstraint("provider", "source", "news_id", "version", name="uq_market_news_version"),
    )
    op.create_index("ix_market_news_published_at", "market_news", ["published_at"])
    op.create_index("ix_market_news_symbols", "market_news", ["symbols"], postgresql_using="gin")


def downgrade() -> None:
    op.drop_index("ix_market_news_symbols", table_name="market_news")
    op.drop_index("ix_market_news_published_at", table_name="market_news")
    op.drop_table("market_news")
