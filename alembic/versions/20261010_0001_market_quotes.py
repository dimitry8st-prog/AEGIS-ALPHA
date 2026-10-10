"""Add market quotes, revisions, quarantine and collection runs.

Revision ID: 20261010_0001
Revises:
Create Date: 2026-10-10

Applies on a fresh database (after init.sql) and on an existing Sprint-0
database without dropping users/items/audit_logs or volumes.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20261010_0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')

    op.create_table(
        "market_collection_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("provider", sa.String(64), nullable=False),
        sa.Column("symbols", postgresql.JSONB(), nullable=False),
        sa.Column("interval", sa.String(16), nullable=False),
        sa.Column("adjustment_mode", sa.String(32), nullable=False),
        sa.Column("window_start", sa.Date(), nullable=False),
        sa.Column("window_end", sa.Date(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="RUNNING"),
        sa.Column("params", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("finished_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("received_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("inserted_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("updated_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("duplicate_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("quarantined_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failed_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("report", postgresql.JSONB(), nullable=True),
        sa.Column("errors", postgresql.JSONB(), nullable=True),
    )
    op.create_index("ix_market_collection_runs_started_at", "market_collection_runs", ["started_at"])

    op.create_table(
        "market_quotes",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("provider", sa.String(64), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("interval", sa.String(16), nullable=False),
        sa.Column("quote_time", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("open", sa.Numeric(20, 8), nullable=False),
        sa.Column("high", sa.Numeric(20, 8), nullable=False),
        sa.Column("low", sa.Numeric(20, 8), nullable=False),
        sa.Column("close", sa.Numeric(20, 8), nullable=False),
        sa.Column("volume", sa.BigInteger(), nullable=False),
        sa.Column("adjustment_mode", sa.String(32), nullable=False),
        sa.Column("session_timezone", sa.String(64), nullable=False, server_default="America/New_York"),
        sa.Column("currency", sa.String(16), nullable=False, server_default="USD"),
        sa.Column("received_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("market_collection_runs.id"), nullable=True),
        sa.Column("payload_hash", sa.String(64), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.UniqueConstraint(
            "provider",
            "symbol",
            "exchange",
            "interval",
            "quote_time",
            "adjustment_mode",
            name="uq_market_quotes_natural_key",
        ),
    )
    op.create_index("ix_market_quotes_symbol_time", "market_quotes", ["symbol", "quote_time"])

    op.create_table(
        "market_quote_revisions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("quote_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("market_quotes.id", ondelete="CASCADE"), nullable=False),
        sa.Column("provider", sa.String(64), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("interval", sa.String(16), nullable=False),
        sa.Column("quote_time", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("adjustment_mode", sa.String(32), nullable=False),
        sa.Column("open", sa.Numeric(20, 8), nullable=False),
        sa.Column("high", sa.Numeric(20, 8), nullable=False),
        sa.Column("low", sa.Numeric(20, 8), nullable=False),
        sa.Column("close", sa.Numeric(20, 8), nullable=False),
        sa.Column("volume", sa.BigInteger(), nullable=False),
        sa.Column("previous_open", sa.Numeric(20, 8), nullable=False),
        sa.Column("previous_high", sa.Numeric(20, 8), nullable=False),
        sa.Column("previous_low", sa.Numeric(20, 8), nullable=False),
        sa.Column("previous_close", sa.Numeric(20, 8), nullable=False),
        sa.Column("previous_volume", sa.BigInteger(), nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("changed_at", sa.TIMESTAMP(timezone=True), nullable=False),
    )
    op.create_index("ix_market_quote_revisions_quote_id", "market_quote_revisions", ["quote_id"])

    op.create_table(
        "market_quote_quarantine",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("provider", sa.String(64), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("interval", sa.String(16), nullable=False),
        sa.Column("quote_time", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("adjustment_mode", sa.String(32), nullable=True),
        sa.Column("reason_code", sa.String(64), nullable=False),
        sa.Column("reason_detail", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("market_collection_runs.id"), nullable=True),
        sa.Column("received_at", sa.TIMESTAMP(timezone=True), nullable=False),
    )
    op.create_index("ix_market_quote_quarantine_run_id", "market_quote_quarantine", ["run_id"])


def downgrade() -> None:
    op.drop_table("market_quote_quarantine")
    op.drop_table("market_quote_revisions")
    op.drop_table("market_quotes")
    op.drop_table("market_collection_runs")
