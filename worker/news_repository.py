"""Append-only storage of Finam news versions."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from worker.finam_news import NewsItem


class NewsRepository:
    def __init__(self, engine: AsyncEngine):
        self.engine = engine

    async def store(self, items: list[NewsItem]) -> dict:
        """Insert unseen versions; a repeated version keeps its first observed_at."""
        inserted = 0
        async with self.engine.begin() as conn:
            for item in items:
                result = await conn.execute(
                    text(
                        """
                        INSERT INTO market_news (
                            source, news_id, version, title, url, symbols, published_at,
                            updated_at, provider_received_at, observed_at, deleted,
                            source_version, language
                        ) VALUES (
                            :source, :news_id, :version, :title, :url, :symbols, :published_at,
                            :updated_at, :provider_received_at, :observed_at, :deleted,
                            :source_version, :language
                        )
                        ON CONFLICT ON CONSTRAINT uq_market_news_version DO NOTHING
                        """
                    ),
                    {
                        "source": item.source, "news_id": item.news_id, "version": item.version,
                        "title": item.title, "url": item.url, "symbols": list(item.symbols),
                        "published_at": item.published_at, "updated_at": item.updated_at,
                        "provider_received_at": item.provider_received_at, "observed_at": item.observed_at,
                        "deleted": item.deleted, "source_version": item.source_version, "language": item.language,
                    },
                )
                inserted += result.rowcount
        return {"inserted": inserted, "already_stored": len(items) - inserted}

    async def first_observed(self, items: list[NewsItem]) -> dict[tuple[str, str, str], object]:
        """Stored observed_at for each (source, news_id, version) in `items`."""
        if not items:
            return {}
        async with self.engine.connect() as conn:
            rows = (await conn.execute(
                text(
                    """
                    SELECT source, news_id, version, observed_at FROM market_news
                    WHERE provider = 'finam'
                      AND (source, news_id, version) IN (
                          SELECT * FROM unnest(CAST(:sources AS text[]), CAST(:ids AS text[]), CAST(:versions AS text[]))
                      )
                    """
                ),
                {
                    "sources": [item.source for item in items],
                    "ids": [item.news_id for item in items],
                    "versions": [item.version for item in items],
                },
            )).all()
        return {(row.source, row.news_id, row.version): row.observed_at for row in rows}
