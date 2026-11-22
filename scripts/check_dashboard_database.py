"""Check the local dashboard database without writing or logging credentials."""

from __future__ import annotations

import asyncio
import os

import asyncpg


async def database_status() -> str:
    dsn = os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://", 1)
    try:
        connection = await asyncpg.connect(dsn, timeout=5)
        try:
            await connection.fetchval("SELECT 1", timeout=5)
        finally:
            await connection.close(timeout=5)
    except asyncpg.PostgresError as error:
        return error.sqlstate or "POSTGRES_ERROR"
    except TimeoutError:
        return "TIMEOUT"
    except OSError:
        return "UNREACHABLE"
    except (ValueError, asyncpg.InterfaceError):
        return "INVALID_CONNECTION_SETTINGS"
    return "READY"


if __name__ == "__main__":
    print(asyncio.run(database_status()))
