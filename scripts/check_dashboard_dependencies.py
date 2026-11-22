"""Check local dashboard dependencies without writing or logging credentials."""

from __future__ import annotations

import asyncio
import os
import sys

import asyncpg
from redis import Redis
from redis.exceptions import RedisError


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


def redis_status() -> str:
    try:
        with Redis.from_url(os.environ["REDIS_URL"], socket_connect_timeout=3, socket_timeout=3) as redis:
            if redis.ping():
                return "READY"
    except (OSError, RedisError):
        pass
    return "UNREACHABLE"


if __name__ == "__main__":
    print(redis_status() if sys.argv[1:] == ["redis"] else asyncio.run(database_status()))
