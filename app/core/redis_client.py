"""
Redis client для кэширования и rate limiting
"""
from typing import Optional
import redis.asyncio as aioredis
from app.core.config import settings
import structlog

logger = structlog.get_logger()


class RedisClient:
    """Singleton Redis client"""
    _instance: Optional['RedisClient'] = None
    _client: Optional[aioredis.Redis] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    async def connect(self):
        """Подключение к Redis"""
        if self._client is None:
            self._client = aioredis.from_url(
                settings.REDIS_URL,
                encoding="utf-8",
                decode_responses=True,
                max_connections=50
            )
            logger.info("Redis client initialized")
    
    async def disconnect(self):
        """Отключение от Redis"""
        if self._client:
            await self._client.close()
            self._client = None
            logger.info("Redis client disconnected")
    
    @property
    def client(self) -> aioredis.Redis:
        """Получение Redis client"""
        if self._client is None:
            raise RuntimeError("Redis client not connected. Call connect() first.")
        return self._client


redis_client = RedisClient()

