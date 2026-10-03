"""
Cache utilities - Cache-Aside pattern
"""
from typing import Optional, TypeVar, Callable, Any
import json
from app.core.redis_client import redis_client
import structlog

logger = structlog.get_logger()

T = TypeVar('T')


async def get_from_cache(key: str) -> Optional[str]:
    """Получение значения из кэша"""
    try:
        value = await redis_client.client.get(key)
        return value
    except Exception as e:
        logger.warning("Cache get failed", key=key, error=str(e))
        return None


async def set_to_cache(key: str, value: str, ttl: int = 3600):
    """Сохранение значения в кэш"""
    try:
        await redis_client.client.setex(key, ttl, value)
    except Exception as e:
        logger.warning("Cache set failed", key=key, error=str(e))


async def delete_from_cache(key: str):
    """Удаление значения из кэша"""
    try:
        await redis_client.client.delete(key)
    except Exception as e:
        logger.warning("Cache delete failed", key=key, error=str(e))


async def cache_aside(
    key: str,
    fetch_func: Callable[[], Any],
    ttl: int = 3600,
    serialize: Callable[[Any], str] = json.dumps,
    deserialize: Callable[[str], Any] = json.loads
) -> Any:
    """
    Cache-Aside паттерн
    
    Args:
        key: Ключ кэша
        fetch_func: Функция для получения данных из БД
        ttl: Время жизни кэша в секундах
        serialize: Функция сериализации
        deserialize: Функция десериализации
    """
    # Пытаемся получить из кэша
    cached = await get_from_cache(key)
    if cached is not None:
        try:
            return deserialize(cached)
        except Exception as e:
            logger.warning("Cache deserialize failed", key=key, error=str(e))
    
    # Получаем из БД
    data = await fetch_func()
    
    # Сохраняем в кэш
    if data is not None:
        try:
            serialized = serialize(data)
            await set_to_cache(key, serialized, ttl)
        except Exception as e:
            logger.warning("Cache serialize failed", key=key, error=str(e))
    
    return data


def generate_cache_key(prefix: str, **kwargs) -> str:
    """Генерация ключа кэша"""
    parts = [prefix]
    for k, v in sorted(kwargs.items()):
        parts.append(f"{k}:{v}")
    return ":".join(parts)












