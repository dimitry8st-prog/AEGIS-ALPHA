"""
Rate Limiting Middleware - Sliding Window
"""
from fastapi import Request, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp
import time
import structlog
from app.core.redis_client import redis_client
from app.core.config import settings

logger = structlog.get_logger()


class RateLimiterMiddleware(BaseHTTPMiddleware):
    """
    Rate limiting на основе sliding window через Redis
    """
    
    def __init__(self, app: ASGIApp):
        super().__init__(app)
        self.requests_limit = settings.RATE_LIMIT_REQUESTS
        self.window_seconds = settings.RATE_LIMIT_WINDOW_SECONDS
    
    async def dispatch(self, request: Request, call_next):
        # Пропускаем health check и metrics
        if request.url.path in ["/health", "/metrics"]:
            return await call_next(request)
        
        # Получаем идентификатор клиента (IP или API ключ)
        client_id = self._get_client_id(request)
        
        # Проверяем rate limit
        is_allowed = await self._check_rate_limit(client_id)
        
        if not is_allowed:
            logger.warning(
                "Rate limit exceeded",
                client_id=client_id,
                path=request.url.path,
                limit=self.requests_limit,
                window=self.window_seconds,
            )
            
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={
                    "error": {
                        "code": "RATE_LIMIT_EXCEEDED",
                        "message": f"Rate limit exceeded: {self.requests_limit} requests per {self.window_seconds} seconds",
                        "retry_after": self.window_seconds
                    }
                },
                headers={"Retry-After": str(self.window_seconds)}
            )
        
        # Добавляем заголовки с информацией о лимитах
        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(self.requests_limit)
        response.headers["X-RateLimit-Window"] = str(self.window_seconds)
        
        return response
    
    def _get_client_id(self, request: Request) -> str:
        """
        Получение идентификатора клиента.
        Приоритет: API ключ из заголовка > IP адрес
        """
        # Проверяем API ключ в заголовке
        api_key = request.headers.get("X-API-Key")
        if api_key:
            return f"api_key:{api_key}"
        
        # Используем IP адрес
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            ip = forwarded_for.split(",")[0].strip()
        else:
            ip = request.client.host if request.client else "unknown"
        
        return f"ip:{ip}"
    
    async def _check_rate_limit(self, client_id: str) -> bool:
        """
        Проверка rate limit используя sliding window в Redis
        """
        try:
            redis = redis_client.client
            now = time.time()
            window_start = now - self.window_seconds
            
            # Ключ для хранения запросов клиента
            key = f"rate_limit:{client_id}"
            
            # Удаляем старые записи (за пределами окна)
            await redis.zremrangebyscore(key, 0, window_start)
            
            # Подсчитываем количество запросов в окне
            count = await redis.zcard(key)
            
            if count >= self.requests_limit:
                return False
            
            # Добавляем текущий запрос
            await redis.zadd(key, {str(now): now})
            await redis.expire(key, self.window_seconds)
            
            return True
            
        except Exception as e:
            # В случае ошибки Redis разрешаем запрос (fail-open)
            logger.error("Rate limit check failed", error=str(e), exc_info=True)
            return True












