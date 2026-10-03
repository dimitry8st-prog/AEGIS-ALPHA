"""
Audit Middleware - автоматическая запись изменений в audit_logs
"""
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp
import json
import structlog
from app.core.config import settings
from app.core.database import AsyncSessionLocal
from sqlalchemy import text

logger = structlog.get_logger()


class AuditMiddleware(BaseHTTPMiddleware):
    """
    Middleware для автоматической записи изменений в audit_logs
    Перехватывает запросы и логирует изменения после их выполнения
    """
    
    async def dispatch(self, request: Request, call_next):
        # Пропускаем GET запросы и служебные эндпоинты
        if request.method == "GET" or request.url.path in ["/health", "/metrics", "/docs", "/openapi.json"]:
            return await call_next(request)
        
        # Выполняем запрос
        response = await call_next(request)
        
        # Логируем изменения для успешных операций (2xx)
        # Примечание: body запроса логируется как None, так как FastAPI уже обработал его
        # В реальной реализации можно использовать request.state для сохранения body до обработки
        if 200 <= response.status_code < 300:
            await self._log_audit(
                request=request,
                response_status=response.status_code,
                body=None  # В production можно добавить логирование body через request.state
            )
        
        return response
    
    async def _log_audit(
        self,
        request: Request,
        response_status: int,
        body: dict = None
    ):
        """Запись в audit_logs"""
        try:
            # Определяем действие
            action_map = {
                "POST": "CREATE",
                "PUT": "UPDATE",
                "PATCH": "UPDATE",
                "DELETE": "DELETE"
            }
            action = action_map.get(request.method)
            if not action:
                return
            
            # Определяем тип ресурса из пути
            path_parts = [p for p in request.url.path.strip("/").split("/") if p]
            if len(path_parts) >= 2:
                # Ищем последний сегмент перед ID (например, /api/v1/users/123 -> users)
                resource_type = path_parts[-2] if len(path_parts) > 2 else path_parts[-1]
                # Последний сегмент может быть ID (UUID)
                try:
                    from uuid import UUID
                    resource_id = str(UUID(path_parts[-1])) if path_parts[-1] else None
                except:
                    resource_id = None
            else:
                return
            
            # Получаем user_id из токена (если есть)
            user_id = None
            authorization = request.headers.get("Authorization")
            if authorization and authorization.startswith("Bearer "):
                try:
                    from app.core.security import decode_access_token
                    token = authorization.replace("Bearer ", "")
                    token_data = decode_access_token(token)
                    if token_data and token_data.user_id:
                        user_id = token_data.user_id
                except:
                    pass
            
            # Получаем IP и User-Agent
            ip_address = request.headers.get("X-Forwarded-For", request.client.host if request.client else None)
            user_agent = request.headers.get("User-Agent")
            
            # Подготавливаем данные
            data_after = json.dumps(body) if body else None
            
            # Записываем в БД
            async with AsyncSessionLocal() as session:
                await session.execute(
                    text("""
                        INSERT INTO audit_logs 
                        (user_id, action, resource_type, resource_id, data_before, data_after, ip_address, user_agent, instance_id)
                        VALUES 
                        (:user_id, :action, :resource_type, :resource_id, :data_before, :data_after, :ip_address, :user_agent, :instance_id)
                    """),
                    {
                        "user_id": str(user_id) if user_id else None,
                        "action": action,
                        "resource_type": resource_type,
                        "resource_id": resource_id,
                        "data_before": None,  # В реальной реализации нужно получать из БД перед изменением
                        "data_after": data_after,
                        "ip_address": ip_address,
                        "user_agent": user_agent,
                        "instance_id": settings.INSTANCE_ID
                    }
                )
                await session.commit()
                
        except Exception as e:
            # Не прерываем выполнение запроса при ошибке аудита
            logger.error("Audit logging failed", error=str(e), exc_info=True)

