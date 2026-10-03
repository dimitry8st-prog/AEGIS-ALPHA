"""
Централизованная обработка ошибок
"""
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from sqlalchemy.exc import SQLAlchemyError
import structlog
import traceback
import uuid

logger = structlog.get_logger()


class AppException(Exception):
    """Базовый класс для пользовательских исключений"""
    def __init__(self, code: str, message: str, status_code: int = 400):
        self.code = code
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)


class NotFoundError(AppException):
    """Ресурс не найден"""
    def __init__(self, resource: str, resource_id: str = None):
        message = f"{resource} not found"
        if resource_id:
            message += f": {resource_id}"
        super().__init__(
            code=f"{resource.upper()}_NOT_FOUND",
            message=message,
            status_code=404
        )


class UnauthorizedError(AppException):
    """Ошибка аутентификации"""
    def __init__(self, message: str = "Unauthorized"):
        super().__init__(
            code="UNAUTHORIZED",
            message=message,
            status_code=401
        )


class ForbiddenError(AppException):
    """Ошибка авторизации"""
    def __init__(self, message: str = "Forbidden"):
        super().__init__(
            code="FORBIDDEN",
            message=message,
            status_code=403
        )


async def app_exception_handler(request: Request, exc: AppException):
    """Обработчик пользовательских исключений"""
    trace_id = str(uuid.uuid4())
    
    logger.error(
        "Application error",
        trace_id=trace_id,
        error_code=exc.code,
        error_message=exc.message,
        path=request.url.path,
        method=request.method,
    )
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": exc.code,
                "message": exc.message,
                "trace_id": trace_id
            }
        }
    )


async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Обработчик ошибок валидации"""
    trace_id = str(uuid.uuid4())
    
    logger.warning(
        "Validation error",
        trace_id=trace_id,
        errors=exc.errors(),
        path=request.url.path,
    )
    
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "Validation failed",
                "details": exc.errors(),
                "trace_id": trace_id
            }
        }
    )


async def database_exception_handler(request: Request, exc: SQLAlchemyError):
    """Обработчик ошибок БД"""
    trace_id = str(uuid.uuid4())
    
    logger.error(
        "Database error",
        trace_id=trace_id,
        error=str(exc),
        path=request.url.path,
        exc_info=True,
    )
    
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": {
                "code": "DATABASE_ERROR",
                "message": "Database operation failed",
                "trace_id": trace_id
            }
        }
    )


async def general_exception_handler(request: Request, exc: Exception):
    """Обработчик всех остальных исключений"""
    trace_id = str(uuid.uuid4())
    
    logger.error(
        "Unhandled exception",
        trace_id=trace_id,
        error=str(exc),
        error_type=type(exc).__name__,
        path=request.url.path,
        method=request.method,
        exc_info=True,
        traceback=traceback.format_exc(),
    )
    
    # В продакшене не возвращаем детали ошибки
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": {
                "code": "INTERNAL_SERVER_ERROR",
                "message": "An unexpected error occurred",
                "trace_id": trace_id
            }
        }
    )


def setup_exception_handlers(app: FastAPI):
    """Настройка обработчиков исключений"""
    app.add_exception_handler(AppException, app_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(SQLAlchemyError, database_exception_handler)
    app.add_exception_handler(Exception, general_exception_handler)












