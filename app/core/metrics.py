"""
Prometheus metrics
"""
from fastapi import APIRouter, Request, Response
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp
import time

# Метрики
http_requests_total = Counter(
    'http_requests_total',
    'Total HTTP requests',
    ['method', 'endpoint', 'status_code']
)

http_request_duration_seconds = Histogram(
    'http_request_duration_seconds',
    'HTTP request duration in seconds',
    ['method', 'endpoint']
)

http_errors_total = Counter(
    'http_errors_total',
    'Total HTTP errors',
    ['status_code', 'error_type']
)

active_connections = Gauge(
    'active_connections',
    'Number of active connections'
)

# Router для /metrics endpoint
metrics_router = APIRouter()


@metrics_router.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )


class MetricsMiddleware(BaseHTTPMiddleware):
    """Middleware для сбора метрик"""
    
    async def dispatch(self, request: Request, call_next):
        # Пропускаем /metrics endpoint
        if request.url.path == "/metrics":
            return await call_next(request)
        
        active_connections.inc()
        start_time = time.time()
        
        try:
            response = await call_next(request)
            
            # Записываем метрики
            duration = time.time() - start_time
            method = request.method
            endpoint = request.url.path
            status_code = response.status_code
            
            http_requests_total.labels(
                method=method,
                endpoint=endpoint,
                status_code=status_code
            ).inc()
            
            http_request_duration_seconds.labels(
                method=method,
                endpoint=endpoint
            ).observe(duration)
            
            # Ошибки (4xx, 5xx)
            if status_code >= 400:
                http_errors_total.labels(
                    status_code=status_code,
                    error_type=f"{status_code // 100}xx"
                ).inc()
            
            return response
            
        finally:
            active_connections.dec()












