"""
AEGIS Backend Core - Production-Ready API Service
"""
import asyncio
from contextlib import asynccontextmanager
from html import escape
from fastapi import FastAPI, Depends
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import structlog

from app.core.config import settings
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import engine, Base, get_db
from app.core.redis_client import redis_client
from app.core.metrics import metrics_router, MetricsMiddleware
from app.core.error_handler import setup_exception_handlers
from app.core.rate_limiter import RateLimiterMiddleware
from app.core.audit import AuditMiddleware
from app.core.logging_config import setup_logging
from app.api.v1.router import api_router
from app.core.dependencies import get_current_active_user

# Настройка логирования
setup_logging(settings.LOG_LEVEL)
logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    # Startup
    logger.info("Starting AEGIS Backend", instance_id=settings.INSTANCE_ID)
    
    # Создание таблиц
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    # Подключение к Redis
    await redis_client.connect()
    logger.info("Redis connected")
    
    yield
    
    # Shutdown
    await redis_client.disconnect()
    logger.info("Shutting down AEGIS Backend")


app = FastAPI(
    title="AEGIS Backend API",
    description="Production-ready backend core for SaaS platform",
    version="1.0.0",
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В продакшене указать конкретные домены
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Middleware (порядок важен!)
app.add_middleware(MetricsMiddleware)  # Первым для сбора всех метрик
app.add_middleware(RateLimiterMiddleware)
app.add_middleware(AuditMiddleware)

# Exception handlers
setup_exception_handlers(app)

# Routers
app.include_router(metrics_router, tags=["monitoring"])
app.include_router(api_router, prefix="/api/v1")


DEPENDENCY_TIMEOUT_SECONDS = 2.0
RESULTS_ITEM_LIMIT = 100


@app.get("/health")
async def health_check():
    """Liveness: the process answers. Dependencies are not checked."""
    return {
        "status": "healthy",
        "instance_id": settings.INSTANCE_ID,
        "role": "liveness",
    }


@app.get("/ready")
async def readiness_check():
    """Readiness of the API process.

    PostgreSQL and Redis are required for the API. The worker's unused quote
    cache is not part of this check. Failures return 503 and name the
    dependency only.
    """
    unavailable: list[str] = []
    try:
        async def ping_db():
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))

        await asyncio.wait_for(ping_db(), timeout=DEPENDENCY_TIMEOUT_SECONDS)
    except Exception:
        unavailable.append("postgres")
    try:
        if redis_client._client is None:
            raise RuntimeError("redis disconnected")
        await asyncio.wait_for(
            redis_client.client.ping(),
            timeout=DEPENDENCY_TIMEOUT_SECONDS,
        )
    except Exception:
        unavailable.append("redis")
    if unavailable:
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready", "dependencies": unavailable},
        )
    return {
        "status": "ready",
        "instance_id": settings.INSTANCE_ID,
        "role": "readiness",
    }


@app.get("/results", response_class=HTMLResponse)
async def results_page(
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """
    HTML-страница списка items. Это не просмотр рыночных котировок.
    Нужен заголовок Authorization: Bearer. Выборка не больше RESULTS_ITEM_LIMIT.
    """
    from app.models.item import Item  # локальный импорт, чтобы избежать циклов

    result = await db.execute(
        select(Item).order_by(Item.created_at.desc()).limit(RESULTS_ITEM_LIMIT)
    )
    items = result.scalars().all()

    rows = ""
    for item in items:
        created = "" if item.created_at is None else str(item.created_at)
        rows += f"""
        <tr>
            <td>{escape(str(item.id))}</td>
            <td>{escape(item.title or "")}</td>
            <td>{escape((item.description or "")[:200])}</td>
            <td>{escape(created)}</td>
        </tr>
        """

    # Формируем содержимое main в зависимости от наличия данных
    if not items:
        main_content = "<div class='empty'>Нет данных. Создайте item через API.</div>"
    else:
        main_content = f"""
            <table>
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Заголовок</th>
                        <th>Описание</th>
                        <th>Создано</th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
            """

    html = f"""
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8" />
        <title>AEGIS – Items</title>
        <style>
            body {{
                font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
                margin: 0;
                padding: 0;
                background: #0f172a;
                color: #e5e7eb;
            }}
            header {{
                padding: 16px 32px;
                background: #020617;
                border-bottom: 1px solid #1f2937;
                display: flex;
                justify-content: space-between;
                align-items: center;
            }}
            header h1 {{
                margin: 0;
                font-size: 20px;
            }}
            header span {{
                font-size: 12px;
                color: #9ca3af;
            }}
            main {{
                padding: 24px 32px 40px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 16px;
                background: #020617;
                border-radius: 8px;
                overflow: hidden;
            }}
            thead {{
                background: #111827;
            }}
            th, td {{
                padding: 10px 12px;
                font-size: 13px;
                border-bottom: 1px solid #1f2937;
                text-align: left;
            }}
            th {{
                font-weight: 600;
                color: #9ca3af;
                text-transform: uppercase;
                letter-spacing: .05em;
                font-size: 11px;
            }}
            tr:nth-child(even) td {{
                background: #020617;
            }}
            tr:hover td {{
                background: #111827;
            }}
            .empty {{
                padding: 32px;
                text-align: center;
                color: #6b7280;
            }}
        </style>
    </head>
    <body>
        <header>
            <h1>AEGIS – Items</h1>
            <span>Показано items: {len(items)} из не более {RESULTS_ITEM_LIMIT}. Это не котировки. {escape(current_user.email or "")}</span>
        </header>
        <main>
            {main_content}
        </main>
    </body>
    </html>
    """
    return HTMLResponse(content=html)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

