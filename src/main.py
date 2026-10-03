import asyncio
import signal
import sys
from contextlib import asynccontextmanager
from loguru import logger
import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI, Depends
from prometheus_client import make_asgi_app

from config.settings import settings
from compliance.audit_logger import AuditLogger
from compliance.data_policy import DataPolicyManager
from data_pipeline.telegram_collector import TelegramCollector
from data_pipeline.market_data import MarketDataCollector
from data_pipeline.file_processor import FileProcessor

# Настройка логирования
logger.add(
    f"{settings.logs_dir}/app.log",
    rotation="500 MB",
    retention=f"{settings.data_retention_days} days",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {module}:{function}:{line} | {message}"
)


class AegisAlpha:
    """Основной класс приложения"""

    def __init__(self):
        self.app = FastAPI(title=settings.app_name)
        self.db_pool = None
        self.redis_client = None
        self.audit_logger = None
        self.data_policy = None
        self.telegram_collector = None
        self.market_collector = None
        self.file_processor = None

        self._setup_lifespan()
        self._setup_routes()

    def _setup_lifespan(self):
        """Настройка жизненного цикла приложения"""

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            # Startup
            await self.startup()
            yield
            # Shutdown
            await self.shutdown()

        self.app.router.lifespan_context = lifespan

    def _setup_routes(self):
        """Настройка маршрутов API"""

        @self.app.get("/")
        async def root():
            return {
                "app": settings.app_name,
                "version": "0.1.0",
                "status": "running",
                "environment": settings.environment
            }

        @self.app.get("/health")
        async def health_check():
            return {
                "database": "connected" if self.db_pool else "disconnected",
                "redis": "connected" if self.redis_client else "disconnected",
                "status": "healthy"
            }

        @self.app.get("/metrics")
        async def metrics():
            return make_asgi_app()(self.app)

    async def startup(self):
        """Запуск приложения"""
        logger.info(f"Starting {settings.app_name} in {settings.environment} environment")

        try:
            # Подключение к базам данных
            await self._connect_databases()

            # Инициализация compliance систем
            self.audit_logger = AuditLogger(self.db_pool)
            self.data_policy = DataPolicyManager()

            # Инициализация сборщиков данных
            self.market_collector = MarketDataCollector(self.db_pool, self.redis_client)
            self.file_processor = FileProcessor(self.db_pool)

            # Telegram collector требует отдельной аутентификации
            if all([settings.telegram_api_id, settings.telegram_api_hash, settings.telegram_phone]):
                self.telegram_collector = TelegramCollector(self.db_pool, self.audit_logger)

            # Создание таблиц
            await self._create_tables()

            # Запуск фоновых задач
            await self._start_background_tasks()

            logger.info("Application started successfully")

        except Exception as e:
            logger.error(f"Failed to start application: {e}")
            await self.shutdown()
            sys.exit(1)

    async def shutdown(self):
        """Остановка приложения"""
        logger.info("Shutting down application...")

        tasks = []
        if self.telegram_collector and self.telegram_collector.client:
            tasks.append(self.telegram_collector.client.disconnect())

        if self.db_pool:
            tasks.append(self.db_pool.close())

        if self.redis_client:
            tasks.append(self.redis_client.close())

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        logger.info("Application shutdown complete")

    async def _connect_databases(self):
        """Подключение к базам данных"""
        logger.info("Connecting to databases...")

        # PostgreSQL
        self.db_pool = await asyncpg.create_pool(
            dsn=str(settings.database_url),
            min_size=5,
            max_size=20,
            command_timeout=60
        )

        # Redis
        self.redis_client = redis.from_url(
            str(settings.redis_url),
            encoding="utf-8",
            decode_responses=True
        )

        # Проверка подключений
        async with self.db_pool.acquire() as conn:
            await conn.execute("SELECT 1")

        await self.redis_client.ping()

        logger.info("Databases connected successfully")

    async def _create_tables(self):
        """Создание таблиц базы данных"""
        logger.info("Creating database tables...")

        create_tables_sql = """
        -- Таблица для логов аудита
        CREATE TABLE IF NOT EXISTS audit_logs (
            id VARCHAR(32) PRIMARY KEY,
            user_id VARCHAR(100) NOT NULL,
            action VARCHAR(100) NOT NULL,
            resource_type VARCHAR(50) NOT NULL,
            resource_id VARCHAR(100) NOT NULL,
            details JSONB,
            ip_address VARCHAR(45),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        -- Индексы для аудита
        CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_logs(user_id);
        CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_logs(action);
        CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_logs(created_at);

        -- Таблица для Telegram сообщений
        CREATE TABLE IF NOT EXISTS telegram_messages (
            id SERIAL PRIMARY KEY,
            message_id BIGINT NOT NULL,
            chat_id BIGINT NOT NULL,
            chat_title VARCHAR(200),
            text TEXT,
            date TIMESTAMP WITH TIME ZONE,
            views INTEGER DEFAULT 0,
            forwards INTEGER DEFAULT 0,
            has_media BOOLEAN DEFAULT FALSE,
            links TEXT[],
            mentions TEXT[],
            hashtags TEXT[],
            collected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE(message_id, chat_id)
        );

        -- Таблица для упоминаний тикеров в Telegram
        CREATE TABLE IF NOT EXISTS telegram_ticker_mentions (
            id SERIAL PRIMARY KEY,
            message_id BIGINT NOT NULL,
            chat_id BIGINT NOT NULL,
            ticker VARCHAR(20) NOT NULL,
            mentioned_at TIMESTAMP WITH TIME ZONE,
            chat_title VARCHAR(200),
            message_text VARCHAR(500),
            collected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        -- Индексы для Telegram
        CREATE INDEX IF NOT EXISTS idx_telegram_ticker ON telegram_ticker_mentions(ticker);
        CREATE INDEX IF NOT EXISTS idx_telegram_date ON telegram_messages(date);

        -- Таблица для рыночных данных
        CREATE TABLE IF NOT EXISTS market_data (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(20) NOT NULL,
            company_name VARCHAR(200),
            sector VARCHAR(100),
            industry VARCHAR(100),
            current_price DECIMAL(12, 4),
            market_cap BIGINT,
            volume BIGINT,
            indicators JSONB,
            collected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE(ticker, collected_at)
        );

        -- Индексы для рыночных данных
        CREATE INDEX IF NOT EXISTS idx_market_ticker ON market_data(ticker);
        CREATE INDEX IF NOT EXISTS idx_market_collected ON market_data(collected_at);

        -- Таблица для обработанных файлов
        CREATE TABLE IF NOT EXISTS processed_files (
            id SERIAL PRIMARY KEY,
            file_path VARCHAR(500) NOT NULL,
            file_name VARCHAR(200) NOT NULL,
            file_size BIGINT,
            content_type VARCHAR(50),
            content_summary JSONB,
            processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            metadata JSONB,
            content_preview TEXT
        );

        -- Индексы для файлов
        CREATE INDEX IF NOT EXISTS idx_files_processed ON processed_files(processed_at);
        CREATE INDEX IF NOT EXISTS idx_files_path ON processed_files(file_path);

        -- Таблица пользователей (заглушка для compliance)
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(100) UNIQUE NOT NULL,
            personal_data JSONB,
            last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(create_tables_sql)
                logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise

    async def _start_background_tasks(self):
        """Запуск фоновых задач"""
        logger.info("Starting background tasks...")

        # Задача для сбора рыночных данных каждые 5 минут
        asyncio.create_task(self.market_collector.run(interval_minutes=5))

        # Задача для обработки файлов
        watch_dir = os.path.join(settings.data_dir, "incoming")
        os.makedirs(watch_dir, exist_ok=True)
        asyncio.create_task(self.file_processor.run(watch_dir))

        # Задача для Telegram (если настроен)
        if self.telegram_collector:
            asyncio.create_task(self.telegram_collector.run())

        # Задача для очистки старых данных (раз в день)
        asyncio.create_task(self._daily_cleanup())

        logger.info("Background tasks started")

    async def _daily_cleanup(self):
        """Ежедневная очистка старых данных"""
        while True:
            try:
                # Ожидание до 3:00 утра
                now = datetime.now()
                next_run = now.replace(hour=3, minute=0, second=0, microsecond=0)
                if now.hour >= 3:
                    next_run += timedelta(days=1)

                wait_seconds = (next_run - now).total_seconds()
                logger.info(f"Next cleanup scheduled at {next_run} (in {wait_seconds:.0f} seconds)")
                await asyncio.sleep(wait_seconds)

                # Выполнение очистки
                logger.info("Starting daily cleanup...")

                # Очистка логов аудита
                if self.audit_logger:
                    await self.audit_logger.cleanup_old_logs()

                # Применение политик хранения
                if self.data_policy:
                    await self.data_policy.apply_retention_policy(self.db_pool)

                logger.info("Daily cleanup completed")

            except Exception as e:
                logger.error(f"Error in daily cleanup: {e}")
                await asyncio.sleep(3600)  # Пауза на час при ошибке


async def main():
    """Точка входа в приложение"""
    # Обработка сигналов завершения
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    # Создание и запуск приложения
    aegis = AegisAlpha()

    try:
        # Запуск сервера
        import uvicorn
        config = uvicorn.Config(
            app=aegis.app,
            host="0.0.0.0",
            port=8000,
            log_level="info"
        )
        server = uvicorn.Server(config)

        # Запуск сервера и ожидание сигнала остановки
        server_task = asyncio.create_task(server.serve())
        await stop_event.wait()

        # Остановка сервера
        server.should_exit = True
        await server_task

    except Exception as e:
        logger.error(f"Application error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())