import asyncio
from datetime import datetime
from typing import List, Dict, Optional
from telethon import TelegramClient, events
from telethon.tl.types import Message, Channel
from loguru import logger
import asyncpg
from ..config.settings import settings
from ..compliance.audit_logger import AuditLogger


class TelegramCollector:
    """Сборщик данных из Telegram каналов"""

    def __init__(self, db_pool: asyncpg.Pool, audit_logger: AuditLogger):
        self.db_pool = db_pool
        self.audit_logger = audit_logger
        self.client = None
        self.channels = settings.telegram_channels
        self.setup_telegram_client()

    def setup_telegram_client(self):
        """Настройка Telegram клиента"""
        self.client = TelegramClient(
            'aegis_session',
            int(settings.telegram_api_id),
            settings.telegram_api_hash
        )

        # Настройка обработчиков событий
        @self.client.on(events.NewMessage(chats=self.channels))
        async def handler(event):
            await self.process_message(event.message)

    async def connect(self):
        """Подключение к Telegram"""
        try:
            await self.client.start(phone=settings.telegram_phone)
            logger.info("Telegram client connected successfully")

            # Логирование подключения
            await self.audit_logger.log_action(
                user_id="system",
                action="telegram_connect",
                resource_type="telegram_client",
                resource_id="telegram",
                details={"channels": self.channels}
            )

        except Exception as e:
            logger.error(f"Failed to connect to Telegram: {e}")
            raise

    async def process_message(self, message: Message):
        """Обработка сообщения из Telegram"""
        try:
            # Rate limiting
            await self._check_rate_limit(message.chat_id)

            # Извлечение данных
            message_data = {
                'message_id': message.id,
                'chat_id': message.chat_id,
                'chat_title': getattr(message.chat, 'title', 'Unknown'),
                'text': message.text or '',
                'date': message.date,
                'views': message.views or 0,
                'forwards': message.forwards or 0,
                'has_media': bool(message.media),
                'links': self._extract_links(message.text or ''),
                'mentions': self._extract_mentions(message.text or ''),
                'hashtags': self._extract_hashtags(message.text or ''),
                'collected_at': datetime.utcnow()
            }

            # Сохранение в базу
            await self.save_message(message_data)

            # Анализ на финансовые тикеры
            tickers = self._extract_tickers(message_data['text'])
            if tickers:
                await self.process_tickers(message_data, tickers)

            logger.debug(f"Processed message {message.id} from {message_data['chat_title']}")

        except Exception as e:
            logger.error(f"Error processing message {message.id}: {e}")

    async def _check_rate_limit(self, chat_id: int):
        """Проверка rate limit для чата"""
        query = """
        SELECT COUNT(*) as msg_count 
        FROM telegram_messages 
        WHERE chat_id = $1 AND collected_at > NOW() - INTERVAL '1 minute'
        """

        async with self.db_pool.acquire() as conn:
            result = await conn.fetchval(query, chat_id)

            if result and result > 10:  # Максимум 10 сообщений в минуту
                logger.warning(f"Rate limit exceeded for chat {chat_id}")
                await asyncio.sleep(60)  # Пауза на минуту

    def _extract_links(self, text: str) -> List[str]:
        """Извлечение ссылок из текста"""
        import re
        url_pattern = re.compile(r'https?://\S+')
        return url_pattern.findall(text)

    def _extract_mentions(self, text: str) -> List[str]:
        """Извлечение упоминаний"""
        import re
        mention_pattern = re.compile(r'@\w+')
        return mention_pattern.findall(text)

    def _extract_hashtags(self, text: str) -> List[str]:
        """Извлечение хештегов"""
        import re
        hashtag_pattern = re.compile(r'#\w+')
        return hashtag_pattern.findall(text)

    def _extract_tickers(self, text: str) -> List[str]:
        """Извлечение финансовых тикеров"""
        import re

        # Паттерны для тикеров (пример)
        ticker_patterns = [
            r'\$([A-Z]{1,5})\b',  # $AAPL
            r'\b([A-Z]{1,5}):(NASDAQ|NYSE|SPX)\b',  # AAPL:NASDAQ
            r'\b([A-Z]{2,5}\.[A-Z]{2})\b'  # BRK.B
        ]

        tickers = set()
        for pattern in ticker_patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                if isinstance(match, tuple):
                    tickers.add(match[0])
                else:
                    tickers.add(match)

        return list(tickers)

    async def save_message(self, message_data: Dict):
        """Сохранение сообщения в базу"""
        query = """
        INSERT INTO telegram_messages 
        (message_id, chat_id, chat_title, text, date, views, forwards, 
         has_media, links, mentions, hashtags, collected_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (message_id, chat_id) DO NOTHING
        """

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    query,
                    message_data['message_id'],
                    message_data['chat_id'],
                    message_data['chat_title'],
                    message_data['text'],
                    message_data['date'],
                    message_data['views'],
                    message_data['forwards'],
                    message_data['has_media'],
                    message_data['links'],
                    message_data['mentions'],
                    message_data['hashtags'],
                    message_data['collected_at']
                )
        except Exception as e:
            logger.error(f"Failed to save message: {e}")

    async def process_tickers(self, message_data: Dict, tickers: List[str]):
        """Обработка найденных тикеров"""
        query = """
        INSERT INTO telegram_ticker_mentions 
        (message_id, chat_id, ticker, mentioned_at, chat_title, message_text)
        VALUES ($1, $2, $3, $4, $5, $6)
        """

        try:
            async with self.db_pool.acquire() as conn:
                for ticker in tickers:
                    await conn.execute(
                        query,
                        message_data['message_id'],
                        message_data['chat_id'],
                        ticker.upper(),
                        message_data['date'],
                        message_data['chat_title'],
                        message_data['text'][:500]  # Ограничение длины
                    )
        except Exception as e:
            logger.error(f"Failed to save ticker mentions: {e}")

    async def get_channel_stats(self) -> Dict:
        """Получение статистики по каналам"""
        query = """
        SELECT 
            chat_title,
            COUNT(*) as message_count,
            MIN(date) as first_message,
            MAX(date) as last_message,
            AVG(views) as avg_views,
            SUM(forwards) as total_forwards
        FROM telegram_messages
        GROUP BY chat_title
        ORDER BY message_count DESC
        """

        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch(query)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Failed to get channel stats: {e}")
            return []

    async def run(self):
        """Запуск сбора данных"""
        logger.info("Starting Telegram data collection")

        try:
            await self.connect()

            # Бесконечный сбор
            while True:
                try:
                    await self.client.run_until_disconnected()
                except Exception as e:
                    logger.error(f"Telegram client disconnected: {e}")
                    await asyncio.sleep(30)  # Пауза перед переподключением
                    await self.connect()

        except KeyboardInterrupt:
            logger.info("Telegram collection stopped by user")
        finally:
            if self.client:
                await self.client.disconnect()
