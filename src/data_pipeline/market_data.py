import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import yfinance as yf
import pandas as pd
import numpy as np
import redis.asyncio as redis
from loguru import logger
import asyncpg
from ..config.settings import settings


class MarketDataCollector:
    """Сборщик рыночных данных"""

    def __init__(self, db_pool: asyncpg.Pool, redis_client: redis.Redis):
        self.db_pool = db_pool
        self.redis = redis_client
        self.tickers = self._load_watchlist()

    def _load_watchlist(self) -> List[str]:
        """Загрузка списка тикеров для отслеживания"""
        # Базовый список (можно расширять)
        base_tickers = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META',
            'TSLA', 'NVDA', 'JPM', 'JNJ', 'V',
            'WMT', 'PG', 'DIS', 'MA', 'HD'
        ]
        return base_tickers

    async def collect_ticker_data(self, ticker: str, period: str = "1d") -> Optional[Dict]:
        """Сбор данных по тикеру"""
        cache_key = f"market:{ticker}:{period}:{datetime.now().date()}"

        # Проверка кэша
        cached = await self.redis.get(cache_key)
        if cached:
            logger.debug(f"Cache hit for {ticker}")
            return pd.read_json(cached, orient='split')

        try:
            # Загрузка данных
            stock = yf.Ticker(ticker)

            # Исторические данные
            hist = stock.history(period=period)
            if hist.empty:
                return None

            # Информация о компании
            info = stock.info

            # Рассчет технических индикаторов
            indicators = self._calculate_indicators(hist)

            # Формирование результата
            result = {
                'ticker': ticker,
                'company_name': info.get('longName', ticker),
                'sector': info.get('sector', 'Unknown'),
                'industry': info.get('industry', 'Unknown'),
                'historical_data': hist,
                'indicators': indicators,
                'current_price': info.get('currentPrice'),
                'market_cap': info.get('marketCap'),
                'volume': info.get('volume'),
                'collected_at': datetime.utcnow()
            }

            # Кэширование
            await self.redis.setex(
                cache_key,
                timedelta(hours=1),
                hist.to_json(orient='split')
            )

            return result

        except Exception as e:
            logger.error(f"Error collecting data for {ticker}: {e}")
            return None

    def _calculate_indicators(self, df: pd.DataFrame) -> Dict:
        """Расчет технических индикаторов"""
        try:
            # Простые скользящие средние
            df['SMA_20'] = df['Close'].rolling(window=20).mean()
            df['SMA_50'] = df['Close'].rolling(window=50).mean()
            df['SMA_200'] = df['Close'].rolling(window=200).mean()

            # RSI
            delta = df['Close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['RSI'] = 100 - (100 / (1 + rs))

            # MACD
            exp1 = df['Close'].ewm(span=12, adjust=False).mean()
            exp2 = df['Close'].ewm(span=26, adjust=False).mean()
            df['MACD'] = exp1 - exp2
            df['MACD_signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
            df['MACD_hist'] = df['MACD'] - df['MACD_signal']

            # Bollinger Bands
            df['BB_middle'] = df['Close'].rolling(window=20).mean()
            bb_std = df['Close'].rolling(window=20).std()
            df['BB_upper'] = df['BB_middle'] + (bb_std * 2)
            df['BB_lower'] = df['BB_middle'] - (bb_std * 2)

            # Volume indicators
            df['Volume_SMA'] = df['Volume'].rolling(window=20).mean()
            df['Volume_Ratio'] = df['Volume'] / df['Volume_SMA']

            # Подготовка результатов
            latest = df.iloc[-1] if not df.empty else None

            return {
                'sma_20': latest['SMA_20'] if latest is not None else None,
                'sma_50': latest['SMA_50'] if latest is not None else None,
                'sma_200': latest['SMA_200'] if latest is not None else None,
                'rsi': latest['RSI'] if latest is not None else None,
                'macd': latest['MACD'] if latest is not None else None,
                'macd_signal': latest['MACD_signal'] if latest is not None else None,
                'bb_upper': latest['BB_upper'] if latest is not None else None,
                'bb_middle': latest['BB_middle'] if latest is not None else None,
                'bb_lower': latest['BB_lower'] if latest is not None else None,
                'volume_ratio': latest['Volume_Ratio'] if latest is not None else None
            }

        except Exception as e:
            logger.error(f"Error calculating indicators: {e}")
            return {}

    async def save_market_data(self, data: Dict):
        """Сохранение рыночных данных в базу"""
        query = """
        INSERT INTO market_data 
        (ticker, company_name, sector, industry, current_price, 
         market_cap, volume, indicators, collected_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (ticker, collected_at) DO UPDATE SET
            current_price = EXCLUDED.current_price,
            market_cap = EXCLUDED.market_cap,
            volume = EXCLUDED.volume,
            indicators = EXCLUDED.indicators
        """

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    query,
                    data['ticker'],
                    data['company_name'],
                    data['sector'],
                    data['industry'],
                    data['current_price'],
                    data['market_cap'],
                    data['volume'],
                    data['indicators'],
                    data['collected_at']
                )
        except Exception as e:
            logger.error(f"Failed to save market data: {e}")

    async def collect_all_tickers(self):
        """Сбор данных по всем тикерам"""
        logger.info(f"Collecting market data for {len(self.tickers)} tickers")

        tasks = []
        for ticker in self.tickers:
            task = asyncio.create_task(self._collect_and_save(ticker))
            tasks.append(task)
            await asyncio.sleep(0.1)  # Rate limiting

        results = await asyncio.gather(*tasks, return_exceptions=True)

        success = sum(1 for r in results if r and not isinstance(r, Exception))
        logger.info(f"Market data collection complete: {success}/{len(self.tickers)} successful")

    async def _collect_and_save(self, ticker: str):
        """Сбор и сохранение данных по одному тикеру"""
        try:
            data = await self.collect_ticker_data(ticker)
            if data:
                await self.save_market_data(data)
                return True
        except Exception as e:
            logger.error(f"Error processing {ticker}: {e}")
        return False

    async def get_price_history(self, ticker: str, days: int = 30) -> Optional[pd.DataFrame]:
        """Получение истории цен из кэша/базы"""
        cache_key = f"history:{ticker}:{days}"

        # Проверка кэша
        cached = await self.redis.get(cache_key)
        if cached:
            return pd.read_json(cached, orient='split')

        # Загрузка из Yahoo Finance
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            stock = yf.Ticker(ticker)
            hist = stock.history(start=start_date, end=end_date)

            if not hist.empty:
                # Кэширование
                await self.redis.setex(
                    cache_key,
                    timedelta(hours=6),
                    hist.to_json(orient='split')
                )

            return hist

        except Exception as e:
            logger.error(f"Error getting price history for {ticker}: {e}")
            return None

    async def run(self, interval_minutes: int = 5):
        """Запуск периодического сбора данных"""
        logger.info(f"Starting market data collection (interval: {interval_minutes}min)")

        while True:
            try:
                await self.collect_all_tickers()
                logger.info(f"Waiting {interval_minutes} minutes before next collection...")
                await asyncio.sleep(interval_minutes * 60)

            except KeyboardInterrupt:
                logger.info("Market data collection stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in market data collection loop: {e}")
                await asyncio.sleep(60)  # Пауза при ошибке