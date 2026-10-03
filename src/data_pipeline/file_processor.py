import os
import asyncio
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import pdfplumber
import pandas as pd
import pytesseract
from PIL import Image
import openpyxl
from loguru import logger
import asyncpg
from ..config.settings import settings


class FileProcessor:
    """Обработчик файлов (PDF, Excel, изображения)"""

    def __init__(self, db_pool: asyncpg.Pool):
        self.db_pool = db_pool
        self.supported_formats = ['.pdf', '.xlsx', '.xls', '.csv', '.png', '.jpg', '.jpeg']

    async def process_directory(self, directory_path: str):
        """Обработка всех файлов в директории"""
        if not os.path.exists(directory_path):
            logger.error(f"Directory not found: {directory_path}")
            return

        logger.info(f"Processing directory: {directory_path}")

        for root, _, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)

                # Проверка формата
                if not any(file.lower().endswith(fmt) for fmt in self.supported_formats):
                    continue

                try:
                    await self.process_file(file_path)
                    logger.info(f"Processed: {file_path}")

                except Exception as e:
                    logger.error(f"Error processing {file_path}: {e}")

    async def process_file(self, file_path: str):
        """Обработка отдельного файла"""
        file_ext = os.path.splitext(file_path)[1].lower()

        # Проверка, не обработан ли уже файл
        if await self._is_already_processed(file_path):
            logger.debug(f"File already processed: {file_path}")
            return

        try:
            if file_ext == '.pdf':
                content = await self._process_pdf(file_path)
            elif file_ext in ['.xlsx', '.xls']:
                content = await self._process_excel(file_path)
            elif file_ext == '.csv':
                content = await self._process_csv(file_path)
            elif file_ext in ['.png', '.jpg', '.jpeg']:
                content = await self._process_image(file_path)
            else:
                logger.warning(f"Unsupported format: {file_ext}")
                return

            # Сохранение результатов
            await self._save_file_content(file_path, content)

        except Exception as e:
            logger.error(f"Failed to process {file_path}: {e}")
            raise

    async def _process_pdf(self, file_path: str) -> Dict:
        """Обработка PDF файлов"""
        content = {
            'text': '',
            'tables': [],
            'metadata': {},
            'pages': []
        }

        try:
            with pdfplumber.open(file_path) as pdf:
                content['metadata'] = {
                    'pages': len(pdf.pages),
                    'author': pdf.metadata.get('Author'),
                    'title': pdf.metadata.get('Title'),
                    'created': pdf.metadata.get('CreationDate')
                }

                for i, page in enumerate(pdf.pages):
                    # Извлечение текста
                    page_text = page.extract_text() or ''
                    content['text'] += page_text + '\n'

                    # Извлечение таблиц
                    tables = page.extract_tables()
                    if tables:
                        for table in tables:
                            if table and len(table) > 0:
                                content['tables'].append({
                                    'page': i + 1,
                                    'table': table
                                })

                    # Информация о странице
                    content['pages'].append({
                        'page_number': i + 1,
                        'text_length': len(page_text),
                        'has_tables': bool(tables)
                    })

        except Exception as e:
            logger.error(f"PDF processing error for {file_path}: {e}")
            raise

        return content

    async def _process_excel(self, file_path: str) -> Dict:
        """Обработка Excel файлов"""
        content = {
            'sheets': [],
            'dataframes': [],
            'summary': {}
        }

        try:
            # Чтение всех листов
            xls = pd.ExcelFile(file_path)
            sheet_names = xls.sheet_names

            for sheet_name in sheet_names:
                try:
                    # Чтение листа
                    df = pd.read_excel(file_path, sheet_name=sheet_name)

                    sheet_info = {
                        'name': sheet_name,
                        'shape': df.shape,
                        'columns': df.columns.tolist(),
                        'head': df.head(5).to_dict('records'),
                        'dtypes': df.dtypes.astype(str).to_dict(),
                        'summary': self._generate_dataframe_summary(df)
                    }

                    content['sheets'].append(sheet_info)
                    content['dataframes'].append(df)

                except Exception as e:
                    logger.error(f"Error reading sheet {sheet_name}: {e}")

            content['summary'] = {
                'total_sheets': len(sheet_names),
                'total_rows': sum(len(df) for df in content['dataframes']),
                'total_columns': sum(len(df.columns) for df in content['dataframes'])
            }

        except Exception as e:
            logger.error(f"Excel processing error for {file_path}: {e}")
            raise

        return content

    async def _process_csv(self, file_path: str) -> Dict:
        """Обработка CSV файлов"""
        content = {
            'dataframe': None,
            'summary': {}
        }

        try:
            # Автоматическое определение разделителя
            df = pd.read_csv(file_path, encoding_errors='ignore')

            content['dataframe'] = df
            content['summary'] = {
                'shape': df.shape,
                'columns': df.columns.tolist(),
                'head': df.head(5).to_dict('records'),
                'dtypes': df.dtypes.astype(str).to_dict(),
                'stats': self._generate_dataframe_summary(df)
            }

        except Exception as e:
            logger.error(f"CSV processing error for {file_path}: {e}")
            raise

        return content

    async def _process_image(self, file_path: str) -> Dict:
        """Обработка изображений (OCR)"""
        content = {
            'text': '',
            'metadata': {},
            'ocr_confidence': 0
        }

        try:
            # Открытие изображения
            image = Image.open(file_path)

            # Метаданные
            content['metadata'] = {
                'format': image.format,
                'size': image.size,
                'mode': image.mode
            }

            # OCR обработка
            text = pytesseract.image_to_string(image, lang='eng+rus')
            content['text'] = text

            # Оценка качества OCR (простая эвристика)
            content['ocr_confidence'] = self._estimate_ocr_confidence(text)

        except Exception as e:
            logger.error(f"Image processing error for {file_path}: {e}")
            raise

        return content

    def _generate_dataframe_summary(self, df: pd.DataFrame) -> Dict:
        """Генерация статистики по DataFrame"""
        summary = {}

        for column in df.columns:
            col_data = df[column]

            col_summary = {
                'dtype': str(col_data.dtype),
                'non_null_count': col_data.count(),
                'null_count': col_data.isnull().sum(),
                'unique_count': col_data.nunique()
            }

            if pd.api.types.is_numeric_dtype(col_data):
                col_summary.update({
                    'mean': float(col_data.mean()) if col_data.count() > 0 else None,
                    'std': float(col_data.std()) if col_data.count() > 1 else None,
                    'min': float(col_data.min()) if col_data.count() > 0 else None,
                    'max': float(col_data.max()) if col_data.count() > 0 else None,
                    'median': float(col_data.median()) if col_data.count() > 0 else None
                })
            elif pd.api.types.is_string_dtype(col_data):
                col_summary.update({
                    'most_common': col_data.mode().iloc[0] if not col_data.mode().empty else None,
                    'avg_length': col_data.str.len().mean() if col_data.count() > 0 else None
                })

            summary[column] = col_summary

        return summary

    def _estimate_ocr_confidence(self, text: str) -> float:
        """Оценка качества OCR"""
        if not text:
            return 0.0

        # Простая эвристика: соотношение осмысленных слов
        lines = text.split('\n')
        meaningful_lines = 0

        for line in lines:
            line = line.strip()
            if len(line) > 10:  # Строки длиннее 10 символов
                words = line.split()
                if len(words) > 1:  # Хотя бы 2 слова
                    meaningful_lines += 1

        confidence = meaningful_lines / max(len(lines), 1)
        return min(max(confidence, 0.0), 1.0)

    async def _is_already_processed(self, file_path: str) -> bool:
        """Проверка, обработан ли уже файл"""
        query = """
        SELECT COUNT(*) FROM processed_files 
        WHERE file_path = $1 AND processed_at > NOW() - INTERVAL '7 days'
        """

        try:
            async with self.db_pool.acquire() as conn:
                count = await conn.fetchval(query, file_path)
                return count > 0
        except Exception:
            return False

    async def _save_file_content(self, file_path: str, content: Dict):
        """Сохранение обработанного файла в базу"""
        query = """
        INSERT INTO processed_files 
        (file_path, file_name, file_size, content_type, content_summary, 
         processed_at, metadata, content_preview)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """

        try:
            file_size = os.path.getsize(file_path)
            file_name = os.path.basename(file_path)

            # Создание сводки
            if 'text' in content:
                content_preview = content['text'][:1000]  # Первые 1000 символов
                content_type = 'text'
            elif 'sheets' in content:
                content_preview = str(content['summary'])
                content_type = 'spreadsheet'
            elif 'dataframe' in content:
                content_preview = str(content['summary'])
                content_type = 'table'
            else:
                content_preview = 'unknown'
                content_type = 'unknown'

            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    query,
                    file_path,
                    file_name,
                    file_size,
                    content_type,
                    json.dumps(content.get('summary', {})),
                    datetime.utcnow(),
                    json.dumps(content.get('metadata', {})),
                    content_preview
                )

        except Exception as e:
            logger.error(f"Failed to save file content: {e}")
            raise

    async def extract_financial_data(self, file_path: str) -> Optional[Dict]:
        """Извлечение финансовых данных из файла"""
        content = await self.process_file(file_path)

        if not content:
            return None

        # Поиск финансовых показателей (базовая реализация)
        financial_data = {
            'revenue': None,
            'profit': None,
            'assets': None,
            'liabilities': None,
            'equity': None,
            'found_at': []
        }

        text = content.get('text', '').lower()

        # Поиск ключевых показателей
        patterns = {
            'revenue': ['revenue', 'sales', 'выручка', 'доход'],
            'profit': ['profit', 'net income', 'чистая прибыль'],
            'assets': ['assets', 'активы', 'имущество'],
            'liabilities': ['liabilities', 'обязательства'],
            'equity': ['equity', 'капитал', 'собственный капитал']
        }

        for metric, keywords in patterns.items():
            for keyword in keywords:
                if keyword in text:
                    financial_data['found_at'].append({
                        'metric': metric,
                        'keyword': keyword,
                        'context': self._extract_context(text, keyword)
                    })

        return financial_data

    def _extract_context(self, text: str, keyword: str, context_len: int = 100) -> str:
        """Извлечение контекста вокруг ключевого слова"""
        import re

        pattern = re.compile(rf'.{{0,{context_len}}}\b{re.escape(keyword)}\b.{{0,{context_len}}}', re.IGNORECASE)
        match = pattern.search(text)

        if match:
            return match.group(0)
        return ''

    async def run(self, watch_directory: Optional[str] = None):
        """Запуск мониторинга директории"""
        if not watch_directory:
            watch_directory = os.path.join(settings.data_dir, 'incoming')

        os.makedirs(watch_directory, exist_ok=True)
        logger.info(f"Watching directory for new files: {watch_directory}")

        processed_files = set()

        while True:
            try:
                # Проверка новых файлов
                current_files = set(
                    os.path.join(root, f)
                    for root, _, files in os.walk(watch_directory)
                    for f in files
                    if any(f.lower().endswith(fmt) for fmt in self.supported_formats)
                )

                new_files = current_files - processed_files

                if new_files:
                    logger.info(f"Found {len(new_files)} new files to process")

                    for file_path in new_files:
                        try:
                            await self.process_file(file_path)
                            processed_files.add(file_path)
                        except Exception as e:
                            logger.error(f"Failed to process {file_path}: {e}")

                await asyncio.sleep(60)  # Проверка каждую минуту

            except KeyboardInterrupt:
                logger.info("File processing stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in file processing loop: {e}")
                await asyncio.sleep(30)