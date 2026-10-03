from datetime import datetime, timedelta
from typing import Dict, List, Optional
from enum import Enum
import hashlib
from pydantic import BaseModel, Field
from loguru import logger
from ..config.settings import settings


class DataCategory(Enum):
    """Категории данных для политики хранения"""
    PERSONAL = "personal"
    FINANCIAL = "financial"
    MARKET = "market"
    NEWS = "news"
    ANALYTICAL = "analytical"


class RetentionPolicy(BaseModel):
    """Политика хранения данных"""
    category: DataCategory
    retention_days: int
    anonymize_after_days: Optional[int] = None
    encryption_required: bool = True


class DataPolicyManager:
    """Менеджер политик хранения и удаления данных"""

    def __init__(self):
        self.policies = self._load_policies()
        self.setup_compliance_logging()

    def _load_policies(self) -> Dict[DataCategory, RetentionPolicy]:
        """Загрузка политик хранения"""
        return {
            DataCategory.PERSONAL: RetentionPolicy(
                category=DataCategory.PERSONAL,
                retention_days=30,
                anonymize_after_days=7,
                encryption_required=True
            ),
            DataCategory.FINANCIAL: RetentionPolicy(
                category=DataCategory.FINANCIAL,
                retention_days=365,
                encryption_required=True
            ),
            DataCategory.MARKET: RetentionPolicy(
                category=DataCategory.MARKET,
                retention_days=90,
                encryption_required=False
            ),
            DataCategory.NEWS: RetentionPolicy(
                category=DataCategory.NEWS,
                retention_days=180,
                encryption_required=False
            ),
            DataCategory.ANALYTICAL: RetentionPolicy(
                category=DataCategory.ANALYTICAL,
                retention_days=730,  # 2 года
                encryption_required=True
            )
        }

    def setup_compliance_logging(self):
        """Настройка логирования для compliance"""
        logger.add(
            f"{settings.logs_dir}/compliance.log",
            format="{time:YYYY-MM-DD HH:mm:ss} | COMPLIANCE | {message}",
            rotation="100 MB",
            retention="180 days",
            level="INFO"
        )

    def categorize_data(self, data_type: str, content: Dict) -> DataCategory:
        """Категоризация данных"""
        # Эвристики для определения категории
        sensitive_keywords = {
            DataCategory.PERSONAL: ['name', 'email', 'phone', 'address'],
            DataCategory.FINANCIAL: ['price', 'volume', 'trade', 'portfolio'],
            DataCategory.MARKET: ['market', 'index', 'sector', 'industry'],
            DataCategory.NEWS: ['news', 'article', 'headline', 'report'],
            DataCategory.ANALYTICAL: ['analysis', 'prediction', 'signal', 'model']
        }

        for category, keywords in sensitive_keywords.items():
            if any(keyword in data_type.lower() for keyword in keywords):
                return category

        return DataCategory.MARKET  # По умолчанию

    def should_retain(self, category: DataCategory, created_at: datetime) -> bool:
        """Проверка, нужно ли сохранять данные"""
        policy = self.policies.get(category)
        if not policy:
            return False

        cutoff_date = datetime.utcnow() - timedelta(days=policy.retention_days)
        return created_at > cutoff_date

    def anonymize_data(self, data: Dict, category: DataCategory) -> Dict:
        """Анонимизация данных"""
        anonymized = data.copy()
        policy = self.policies.get(category)

        if not policy or not policy.anonymize_after_days:
            return anonymized

        # Анонимизация персональных данных
        if category == DataCategory.PERSONAL:
            for key in ['name', 'email', 'phone', 'ip_address']:
                if key in anonymized:
                    if isinstance(anonymized[key], str):
                        anonymized[key] = f"ANON_{hashlib.sha256(anonymized[key].encode()).hexdigest()[:8]}"

        return anonymized

    async def apply_retention_policy(self, db_pool):
        """Применение политик хранения к базе данных"""
        logger.info("Applying data retention policies")

        for category, policy in self.policies.items():
            cutoff_date = datetime.utcnow() - timedelta(days=policy.retention_days)

            # Удаление старых данных
            if category == DataCategory.PERSONAL:
                query = """
                UPDATE users 
                SET personal_data = NULL 
                WHERE last_updated < $1
                """
            elif category == DataCategory.MARKET:
                query = """
                DELETE FROM market_data 
                WHERE timestamp < $1
                """
            elif category == DataCategory.NEWS:
                query = """
                DELETE FROM news_articles 
                WHERE published_at < $1
                """
            else:
                continue

            try:
                async with db_pool.acquire() as conn:
                    result = await conn.execute(query, cutoff_date)
                    logger.info(f"Applied retention for {category}: {result}")
            except Exception as e:
                logger.error(f"Failed to apply retention for {category}: {e}")

    def get_compliance_report(self) -> Dict:
        """Генерация отчета о compliance"""
        report = {
            "generated_at": datetime.utcnow().isoformat(),
            "policies": {},
            "recommendations": []
        }

        for category, policy in self.policies.items():
            report["policies"][category.value] = {
                "retention_days": policy.retention_days,
                "anonymize_after_days": policy.anonymize_after_days,
                "encryption_required": policy.encryption_required
            }

        # Рекомендации
        if settings.data_retention_days > 365:
            report["recommendations"].append(
                "Consider reducing global retention days to comply with GDPR"
            )

        logger.info("Compliance report generated")
        return report