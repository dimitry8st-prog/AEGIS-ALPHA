import json
import hashlib
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from loguru import logger
import asyncpg
from ..config.settings import settings


class AuditLogger:
    """Система аудита и логирования всех действий"""

    def __init__(self, db_pool: asyncpg.Pool):
        self.db_pool = db_pool
        self.setup_logging()

    def setup_logging(self):
        """Настройка структурированного логирования"""
        logger.add(
            f"{settings.logs_dir}/audit.log",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
            rotation="500 MB",
            retention=f"{settings.data_retention_days} days",
            serialize=True
        )

    async def log_action(
            self,
            user_id: str,
            action: str,
            resource_type: str,
            resource_id: str,
            details: Dict[str, Any],
            ip_address: Optional[str] = None
    ) -> str:
        """Логирование действия с хешированием конфиденциальных данных"""

        # Хеширование конфиденциальных данных
        hashed_details = self._hash_sensitive_data(details)
        audit_id = hashlib.sha256(
            f"{user_id}{action}{resource_type}{resource_id}{datetime.utcnow().isoformat()}".encode()
        ).hexdigest()[:32]

        query = """
        INSERT INTO audit_logs 
        (id, user_id, action, resource_type, resource_id, details, ip_address, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    query,
                    audit_id,
                    user_id,
                    action,
                    resource_type,
                    resource_id,
                    json.dumps(hashed_details),
                    ip_address,
                    datetime.utcnow()
                )

            # Структурированное логирование
            logger.info(
                "Audit log created",
                audit_id=audit_id,
                user_id=user_id,
                action=action,
                resource_type=resource_type,
                resource_id=resource_id
            )

            return audit_id

        except Exception as e:
            logger.error(f"Failed to log audit action: {e}")
            raise

    def _hash_sensitive_data(self, details: Dict[str, Any]) -> Dict[str, Any]:
        """Хеширование конфиденциальных данных"""
        sensitive_fields = {'api_key', 'password', 'token', 'secret', 'phone'}
        hashed_details = details.copy()

        for key, value in details.items():
            if any(sensitive in key.lower() for sensitive in sensitive_fields):
                if isinstance(value, str):
                    hashed_details[key] = f"HASHED:{hashlib.sha256(value.encode()).hexdigest()[:16]}"

        return hashed_details

    async def get_audit_trail(
            self,
            user_id: Optional[str] = None,
            action: Optional[str] = None,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
            limit: int = 100
    ) -> list:
        """Получение логов аудита с фильтрами"""

        query = """
        SELECT * FROM audit_logs 
        WHERE 1=1
        """
        params = []
        param_count = 0

        if user_id:
            param_count += 1
            query += f" AND user_id = ${param_count}"
            params.append(user_id)

        if action:
            param_count += 1
            query += f" AND action = ${param_count}"
            params.append(action)

        if start_date:
            param_count += 1
            query += f" AND created_at >= ${param_count}"
            params.append(start_date)

        if end_date:
            param_count += 1
            query += f" AND created_at <= ${param_count}"
            params.append(end_date)

        query += f" ORDER BY created_at DESC LIMIT ${param_count + 1}"
        params.append(limit)

        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch(query, *params)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Failed to get audit trail: {e}")
            return []

    async def cleanup_old_logs(self):
        """Очистка старых логов согласно политике хранения"""
        cutoff_date = datetime.utcnow() - timedelta(days=settings.data_retention_days)

        query = "DELETE FROM audit_logs WHERE created_at < $1"

        try:
            async with self.db_pool.acquire() as conn:
                deleted = await conn.execute(query, cutoff_date)
                logger.info(f"Cleaned up old audit logs: {deleted}")
        except Exception as e:
            logger.error(f"Failed to cleanup audit logs: {e}")