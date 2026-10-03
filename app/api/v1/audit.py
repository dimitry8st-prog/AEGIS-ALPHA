"""
Audit module - просмотр audit logs (admin only)
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, desc
from typing import List, Optional
from datetime import datetime, timedelta
from uuid import UUID

from app.core.database import get_db
from app.core.dependencies import get_current_admin_user
from app.core.config import settings
from app.models.user import User
from app.models.audit_log import AuditLog
from app.schemas.audit import AuditLogResponse

router = APIRouter()


@router.get("", response_model=List[AuditLogResponse])
async def get_audit_logs(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    resource_type: Optional[str] = Query(None),
    action: Optional[str] = Query(None),
    user_id: Optional[UUID] = Query(None),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user)
):
    """
    Получение audit logs с фильтрацией (admin only)
    """
    query = select(AuditLog)
    
    # Применяем фильтры
    conditions = []
    
    if resource_type:
        conditions.append(AuditLog.resource_type == resource_type)
    
    if action:
        conditions.append(AuditLog.action == action)
    
    if user_id:
        conditions.append(AuditLog.user_id == user_id)
    
    if start_date:
        conditions.append(AuditLog.created_at >= start_date)
    
    if end_date:
        conditions.append(AuditLog.created_at <= end_date)
    
    if conditions:
        query = query.where(and_(*conditions))
    
    # Сортировка по дате (новые сначала)
    query = query.order_by(desc(AuditLog.created_at))
    
    # Пагинация
    query = query.offset(skip).limit(limit)
    
    result = await db.execute(query)
    logs = result.scalars().all()
    
    return logs


@router.delete("/cleanup", status_code=204)
async def cleanup_old_audit_logs(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user)
):
    """
    Удаление старых audit logs согласно политике хранения
    """
    retention_days = settings.AUDIT_LOG_RETENTION_DAYS
    cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
    
    from sqlalchemy import delete
    stmt = delete(AuditLog).where(AuditLog.created_at < cutoff_date)
    
    result = await db.execute(stmt)
    await db.commit()
    
    deleted_count = result.rowcount
    
    return {
        "message": f"Deleted {deleted_count} audit logs older than {retention_days} days",
        "deleted_count": deleted_count
    }












