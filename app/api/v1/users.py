"""
Users module - CRUD операции для пользователей (admin only)
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from typing import List
from uuid import UUID

from app.core.database import get_db
from app.core.dependencies import get_current_admin_user
from app.core.cache import cache_aside, generate_cache_key, delete_from_cache
from app.core.error_handler import NotFoundError
from app.models.user import User
from app.schemas.user import UserCreate, UserUpdate, UserResponse
from app.core.security import get_password_hash
import json

router = APIRouter()


def serialize_user(user: User) -> str:
    """Сериализация пользователя для кэша"""
    return json.dumps({
        "id": str(user.id),
        "email": user.email,
        "role": user.role,
        "is_active": user.is_active,
        "created_at": user.created_at.isoformat() if user.created_at else None,
        "updated_at": user.updated_at.isoformat() if user.updated_at else None,
    })


def deserialize_user(data: str) -> User:
    """Десериализация пользователя из кэша"""
    user_data = json.loads(data)
    user = User()
    user.id = UUID(user_data["id"])
    user.email = user_data["email"]
    user.role = user_data["role"]
    user.is_active = user_data["is_active"]
    # created_at и updated_at можно пропустить для простоты
    return user


@router.get("", response_model=List[UserResponse])
async def get_users(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user)
):
    """Получение списка пользователей (с кэшированием)"""
    cache_key = generate_cache_key("users:list", skip=skip, limit=limit)
    
    async def fetch_users():
        result = await db.execute(
            select(User)
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()
    
    users = await cache_aside(
        cache_key,
        fetch_users,
        ttl=300,  # 5 минут
        serialize=lambda users: json.dumps([serialize_user(u) for u in users]),
        deserialize=lambda data: [deserialize_user(u) for u in json.loads(data)]
    )
    
    return users


@router.get("/{user_id}", response_model=UserResponse)
async def get_user(
    user_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user)
):
    """Получение пользователя по ID (с кэшированием)"""
    cache_key = generate_cache_key("user", user_id=str(user_id))
    
    async def fetch_user():
        result = await db.execute(select(User).where(User.id == user_id))
        user = result.scalar_one_or_none()
        if not user:
            raise NotFoundError("User", str(user_id))
        return user
    
    user = await cache_aside(
        cache_key,
        fetch_user,
        ttl=600,  # 10 минут
        serialize=serialize_user,
        deserialize=deserialize_user
    )
    
    return user


@router.put("/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: UUID,
    user_update: UserUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user)
):
    """Обновление пользователя"""
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    
    if not user:
        raise NotFoundError("User", str(user_id))
    
    # Обновляем поля
    update_data = user_update.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(user, field, value)
    
    await db.commit()
    await db.refresh(user)
    
    # Инвалидируем кэш
    cache_key = generate_cache_key("user", user_id=str(user_id))
    await delete_from_cache(cache_key)
    await delete_from_cache("users:list:*")  # Инвалидируем список
    
    return user


@router.delete("/{user_id}", status_code=204)
async def delete_user(
    user_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user)
):
    """Удаление пользователя"""
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    
    if not user:
        raise NotFoundError("User", str(user_id))
    
    await db.delete(user)
    await db.commit()
    
    # Инвалидируем кэш
    cache_key = generate_cache_key("user", user_id=str(user_id))
    await delete_from_cache(cache_key)
    await delete_from_cache("users:list:*")
    
    return None












