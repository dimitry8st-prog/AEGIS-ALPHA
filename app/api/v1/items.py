"""
Items module - CRUD для бизнес-сущности Item (с кэшированием и аудитом)
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List
from uuid import UUID

from app.core.database import get_db
from app.core.dependencies import get_current_active_user
from app.core.cache import cache_aside, generate_cache_key, delete_from_cache
from app.core.error_handler import NotFoundError
from app.models.user import User
from app.models.item import Item
from app.schemas.item import ItemCreate, ItemUpdate, ItemResponse
import json

router = APIRouter()


def serialize_item(item: Item) -> str:
    """Сериализация item для кэша"""
    return json.dumps({
        "id": str(item.id),
        "title": item.title,
        "description": item.description,
        "created_by": str(item.created_by) if item.created_by else None,
        "created_at": item.created_at.isoformat() if item.created_at else None,
        "updated_at": item.updated_at.isoformat() if item.updated_at else None,
    })


def deserialize_item(data: str) -> Item:
    """Десериализация item из кэша"""
    item_data = json.loads(data)
    item = Item()
    item.id = UUID(item_data["id"])
    item.title = item_data["title"]
    item.description = item_data["description"]
    item.created_by = UUID(item_data["created_by"]) if item_data["created_by"] else None
    return item


@router.get("", response_model=List[ItemResponse])
async def get_items(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """Получение списка items (с кэшированием)"""
    cache_key = generate_cache_key("items:list", skip=skip, limit=limit)
    
    async def fetch_items():
        result = await db.execute(
            select(Item)
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()
    
    items = await cache_aside(
        cache_key,
        fetch_items,
        ttl=300,  # 5 минут
        serialize=lambda items: json.dumps([serialize_item(i) for i in items]),
        deserialize=lambda data: [deserialize_item(i) for i in json.loads(data)]
    )
    
    return items


@router.get("/{item_id}", response_model=ItemResponse)
async def get_item(
    item_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """Получение item по ID (с кэшированием)"""
    cache_key = generate_cache_key("item", item_id=str(item_id))
    
    async def fetch_item():
        result = await db.execute(select(Item).where(Item.id == item_id))
        item = result.scalar_one_or_none()
        if not item:
            raise NotFoundError("Item", str(item_id))
        return item
    
    item = await cache_aside(
        cache_key,
        fetch_item,
        ttl=600,  # 10 минут
        serialize=serialize_item,
        deserialize=deserialize_item
    )
    
    return item


@router.post("", response_model=ItemResponse, status_code=201)
async def create_item(
    item_data: ItemCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """Создание нового item (аудит автоматически через middleware)"""
    new_item = Item(
        title=item_data.title,
        description=item_data.description,
        created_by=current_user.id
    )
    
    db.add(new_item)
    await db.commit()
    await db.refresh(new_item)
    
    # Инвалидируем кэш списка
    await delete_from_cache("items:list:*")
    
    return new_item


@router.put("/{item_id}", response_model=ItemResponse)
async def update_item(
    item_id: UUID,
    item_update: ItemUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """Обновление item (аудит автоматически через middleware)"""
    result = await db.execute(select(Item).where(Item.id == item_id))
    item = result.scalar_one_or_none()
    
    if not item:
        raise NotFoundError("Item", str(item_id))
    
    # Обновляем поля
    update_data = item_update.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(item, field, value)
    
    await db.commit()
    await db.refresh(item)
    
    # Инвалидируем кэш
    cache_key = generate_cache_key("item", item_id=str(item_id))
    await delete_from_cache(cache_key)
    await delete_from_cache("items:list:*")
    
    return item


@router.delete("/{item_id}", status_code=204)
async def delete_item(
    item_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """Удаление item (аудит автоматически через middleware)"""
    result = await db.execute(select(Item).where(Item.id == item_id))
    item = result.scalar_one_or_none()
    
    if not item:
        raise NotFoundError("Item", str(item_id))
    
    await db.delete(item)
    await db.commit()
    
    # Инвалидируем кэш
    cache_key = generate_cache_key("item", item_id=str(item_id))
    await delete_from_cache(cache_key)
    await delete_from_cache("items:list:*")
    
    return None

