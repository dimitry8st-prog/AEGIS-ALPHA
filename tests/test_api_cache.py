"""API cache timestamps, prefix invalidation, and fail-open reads."""

from __future__ import annotations

import fnmatch
import uuid
from datetime import datetime, timezone

import pytest

from app.api.v1.items import deserialize_item, get_items, serialize_item, update_item
from app.api.v1.users import deserialize_user, serialize_user
from app.core import cache as cache_module
from app.core.cache import cache_aside, delete_cache_prefix
from app.models.item import Item
from app.models.user import User
from app.schemas.item import ItemUpdate


class MemoryRedis:
    def __init__(self):
        self.store: dict[str, str] = {}

    async def get(self, key: str):
        return self.store.get(key)

    async def setex(self, key: str, ttl: int, value: str):
        self.store[key] = value

    async def scan_iter(self, match: str, count: int = 200):
        for key in list(self.store):
            if fnmatch.fnmatch(key, match):
                yield key

    async def delete(self, *keys):
        for key in keys:
            self.store.pop(key, None)

    async def keys(self, *args, **kwargs):
        raise AssertionError("KEYS must not be used")


def test_cache_roundtrip_restores_timestamps():
    created = datetime(2024, 1, 2, 15, 0, tzinfo=timezone.utc)
    updated = datetime(2024, 1, 3, 15, 0, tzinfo=timezone.utc)
    user = User()
    user.id = uuid.uuid4()
    user.email = "cache@example.com"
    user.role = "user"
    user.is_active = True
    user.created_at = created
    user.updated_at = updated
    restored_user = deserialize_user(serialize_user(user))
    assert restored_user.created_at == created
    assert restored_user.updated_at == updated

    item = Item()
    item.id = uuid.uuid4()
    item.title = "t"
    item.description = "d"
    item.created_by = user.id
    item.created_at = created
    item.updated_at = updated
    restored = deserialize_item(serialize_item(item))
    assert restored.created_at == created
    assert restored.updated_at == updated


class _Rows:
    def __init__(self, value):
        self.value = value

    def scalars(self):
        return self

    def all(self):
        return self.value

    def scalar_one_or_none(self):
        return self.value


class _Session:
    def __init__(self, item):
        self.item = item
        self.calls = 0

    async def execute(self, *args, **kwargs):
        self.calls += 1
        return _Rows([self.item])

    async def commit(self):
        return None

    async def refresh(self, obj):
        return None


@pytest.mark.asyncio
async def test_items_cache_miss_and_hit_share_timestamps():
    created = datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc)
    item = Item()
    item.id = uuid.uuid4()
    item.title = "first"
    item.description = "body"
    item.created_by = uuid.uuid4()
    item.created_at = created
    item.updated_at = created
    session = _Session(item)
    redis = MemoryRedis()
    previous = cache_module.redis_client._client
    cache_module.redis_client._client = redis
    user = User()
    user.id = item.created_by
    try:
        first = await get_items(skip=0, limit=100, db=session, current_user=user)
        second = await get_items(skip=0, limit=100, db=session, current_user=user)
    finally:
        cache_module.redis_client._client = previous
    assert session.calls == 1
    assert first[0].created_at == created
    assert second[0].updated_at == created
    assert second[0].title == "first"


@pytest.mark.asyncio
async def test_list_pages_are_invalidated_without_keys_command():
    created = datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc)
    item = Item()
    item.id = uuid.uuid4()
    item.title = "first"
    item.description = "body"
    item.created_by = uuid.uuid4()
    item.created_at = created
    item.updated_at = created
    session = _Session(item)
    redis = MemoryRedis()
    previous = cache_module.redis_client._client
    cache_module.redis_client._client = redis
    user = User()
    user.id = item.created_by
    try:
        await get_items(skip=0, limit=100, db=session, current_user=user)
        await get_items(skip=100, limit=50, db=session, current_user=user)
        assert any(key.startswith("items:list:") for key in redis.store)
        item.title = "updated"
        item.updated_at = datetime(2024, 1, 4, 12, 0, tzinfo=timezone.utc)

        class _Update(_Session):
            async def execute(self, *args, **kwargs):
                return _Rows(item)

        await update_item(
            item.id,
            ItemUpdate(title="updated"),
            db=_Update(item),
            current_user=user,
        )
        assert not any(key.startswith("items:list:") for key in redis.store)
        refreshed = _Session(item)
        third = await get_items(skip=0, limit=100, db=refreshed, current_user=user)
    finally:
        cache_module.redis_client._client = previous
    assert third[0].title == "updated"
    assert third[0].updated_at == item.updated_at
    assert refreshed.calls == 1


@pytest.mark.asyncio
async def test_corrupt_and_unavailable_cache_read_the_source():
    calls = {"n": 0}

    async def fetch():
        calls["n"] += 1
        return {"ok": True}

    redis = MemoryRedis()
    redis.store["broken"] = "{not-json"
    previous = cache_module.redis_client._client
    cache_module.redis_client._client = redis
    try:
        payload = await cache_aside("broken", fetch)
        assert payload == {"ok": True}

        class Down(MemoryRedis):
            async def get(self, key: str):
                raise ConnectionError("redis down")

        cache_module.redis_client._client = Down()
        again = await cache_aside("missing", fetch)
    finally:
        cache_module.redis_client._client = previous
    assert again == {"ok": True}
    assert calls["n"] == 2


@pytest.mark.asyncio
async def test_prefix_delete_ignores_redis_outage():
    class Down(MemoryRedis):
        async def scan_iter(self, match: str, count: int = 200):
            raise ConnectionError("redis down")
            yield ""

    previous = cache_module.redis_client._client
    cache_module.redis_client._client = Down()
    try:
        await delete_cache_prefix("items:list:")
    finally:
        cache_module.redis_client._client = previous
