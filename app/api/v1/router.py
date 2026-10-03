"""
Main API router
"""
from fastapi import APIRouter
from app.api.v1 import auth, users, items, audit

api_router = APIRouter()

api_router.include_router(auth.router, prefix="/auth", tags=["auth"])
api_router.include_router(users.router, prefix="/users", tags=["users"])
api_router.include_router(items.router, prefix="/items", tags=["items"])
api_router.include_router(audit.router, prefix="/audit", tags=["audit"])












