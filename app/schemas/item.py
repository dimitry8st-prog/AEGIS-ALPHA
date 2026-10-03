"""
Item schemas
"""
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from uuid import UUID


class ItemCreate(BaseModel):
    title: str
    description: Optional[str] = None


class ItemUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None


class ItemResponse(BaseModel):
    id: UUID
    title: str
    description: Optional[str]
    created_by: Optional[UUID]
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True












