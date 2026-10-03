"""
Audit schemas
"""
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from uuid import UUID


class AuditLogResponse(BaseModel):
    id: UUID
    user_id: Optional[UUID]
    action: str
    resource_type: str
    resource_id: Optional[UUID]
    data_before: Optional[dict]
    data_after: Optional[dict]
    ip_address: Optional[str]
    user_agent: Optional[str]
    instance_id: Optional[str]
    created_at: datetime
    
    class Config:
        from_attributes = True












