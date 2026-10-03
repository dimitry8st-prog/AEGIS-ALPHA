"""
AuditLog model
"""
from sqlalchemy import Column, String, Text, ForeignKey, DateTime, func, Index
from sqlalchemy.dialects.postgresql import UUID, JSONB, INET
import uuid
from app.core.database import Base


class AuditLog(Base):
    __tablename__ = "audit_logs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    action = Column(String(50), nullable=False, index=True)  # CREATE, UPDATE, DELETE
    resource_type = Column(String(100), nullable=False, index=True)
    resource_id = Column(UUID(as_uuid=True), nullable=True)
    data_before = Column(JSONB)
    data_after = Column(JSONB)
    ip_address = Column(INET)
    user_agent = Column(Text)
    instance_id = Column(String(100))
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    
    __table_args__ = (
        Index('idx_audit_resource', 'resource_type', 'resource_id'),
    )
    
    def __repr__(self):
        return f"<AuditLog(id={self.id}, action={self.action}, resource={self.resource_type})>"












