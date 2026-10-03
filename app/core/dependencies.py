"""
Dependencies для FastAPI
"""
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.core.database import get_db
from app.core.security import decode_access_token
from app.models.user import User
from app.core.error_handler import UnauthorizedError, ForbiddenError
from app.schemas.auth import TokenData

security = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db)
) -> User:
    """Получение текущего пользователя из JWT токена"""
    token = credentials.credentials
    token_data = decode_access_token(token)
    
    if token_data is None:
        raise UnauthorizedError("Invalid authentication credentials")
    
    from uuid import UUID
    user_id_uuid = UUID(token_data.user_id) if isinstance(token_data.user_id, str) else token_data.user_id
    result = await db.execute(select(User).where(User.id == user_id_uuid))
    user = result.scalar_one_or_none()
    
    if user is None:
        raise UnauthorizedError("User not found")
    
    if not user.is_active:
        raise UnauthorizedError("User is inactive")
    
    return user


async def get_current_active_user(
    current_user: User = Depends(get_current_user)
) -> User:
    """Получение активного пользователя"""
    return current_user


async def get_current_admin_user(
    current_user: User = Depends(get_current_user)
) -> User:
    """Получение пользователя с ролью admin"""
    if current_user.role != "admin":
        raise ForbiddenError("Admin access required")
    return current_user

