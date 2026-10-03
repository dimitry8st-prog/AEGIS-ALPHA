"""
Auth module - регистрация, логин, получение текущего пользователя
"""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from datetime import timedelta

from app.core.database import get_db
from app.core.security import (
    verify_password,
    get_password_hash,
    create_access_token
)
from app.core.config import settings
from app.core.dependencies import get_current_active_user
from app.core.error_handler import AppException
from app.models.user import User
from app.schemas.auth import UserRegister, UserLogin, Token
from app.schemas.user import UserResponse

router = APIRouter()


@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def register(
    user_data: UserRegister,
    db: AsyncSession = Depends(get_db)
):
    """Регистрация нового пользователя"""
    # Проверяем, существует ли пользователь
    result = await db.execute(select(User).where(User.email == user_data.email))
    existing_user = result.scalar_one_or_none()
    
    if existing_user:
        raise AppException(
            code="USER_ALREADY_EXISTS",
            message="User with this email already exists",
            status_code=status.HTTP_400_BAD_REQUEST
        )
    
    # Создаем нового пользователя
    new_user = User(
        email=user_data.email,
        password_hash=get_password_hash(user_data.password),
        role="user"
    )
    
    db.add(new_user)
    await db.commit()
    await db.refresh(new_user)
    
    return new_user


@router.post("/login", response_model=Token)
async def login(
    user_data: UserLogin,
    db: AsyncSession = Depends(get_db)
):
    """Логин пользователя, возвращает JWT токен"""
    # Находим пользователя
    result = await db.execute(select(User).where(User.email == user_data.email))
    user = result.scalar_one_or_none()
    
    if not user or not verify_password(user_data.password, user.password_hash):
        raise AppException(
            code="INVALID_CREDENTIALS",
            message="Invalid email or password",
            status_code=status.HTTP_401_UNAUTHORIZED
        )
    
    if not user.is_active:
        raise AppException(
            code="USER_INACTIVE",
            message="User account is inactive",
            status_code=status.HTTP_403_FORBIDDEN
        )
    
    # Создаем токен
    access_token_expires = timedelta(hours=settings.JWT_EXPIRATION_HOURS)
    access_token = create_access_token(
        data={
            "sub": str(user.id),
            "email": user.email,
            "role": user.role
        },
        expires_delta=access_token_expires
    )
    
    return {"access_token": access_token, "token_type": "bearer"}


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(
    current_user: User = Depends(get_current_active_user)
):
    """Получение информации о текущем пользователе"""
    return current_user












