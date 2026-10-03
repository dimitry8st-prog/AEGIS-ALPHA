"""
Конфигурация приложения
"""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # Application
    APP_PORT: int = 8000
    LOG_LEVEL: str = "INFO"
    INSTANCE_ID: str = "instance-1"
    
    # Database
    # ВАЖНО: значения по умолчанию синхронизированы с docker-compose.yml
    DATABASE_URL: str
    POSTGRES_USER: str = "aegis"
    POSTGRES_PASSWORD: str = "aegis123"
    POSTGRES_DB: str = "aegis"
    
    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"
    
    # JWT
    JWT_SECRET_KEY: str
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRATION_HOURS: int = 24
    
    # Rate Limiting
    RATE_LIMIT_REQUESTS: int = 100
    RATE_LIMIT_WINDOW_SECONDS: int = 60
    
    # Audit
    AUDIT_LOG_RETENTION_DAYS: int = 90
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()






