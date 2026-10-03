FROM python:3.11-slim

WORKDIR /app

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Копирование requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование всего проекта так, чтобы пакет app был доступен как модуль
COPY . /app

# Создание пользователя для безопасности
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Переменные окружения
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/health')"

EXPOSE 8000

# Запускаем FastAPI-приложение, используя модуль app.main
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
