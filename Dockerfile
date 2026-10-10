FROM python:3.11-slim

WORKDIR /app

# No apt packages: runtime deps ship as wheels (psycopg2-binary, asyncpg,
# numpy/pandas, cryptography). gcc and postgresql-client are not required in
# the final image and previously exhausted host RAM during `apt-get install`.

COPY requirements.txt .
# Omit test-only packages from the runtime image to cut build peak memory.
RUN grep -vE '^(pytest|pytest-)' requirements.txt > /tmp/requirements.runtime.txt \
    && pip install --no-cache-dir --no-compile -r /tmp/requirements.runtime.txt \
    && rm /tmp/requirements.runtime.txt

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
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

EXPOSE 8000

# Запускаем FastAPI-приложение, используя модуль app.main
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
