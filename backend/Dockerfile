# syntax=docker/dockerfile:1.7
FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    BACKEND_PORT=8000 \
    CLICKHOUSE_URL= \
    CLICKHOUSE_HOST=db \
    CLICKHOUSE_PORT=9000 \
    CLICKHOUSE_DATABASE=gpw \
    CLICKHOUSE_USER=default \
    CLICKHOUSE_PASSWORD= \
    CLICKHOUSE_SECURE=false \
    CLICKHOUSE_VERIFY=false

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api ./api

EXPOSE 8000

CMD ["sh", "-c", "uvicorn api.main:app --host 0.0.0.0 --port ${BACKEND_PORT:-8000}"]
