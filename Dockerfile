# Multi-stage Dockerfile for all RecSys services
# Usage:
#   docker build --target ingestion -t recsys-ingestion .
#   docker build --target consumer -t recsys-consumer .
#   docker build --target recommendation -t recsys-recommendation .

# Builder stage: install Poetry + deps
FROM python:3.10-slim AS builder

ENV POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1

RUN pip install poetry && \
    poetry --version

WORKDIR /app
COPY pyproject.toml poetry.lock* ./
RUN poetry install --no-root --only main

COPY common/ common/
COPY ingestion_service/ ingestion_service/
COPY consumer_service/ consumer_service/
COPY recommendation_api/ recommendation_api/
COPY scripts/ scripts/
COPY alembic/ alembic/
COPY alembic.ini ./
COPY README.md ./

RUN poetry install --only main

# Ingestion Service
FROM python:3.10-slim AS ingestion
WORKDIR /app
COPY --from=builder /app /app
ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8001
CMD ["uvicorn", "ingestion_service.main:app", "--host", "0.0.0.0", "--port", "8001"]

# Consumer Service
FROM python:3.10-slim AS consumer
WORKDIR /app
COPY --from=builder /app /app
ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8003
CMD ["python", "-m", "consumer_service.main"]

# Recommendation API
FROM python:3.10-slim AS recommendation
WORKDIR /app
COPY --from=builder /app /app
ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8002
CMD ["uvicorn", "recommendation_api.main:app", "--host", "0.0.0.0", "--port", "8002"]
