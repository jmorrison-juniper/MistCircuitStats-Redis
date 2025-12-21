# syntax=docker/dockerfile:1
FROM python:3.13-slim

# Labels
LABEL org.opencontainers.image.title="MistCircuitStats-Redis"
LABEL org.opencontainers.image.description="Redis-cached Mist Gateway WAN Statistics"
LABEL org.opencontainers.image.source="https://github.com/jmorrison-juniper/MistCircuitStats-Redis"

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Create non-root user
RUN groupadd -r appgroup && useradd -r -g appgroup appuser

WORKDIR /app

# Install curl for health checks
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app.py worker.py mist_connection.py redis_cache.py ./
COPY templates/ templates/

# Change ownership to non-root user
RUN chown -R appuser:appgroup /app

# Switch to non-root user
USER appuser

# Default command (override in docker-compose)
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "2", "--threads", "4", "app:app"]
