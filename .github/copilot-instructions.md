# MistCircuitStats-Redis

A Redis-backed version of MistCircuitStats for multi-user scalability.

## Architecture

- **Flask Web App**: Read-only frontend that queries Redis cache
- **Background Worker**: Fetches data from Mist API on a schedule and stores in Redis
- **Redis**: Caches gateway stats, VPN peer paths, and traffic insights

## Environment Variables

- `MIST_API_TOKEN`: Comma-separated list of Mist API tokens
- `MIST_ORG_ID`: Mist organization ID (optional, auto-detected)
- `MIST_HOST`: Mist API host (default: api.mist.com)
- `REDIS_URL`: Redis connection URL (default: redis://localhost:6379)
- `WORKER_INTERVAL`: Data refresh interval in seconds (default: 300)
- `LOG_LEVEL`: Logging level (default: INFO)

## Development

```bash
# Start Redis
docker-compose up redis -d

# Run worker
python worker.py

# Run web app
python app.py
```

## Production

```bash
docker-compose up -d
```
