# MistCircuitStats-Redis

A Redis-cached version of MistCircuitStats that supports multiple concurrent users without consuming additional API tokens.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Browser   │────▶│   Flask     │────▶│   Redis     │
│   (Users)   │◀────│   Web App   │◀────│   Cache     │
└─────────────┘     └─────────────┘     └─────────────┘
                                              ▲
                           ┌──────────────────┘
                           │
                    ┌──────┴──────┐
                    │  Background │────▶ Mist API
                    │   Worker    │
                    └─────────────┘
```

### Components

- **Redis**: Caches all gateway data, VPN peer paths, and traffic insights with configurable TTL
- **Worker**: Background process that periodically fetches data from Mist API and populates Redis
- **Web App**: Read-only Flask application that serves data from Redis cache

### Benefits

- **Token Efficiency**: Only the worker consumes API tokens, regardless of user count
- **Fast Responses**: Redis provides sub-millisecond response times
- **Scalable**: Multiple web app replicas can share the same Redis instance
- **Resilient**: Data remains available even during brief Mist API outages

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Mist API Token(s)

### 1. Clone and Configure

```bash
git clone https://github.com/yourusername/MistCircuitStats-Redis.git
cd MistCircuitStats-Redis

# Copy example environment file
cp .env.example .env

# Edit .env with your Mist API token(s)
nano .env
```

### 2. Run with Docker Compose

```bash
# Production mode
docker-compose up -d

# View logs
docker-compose logs -f

# Development mode (with hot reload)
docker-compose -f docker-compose.dev.yml up
```

### 3. Access the Dashboard

Open http://localhost:5000 in your browser.

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MIST_API_TOKEN` | Comma-separated list of Mist API tokens | Required |
| `MIST_ORG_ID` | Mist Organization ID (auto-detected if not set) | Auto |
| `MIST_HOST` | Mist API hostname | `api.mist.com` |
| `REDIS_URL` | Redis connection URL | `redis://localhost:6379` |
| `WORKER_INTERVAL` | Data refresh interval in seconds | `300` (5 min) |
| `LOG_LEVEL` | Logging level (DEBUG, INFO, WARNING, ERROR) | `INFO` |

### Multiple API Tokens

For large organizations, use multiple API tokens to avoid rate limiting:

```env
MIST_API_TOKEN=token1,token2,token3
```

The worker will automatically rotate between tokens when rate limited.

## Development

### Local Development (without Docker)

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Start Redis (using Docker)
docker run -d -p 6379:6379 redis:7-alpine

# Copy and configure environment
cp .env.example .env
# Edit .env with your settings

# Run worker in one terminal
python worker.py

# Run web app in another terminal
python app.py
```

### Project Structure

```
MistCircuitStats-Redis/
├── app.py              # Flask web application (read-only)
├── worker.py           # Background data fetcher
├── redis_cache.py      # Redis cache manager
├── mist_connection.py  # Mist API connection handler
├── templates/
│   └── index.html      # Dashboard template
├── docker-compose.yml  # Production Docker setup
├── docker-compose.dev.yml  # Development Docker setup
├── Dockerfile
├── requirements.txt
└── .env.example
```

## Data Flow

1. **Worker starts**: Fetches all data from Mist API
2. **Data cached**: Gateways, VPN peers, and insights stored in Redis with TTL
3. **Users connect**: Web app serves data directly from Redis (instant)
4. **Worker refreshes**: Every N seconds (default 5 min), worker updates cache
5. **Cache expires**: If worker fails, data expires after 3x refresh interval

## Redis Data Persistence

Data survives container restarts via Redis AOF (Append Only File):

```yaml
command: redis-server --appendonly yes --save 300 1
volumes:
  - redis_data:/data
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /` | Dashboard HTML |
| `GET /api/status` | Cache and worker status |
| `GET /api/organization` | Organization info |
| `GET /api/sites` | Sites list |
| `GET /api/gateways` | All gateway data |
| `GET /api/vpn-peers/all` | All VPN peer paths |
| `GET /api/insights/all` | All traffic insights |
| `GET /api/token-status` | Worker/cache status |

## License

MIT License - See [LICENSE](LICENSE) for details.
