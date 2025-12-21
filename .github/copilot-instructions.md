# MistCircuitStats-Redis - Copilot Instructions

## Project Overview
Redis-cached Flask web application for displaying Juniper Mist Gateway WAN port statistics. 
This version separates API fetching (worker) from serving (web app) to support multiple concurrent users without consuming additional API tokens.

## Key Architecture Patterns
- **Three-tier architecture**: Redis cache, Background worker, Flask web app
- **Worker**: Only component that consumes Mist API tokens (runs on schedule)
- **Web App**: Read-only, serves cached data from Redis (instant responses)
- **Redis**: AOF persistence for data durability across restarts
- Flask with application factory pattern
- mistapi SDK for Mist API integration
- Bootstrap 5.3.2 dark theme with T-Mobile magenta accent (#E20074)
- Single-page application with vanilla JavaScript
- Multi-architecture Docker containers (amd64/arm64)

## Core Components
- `app.py` - Flask web app (reads from Redis only, no API calls)
- `worker.py` - Background data fetcher (only component using API tokens)
- `redis_cache.py` - Redis cache manager with TTL support
- `mist_connection.py` - Mist API connection wrapper with multi-token rotation

## Environment Variables
- MIST_API_TOKEN (required): Comma-separated Mist API tokens (worker only)
- MIST_ORG_ID (optional): Auto-detected from token
- MIST_HOST (default: api.mist.com)
- REDIS_URL (default: redis://localhost:6379)
- WORKER_INTERVAL (default: 300): Data refresh interval in seconds
- PORT (default: 5000)
- LOG_LEVEL (default: INFO)

## Redis Cache Keys
- `mist:org` - Organization info
- `mist:sites` - Sites list
- `mist:gateways` - All gateway data
- `mist:vpn_peers:{gateway_id}-{mac}` - VPN peer paths per gateway
- `mist:insights:{gateway_id}:{port_id}` - Traffic insights per port
- `mist:metadata:last_update` - Last worker update timestamp
- `mist:metadata:worker_status` - Worker status (idle/running/error)

## Development Guidelines
- Web app should NEVER make Mist API calls directly
- All API token usage is isolated to worker.py
- Cache TTL should be 3x the worker interval
- Non-root container user for security
- OCI labels with YY.MM.DD.HH.MM version format
- Health check endpoint at /health
- Responsive design optimized for iPad landscape

## Code Quality Guidelines
- Always check for linter issues before committing
- Run Pylance compile checks on all Python files
- Use type hints for function parameters and return values
- Handle Optional types properly (provide defaults or validate)
- Use proper error handling with specific error messages
- Validate environment variables before use

## mistapi SDK Usage (worker.py only)
- API responses have `.status_code` and `.data` attributes
- Check status_code == 200 for successful responses
- Multi-token support: SDK rotates tokens on 429 rate limit

## Mist API Endpoints Used

| API Endpoint | Method | Description |
|--------------|--------|-------------|
| `mistapi.api.v1.self.self.getSelf` | GET | Get current API token info and privileges |
| `mistapi.api.v1.orgs.orgs.getOrg` | GET | Get organization details |
| `mistapi.api.v1.orgs.sites.listOrgSites` | GET | List all sites in organization |
| `mistapi.api.v1.orgs.stats.listOrgDevicesStats` | GET | List gateway device statistics (type=gateway) |
| `mistapi.api.v1.orgs.stats.searchOrgSwOrGwPorts` | GET | Search WAN port statistics (paginated) |
| `/api/v1/orgs/{org_id}/stats/vpn_peers/search` | GET | VPN peer path statistics (per device) |
| `/api/v1/sites/{site_id}/insights/gateway/{device_id}/stats` | GET | Port-specific time-series traffic data |

> **Note**: VPN peers and Insights use direct HTTP requests (not SDK) due to SDK limitations

## Container Development Guidelines
- Docker Compose services: redis, worker, web
- Redis uses AOF persistence: `--appendonly yes --save 300 1`
- Worker depends on Redis health check
- Web app depends on both Redis and worker
- Build command: `docker-compose build`
- Test locally: `docker-compose -f docker-compose.dev.yml up`

## Dependency Management
- `mistapi` requires `python-dotenv>=0.15.0,<0.17` (not 1.0+)
- `redis>=5.0.0` for Python Redis client
- `schedule` for worker scheduling
- Always check for dependency conflicts when updating

## Git Repository Management
- Configure git user: `git config user.name` and `git config user.email`
- Use descriptive commit messages
- Container image names MUST be lowercase

## Release Management & Versioning
- Use YY.MM.DD.HH.MM format for version tags
- Create annotated tags: `git tag -a 25.12.19.12.00 -m "Release notes"`
- Push tags to trigger builds: `git push origin 25.12.19.12.00`

## CI/CD
- GitHub Actions builds on push to main and v* tags
- Publishes to ghcr.io
- Multi-arch builds: linux/amd64, linux/arm64
- Three container images: redis (official), worker, web
