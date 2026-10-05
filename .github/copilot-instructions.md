# MistCircuitStats-Redis agent instructions

This file holds the rules that apply to MistCircuitStats-Redis only. The rules that apply to each
repository of this owner are in `AGENTS.md` at the repository root. Read `AGENTS.md` first. This
file adds to it, and it does not hold a copy of a rule from it. Where the two files disagree, obey
`AGENTS.md` for a writing rule, a safety rule, or a security rule.

## What this repository is

This Python 3.13 dashboard shows WAN port statistics for gateways that use Juniper Mist, VPN paths, and traffic charts. Flask serves cached Redis data to network operators. The dashboard uses vanilla JavaScript. Run it with Docker Compose on a Docker host.

## Language and environment

Use Python 3.13, pip, and Docker Compose. Make a local environment with:

```sh
python3.13 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-dev.txt
```

The development dependencies include the runtime requirements. The container image installs only `requirements.txt`.
The `mistapi` SDK requires `python-dotenv` version 1.1.0 or above. The Python Redis client uses `redis>=8.1.0,<9`.

## Local gates

Run the same gates that the `Quality Gates` workflow runs. Set the Radon maximum complexity to 15.

| Gate | Command | Expected result |
| - | - | - |
| Compile | `python -m compileall -q app.py mist_connection.py redis_cache.py worker.py tests docs` | No syntax errors |
| Ruff | `ruff check .` | No lint findings |
| Black | `black --check .` | No formatting changes |
| mypy | `mypy app.py mist_connection.py redis_cache.py worker.py` | No type errors |
| pytest | `python -m pytest tests` | All tests pass |
| Bandit | `bandit -r . -ll -x ./.github,./templates` | No findings of high or critical severity |
| pip-audit | `pip-audit -r requirements.txt` | No known dependency findings |
| Radon | `radon cc . -j \| complexity-gate --max 15` | No block exceeds 15 |
| Vulture | `vulture . --min-confidence 90 --exclude .github,templates,.venv,.venv-ste` | No dead code findings |
| STE | `ste-linter --config .ste-linter.toml --min-score 80 README.md AGENTS.md .github/copilot-instructions.md` | Each file scores at least 80 |

For Radon, use `set -o pipefail` in a shell that supports it. Put `.venv-ste` in your home folder, or make Vulture skip it.

## Architecture and conventions

Compose starts Redis, the Mist data worker, and the Flask app. The worker fetches Mist data and writes it to Redis. The web app reads the cache and does not call Mist APIs. `app.py` holds the Flask app when Python loads the module. This module does not use an app factory.

The worker reads `MIST_API_TOKEN` or the legacy `MIST_APITOKEN` variable. It accepts `MIST_ORG_ID`, `MIST_HOST`, `WORKER_INTERVAL`, `LOG_LEVEL`, `SKIP_ENRICHMENT`, `PARALLEL_WORKERS`, `USE_CACHED_ON_STARTUP`, `INCREMENTAL_REFRESH`, and `STALE_THRESHOLD`. The Mist host defaults to `api.mist.com`. The worker interval defaults to 300 seconds. The worker uses 4 threads by default, and the stale threshold defaults to 600 seconds. Production Compose sets 5 worker threads.

The Flask app reads `REDIS_URL`, `LOG_LEVEL`, `PORT`, and `APP_HOST`. The Redis URL defaults to `redis://localhost:6379`. The web port defaults to 5000, and the host defaults to `0.0.0.0`. `GET /health` checks Redis. `mist_connection.py` reads `API_DELAY_MS`, which defaults to 2 milliseconds.

The worker sets a 31-day TTL for the data that it writes. The cache class has its own 900-second default for calls without a TTL. Common Redis keys include `mist:org`, `mist:sites`, `mist:gateways`, `mist:vpn_peers:{gateway_id}-{mac}`, `mist:insights:{gateway_id}:{port_id}`, `mist:templates:{template_id}`, `mist:metadata:last_update`, `mist:metadata:worker_status`, and `mist:metadata:rate_limit`.

The worker uses the `mistapi` SDK for most Mist requests. SDK responses have `.status_code` and `.data`. The SDK session can change API tokens after a 429 response. Gateway insight statistics use `requests` with `/api/v1/sites/{site_id}/insights/gateway/{gateway_id}/stats`.

The frontend shows if a port uses a different type from its gateway template. It reads the site's `gatewaytemplate_id`. It reads `template.port_config[port_name].ip_config.type` as the expected type. It compares that type with the runtime port data. Keep this logic in `templates/index.html`.

Keep the layout for iPad landscape. The template has a media rule for landscape screens with a width of 1024 pixels or more.

| File | Purpose |
| - | - |
| `app.py` | Flask routes and cached-data responses |
| `worker.py` | Scheduled Mist data collection |
| `redis_cache.py` | Redis keys, values, and cache operations |
| `mist_connection.py` | Mist SDK and REST requests |
| `templates/index.html` | Dashboard and port override display |
| `docker-compose.yml` | Production Redis, worker, and web services |
| `docker-compose.dev.yml` | Local development services |
| `tests/` | Offline application and worker tests |

This repository has no context file for Spec Kit. Do not put generated context in `AGENTS.md`, this file, or `CLAUDE.md`.

## Safety in this repository

Warning: `RedisCache.clear_all` can cause loss of Redis cache data. Get typed confirmation before you call it. No Flask endpoint or code caller uses this method.

## Containers and ports

Use `docker-compose.yml` as the production Compose group. Production Redis uses the `redis:8-alpine` image, the `redis_data` volume, and AOF. Redis writes an RDB snapshot after one change in 60 seconds or 10 changes in 300 seconds. The development Compose file sets AOF and no explicit RDB save intervals.

Compose files set fixed container names. Standard tests do not start containers. If a container test is necessary, use a temporary Compose file. Set its project name to `mist-<issue-or-pr>-test`. Set each container name to `mist-<issue-or-pr>-<service>-test`.

Use host ports 5060 through 5099 for test containers. Do not publish local stack ports 5000 or 6379.

Remove the test project with `docker compose -p mist-<issue-or-pr>-test -f docker-compose.dev.yml -f <temporary-compose.yml> down --volumes --remove-orphans`. This command removes the test containers, volumes, and network.

| Port | Owner |
| - | - |
| 5000 | Flask web service |
| 6379 | Redis service |
| 5055 | Offline documentation preview |

The Dockerfile sets `appuser` as the user. Keep `--no-control-socket` in the production Gunicorn command. Gunicorn 26.2.0 fails if it tries to write its default control socket in the home directory of `appuser`.

## Git and GitHub in this repository

The `Quality Gates` workflow runs Ruff, Black, mypy, pytest, Bandit, pip-audit, Radon, and Vulture. The `STE lint` workflow grades `README.md`, `AGENTS.md`, and this file. The `Build and Push Multi-Arch Container` workflow builds on pushes to `main`, version tags, and manual runs. Its default platforms are `linux/amd64` and `linux/arm64`. The `Stranded Branch Report` workflow runs each Monday. The `CodeQL` workflow examines the Python code and sends the alerts to the Security tab.

| Workflow file | Workflow name | Runs on |
| - | - | - |
| `quality-gates.yml` | `Quality Gates` | Pull requests, pushes to `main`, and manual runs |
| `ste-lint.yml` | `STE lint` | Pull requests, pushes to `main`, and manual runs |
| `codeql.yml` | `CodeQL` | Pull requests, pushes to `main`, each Monday, and manual runs |
| `build-and-push.yml` | `Build and Push Multi-Arch Container` | Pushes to `main`, version tags, and manual runs |
| `stranded-branch-report.yml` | `Stranded Branch Report` | Each Monday and manual runs |

Each workflow calls a shared workflow of misthelper-devtools. Each pin names release v0.6.2 at commit `da02d4c6`. The `requirements-dev.txt` pin names the same commit. Change each pin and its release comment together, then run `devtools-pin-check`.

Branch protection on `main` requires the `gates / ...` checks of the `Quality Gates` workflow. The `CodeQL` workflow reports the `codeql / Analyze (python)` check and the `CodeQL` code scanning check. The owner adds these two checks to the required list after the first green CodeQL run on `main`. Do not change branch protection yourself.

Use `YY.MM.DD.HH.MM` in UTC for release tags. The repository uses `ci` and `documentation` labels. It has no separate issue type or scope label scheme, pull request template, changelog, or `auto-merge` label.

Issue #18 added the Radon gate with a maximum complexity of 15. Pull request #21 added `--no-control-socket` after Gunicorn failed to write its default socket as `appuser`. Pull request #23 reduced complexity in `worker.py`.

## Known pitfalls

- Keep the production Gunicorn flag `--no-control-socket`. The user `appuser` cannot write the default socket to its home directory (#21).
- Keep the Radon maximum at 15. The gate measures cyclomatic complexity (#18, #23).
- The worker's Compose token mapping reads `MIST_APITOKEN`, but `.env.example` names `MIST_API_TOKEN`. Compare these names when you change token settings.

## External resources

- [Mist API documentation](https://www.juniper.net/documentation/us/en/software/mist/api/http/getting-started/how-to-get-started)
- [mistapi Python SDK](https://github.com/tmunzer/mistapi_python)
- [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)
- [Bootstrap 5.3 documentation](https://getbootstrap.com/docs/5.3/)
- [Chart.js documentation](https://www.chartjs.org/docs/latest/)
