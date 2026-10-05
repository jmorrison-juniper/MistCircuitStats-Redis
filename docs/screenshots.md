# Genuine offline UI screenshots

The four PNG files in [screenshots/](screenshots/) are browser captures of the
real Flask application and its unmodified `templates/index.html`. The cache
substitute in `sample_cache.py` contains fictional branches, reserved
documentation IP addresses, VPN metrics, and traffic samples. They demonstrate
the UI; they are not evidence of production circuit measurements.

No Mist tokens, worker, live Redis server, or production data are used. The
browser downloads the application's normal public Bootstrap and Chart.js CDN
assets; it does not contact Mist. Do not configure credentials for this preview.

## Reproduce

Use Python 3.13 and install the project's development requirements in `.venv`.
Install the optional screenshot tooling:

```bash
.venv/bin/python -m pip install 'playwright>=1.63,<2'
.venv/bin/python -m playwright install chromium
.venv/bin/python -m docs.capture_screenshots
```

The script starts a temporary loopback HTTP server on an available port, loads
the actual dashboard, expands WAN ports, searches for Seattle, opens the traffic
chart, switches to a one-hour view, and opens the VPN path modal. It checks
rendered results and browser errors, writes screenshots, and shuts down the
server. Screenshot images are not drawn or reconstructed.

For manual inspection, run `.venv/bin/python -m docs.sample_cache`, then browse
<http://127.0.0.1:5055>. Stop it with Ctrl-C when finished. This preview is only
for documentation, not a production configuration.
