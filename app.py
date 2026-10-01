"""
MistCircuitStats-Redis - Flask Web Application

Read-only web frontend that serves data from Redis cache.
Does NOT make any Mist API calls - all data comes from the background worker.
"""

import logging
import os

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request
from redis.exceptions import RedisError

from redis_cache import RedisCache

# Load environment variables
load_dotenv()

# Configure logging
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

APP_ERRORS = (RedisError, RuntimeError, TypeError, ValueError, KeyError, AttributeError)

app = Flask(__name__)

# Initialize Redis cache (read-only for this app)
cache = None


def get_cache():
    """Get or create Redis cache connection"""
    global cache
    if cache is None:
        cache = RedisCache()
    return cache


# ==================== Frontend Routes ====================


@app.route("/")
def index():
    """Serve main dashboard page"""
    return render_template("index.html")


# ==================== API Routes ====================


@app.route("/api/status")
def api_status():
    """Get cache and worker status including loading phase"""
    try:
        c = get_cache()
        stats = c.get_cache_stats()
        loading_phase = c.get_loading_phase()
        return jsonify(
            {
                "success": True,
                "data": {
                    "cache_valid": c.is_cache_valid(),
                    "stats": stats,
                    "loading_phase": loading_phase,
                },
            }
        )
    except APP_ERRORS as e:
        logger.error("Error getting status: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/organization")
def api_organization():
    """Get organization info from cache"""
    try:
        c = get_cache()
        org = c.get_organization()
        if org:
            return jsonify({"success": True, "data": org})
        else:
            return (
                jsonify({"success": False, "error": "No organization data in cache"}),
                404,
            )
    except APP_ERRORS as e:
        logger.error("Error getting organization: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/sites")
def api_sites():
    """Get sites list from cache"""
    try:
        c = get_cache()
        sites = c.get_sites()
        if sites is not None:
            return jsonify({"success": True, "data": sites})
        else:
            return jsonify({"success": False, "error": "No sites data in cache"}), 404
    except APP_ERRORS as e:
        logger.error("Error getting sites: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/cache-stats")
def api_cache_stats():
    """Get cache statistics with counts from Redis"""
    try:
        c = get_cache()
        stats = c.get_cache_stats()
        return jsonify({"success": True, "data": stats})
    except APP_ERRORS as e:
        logger.error("Error getting cache stats: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/gateways")
def api_gateways():
    """Get all gateway data from cache"""
    try:
        c = get_cache()
        gateways = c.get_gateways()

        if gateways is not None:
            # Optional site filter
            site_id = request.args.get("site_id")
            if site_id:
                gateways = [gw for gw in gateways if gw.get("site_id") == site_id]

            return jsonify({"success": True, "data": gateways})
        else:
            return (
                jsonify(
                    {
                        "success": False,
                        "error": "No gateway data in cache. Please wait for the worker to populate data.",
                    }
                ),
                404,
            )
    except APP_ERRORS as e:
        logger.error("Error getting gateways: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/vpn-peers/<gateway_id>/<mac>")
def api_vpn_peers(gateway_id, mac):
    """Get VPN peer paths for a specific gateway from cache"""
    try:
        c = get_cache()
        peers = c.get_vpn_peers(gateway_id, mac)
        if peers is not None:
            return jsonify({"success": True, "data": peers})
        else:
            return jsonify({"success": True, "data": {}})  # Empty is OK
    except APP_ERRORS as e:
        logger.error("Error getting VPN peers: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/vpn-peers/all")
def api_all_vpn_peers():
    """Get all VPN peer paths from cache"""
    try:
        c = get_cache()
        all_peers = c.get_all_vpn_peers()
        return jsonify({"success": True, "data": all_peers})
    except APP_ERRORS as e:
        logger.error("Error getting all VPN peers: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/insights/<gateway_id>/<path:port_id>")
def api_insights(gateway_id, port_id):
    """Get traffic insights for a specific port from cache"""
    try:
        from urllib.parse import unquote

        port_id = unquote(port_id)

        c = get_cache()
        insights = c.get_insights(gateway_id, port_id)
        if insights is not None:
            return jsonify({"success": True, "data": insights})
        else:
            return jsonify({"success": True, "data": {}})  # Empty is OK
    except APP_ERRORS as e:
        logger.error("Error getting insights: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/insights/all")
def api_all_insights():
    """Get all traffic insights from cache"""
    try:
        c = get_cache()
        all_insights = c.get_all_insights()
        return jsonify({"success": True, "data": all_insights})
    except APP_ERRORS as e:
        logger.error("Error getting all insights: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/gateway/<gateway_id>/port/<path:port_id>/traffic")
def api_port_traffic(gateway_id, port_id):
    """Get traffic time-series for chart display.

    Multi-resolution support: Returns data at the appropriate resolution based on duration.

    Query parameters:
        duration: Time range to return - '1h', '6h', '1d', '7d' (default: '7d')
                  Each duration returns data at the optimal resolution for that timeframe.

    Resolution mapping:
        - 1h: 1-minute intervals (~60 points)
        - 6h: 2-minute intervals (~180 points)
        - 1d: 10-minute intervals (~144 points)
        - 7d: 1-hour intervals (~168 points)
    """
    try:
        from urllib.parse import unquote

        port_id = unquote(port_id)
        duration = request.args.get("duration", "7d")

        # Map duration to resolution key
        resolution_map = {
            "1h": "1h",
            "6h": "6h",
            "1d": "1d",
            "24h": "1d",  # Alias
            "7d": "7d",
            "30d": "7d",  # Use 7d resolution for 30d requests (filter client-side if needed)
        }
        resolution = resolution_map.get(duration, "7d")

        c = get_cache()

        # Try to get resolution-specific data first
        insights = c.get_insights_by_resolution(gateway_id, port_id, resolution)

        # Fall back to default insights if resolution-specific not available
        if not insights:
            insights = c.get_insights(gateway_id, port_id)

        if not insights:
            return jsonify(
                {
                    "success": True,
                    "data": {
                        "timestamps": [],
                        "rx_bps": [],
                        "tx_bps": [],
                        "rx_bytes": 0,
                        "tx_bytes": 0,
                        "resolution": resolution,
                        "interval": 0,
                    },
                }
            )

        timestamps = insights.get("timestamps", [])
        rx_bps = insights.get("rx_bps", [])
        tx_bps = insights.get("tx_bps", [])
        interval = insights.get("interval", 600)
        rx_bytes = insights.get("rx_bytes", 0)
        tx_bytes = insights.get("tx_bytes", 0)

        return jsonify(
            {
                "success": True,
                "data": {
                    "timestamps": timestamps,
                    "rx_bps": rx_bps,
                    "tx_bps": tx_bps,
                    "rx_bytes": rx_bytes,
                    "tx_bytes": tx_bytes,
                    "resolution": resolution,
                    "interval": interval,
                },
            }
        )
    except APP_ERRORS as e:
        logger.error("Error getting port traffic: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/token-status")
def api_token_status():
    """Get cache status (no actual tokens used in this app)"""
    try:
        c = get_cache()
        worker_status = c.get_worker_status()
        last_update = c.get_last_update()
        rate_limit_status = c.get_rate_limit_status()

        return jsonify(
            {
                "success": True,
                "data": {
                    "mode": "redis-cache",
                    "worker_status": worker_status,
                    "last_update": last_update,
                    "cache_valid": c.is_cache_valid(),
                    "rate_limit": rate_limit_status,
                },
            }
        )
    except APP_ERRORS as e:
        logger.error("Error getting token status: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/templates")
def api_templates():
    """Get all gateway templates from cache"""
    try:
        c = get_cache()
        templates = c.get_all_gateway_templates()
        return jsonify({"success": True, "data": templates})
    except APP_ERRORS as e:
        logger.error("Error getting templates: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/health")
def health_check():
    """Health check endpoint for container orchestration"""
    try:
        c = get_cache()
        redis_ok = c.client.ping()
        cache_valid = c.is_cache_valid()

        status = "healthy" if redis_ok else "unhealthy"
        code = 200 if redis_ok else 503

        return (
            jsonify(
                {
                    "status": status,
                    "redis": "connected" if redis_ok else "disconnected",
                    "cache_valid": cache_valid,
                    "mode": "redis-cache",
                }
            ),
            code,
        )
    except APP_ERRORS as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 503


# ==================== Main ====================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    logger.info("Starting MistCircuitStats-Redis on port %s", port)
    logger.info("This app reads from Redis cache only - no Mist API calls")
    # The container must listen on all interfaces by default.
    host = os.environ.get("APP_HOST", "0.0.0.0")  # nosec B104
    app.run(host=host, port=port, debug=(LOG_LEVEL == "DEBUG"))
