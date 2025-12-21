"""
MistCircuitStats-Redis - Flask Web Application

Read-only web frontend that serves data from Redis cache.
Does NOT make any Mist API calls - all data comes from the background worker.
"""

import os
import logging
from flask import Flask, render_template, jsonify, request
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from redis_cache import RedisCache

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

@app.route('/')
def index():
    """Serve main dashboard page"""
    return render_template('index.html')


# ==================== API Routes ====================

@app.route('/api/status')
def api_status():
    """Get cache and worker status including loading phase"""
    try:
        c = get_cache()
        stats = c.get_cache_stats()
        loading_phase = c.get_loading_phase()
        return jsonify({
            'success': True,
            'data': {
                'cache_valid': c.is_cache_valid(),
                'stats': stats,
                'loading_phase': loading_phase
            }
        })
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/organization')
def api_organization():
    """Get organization info from cache"""
    try:
        c = get_cache()
        org = c.get_organization()
        if org:
            return jsonify({'success': True, 'data': org})
        else:
            return jsonify({'success': False, 'error': 'No organization data in cache'}), 404
    except Exception as e:
        logger.error(f"Error getting organization: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/sites')
def api_sites():
    """Get sites list from cache"""
    try:
        c = get_cache()
        sites = c.get_sites()
        if sites is not None:
            return jsonify({'success': True, 'data': sites})
        else:
            return jsonify({'success': False, 'error': 'No sites data in cache'}), 404
    except Exception as e:
        logger.error(f"Error getting sites: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/cache-stats')
def api_cache_stats():
    """Get cache statistics with counts from Redis"""
    try:
        c = get_cache()
        stats = c.get_cache_stats()
        return jsonify({'success': True, 'data': stats})
    except Exception as e:
        logger.error(f"Error getting cache stats: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/gateways')
def api_gateways():
    """Get all gateway data from cache"""
    try:
        c = get_cache()
        gateways = c.get_gateways()
        
        if gateways is not None:
            # Optional site filter
            site_id = request.args.get('site_id')
            if site_id:
                gateways = [gw for gw in gateways if gw.get('site_id') == site_id]
            
            return jsonify({'success': True, 'data': gateways})
        else:
            return jsonify({
                'success': False, 
                'error': 'No gateway data in cache. Please wait for the worker to populate data.'
            }), 404
    except Exception as e:
        logger.error(f"Error getting gateways: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/vpn-peers/<gateway_id>/<mac>')
def api_vpn_peers(gateway_id, mac):
    """Get VPN peer paths for a specific gateway from cache"""
    try:
        c = get_cache()
        peers = c.get_vpn_peers(gateway_id, mac)
        if peers is not None:
            return jsonify({'success': True, 'data': peers})
        else:
            return jsonify({'success': True, 'data': {}})  # Empty is OK
    except Exception as e:
        logger.error(f"Error getting VPN peers: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/vpn-peers/all')
def api_all_vpn_peers():
    """Get all VPN peer paths from cache"""
    try:
        c = get_cache()
        all_peers = c.get_all_vpn_peers()
        return jsonify({'success': True, 'data': all_peers})
    except Exception as e:
        logger.error(f"Error getting all VPN peers: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/insights/<gateway_id>/<path:port_id>')
def api_insights(gateway_id, port_id):
    """Get traffic insights for a specific port from cache"""
    try:
        from urllib.parse import unquote
        port_id = unquote(port_id)
        
        c = get_cache()
        insights = c.get_insights(gateway_id, port_id)
        if insights is not None:
            return jsonify({'success': True, 'data': insights})
        else:
            return jsonify({'success': True, 'data': {}})  # Empty is OK
    except Exception as e:
        logger.error(f"Error getting insights: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/insights/all')
def api_all_insights():
    """Get all traffic insights from cache"""
    try:
        c = get_cache()
        all_insights = c.get_all_insights()
        return jsonify({'success': True, 'data': all_insights})
    except Exception as e:
        logger.error(f"Error getting all insights: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/gateway/<gateway_id>/port/<path:port_id>/traffic')
def api_port_traffic(gateway_id, port_id):
    """Get traffic time-series for chart display"""
    try:
        from urllib.parse import unquote
        port_id = unquote(port_id)
        
        c = get_cache()
        insights = c.get_insights(gateway_id, port_id)
        
        if insights:
            return jsonify({
                'success': True,
                'data': {
                    'timestamps': insights.get('timestamps', []),
                    'rx_bps': insights.get('rx_bps', []),
                    'tx_bps': insights.get('tx_bps', []),
                    'rx_bytes': insights.get('rx_bytes', 0),
                    'tx_bytes': insights.get('tx_bytes', 0)
                }
            })
        else:
            return jsonify({
                'success': True, 
                'data': {'timestamps': [], 'rx_bps': [], 'tx_bps': [], 'rx_bytes': 0, 'tx_bytes': 0}
            })
    except Exception as e:
        logger.error(f"Error getting port traffic: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/token-status')
def api_token_status():
    """Get cache status (no actual tokens used in this app)"""
    try:
        c = get_cache()
        worker_status = c.get_worker_status()
        last_update = c.get_last_update()
        
        return jsonify({
            'success': True,
            'data': {
                'mode': 'redis-cache',
                'worker_status': worker_status,
                'last_update': last_update,
                'cache_valid': c.is_cache_valid()
            }
        })
    except Exception as e:
        logger.error(f"Error getting token status: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500



@app.route('/api/templates')
def api_templates():
    """Get all gateway templates from cache"""
    try:
        c = get_cache()
        templates = c.get_all_gateway_templates()
        return jsonify({'success': True, 'data': templates})
    except Exception as e:
        logger.error(f"Error getting templates: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/health')
def health_check():
    """Health check endpoint for container orchestration"""
    try:
        c = get_cache()
        redis_ok = c.client.ping()
        cache_valid = c.is_cache_valid()
        
        status = 'healthy' if redis_ok else 'unhealthy'
        code = 200 if redis_ok else 503
        
        return jsonify({
            'status': status,
            'redis': 'connected' if redis_ok else 'disconnected',
            'cache_valid': cache_valid,
            'mode': 'redis-cache'
        }), code
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 503


# ==================== Main ====================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"Starting MistCircuitStats-Redis on port {port}")
    logger.info("This app reads from Redis cache only - no Mist API calls")
    app.run(host='0.0.0.0', port=port, debug=(LOG_LEVEL == 'DEBUG'))
