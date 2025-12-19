"""
Background Worker for MistCircuitStats-Redis

Periodically fetches data from Mist API and stores in Redis cache.
This is the only component that consumes API tokens.
"""

import os
import sys
import time
import signal
import logging
import schedule
import traceback
from datetime import datetime
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

from mist_connection import MistConnection
from redis_cache import RedisCache


class DataWorker:
    """Background worker that fetches Mist data and populates Redis cache"""
    
    def __init__(self):
        """Initialize worker with Mist connection and Redis cache"""
        self.running = True
        self.mist = None
        self.cache = None
        
        # Configuration
        self.interval = int(os.environ.get('WORKER_INTERVAL', 300))  # Default 5 minutes
        self.cache_ttl = self.interval * 3  # TTL is 3x the refresh interval
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"Worker initialized with {self.interval}s refresh interval")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
    
    def connect(self) -> bool:
        """Establish connections to Mist API and Redis"""
        try:
            # Get API token from environment
            api_token = os.environ.get('MIST_API_TOKEN') or os.environ.get('MIST_APITOKEN')
            if not api_token:
                raise ValueError("MIST_API_TOKEN or MIST_APITOKEN environment variable is required")
            
            org_id = os.environ.get('MIST_ORG_ID')
            host = os.environ.get('MIST_HOST', 'api.mist.com')
            
            # Connect to Mist
            self.mist = MistConnection(api_token=api_token, org_id=org_id, host=host)
            logger.info(f"Connected to Mist API: {self.mist.host}")
            
            # Connect to Redis
            self.cache = RedisCache()
            logger.info("Connected to Redis")
            
            return True
        except Exception as e:
            logger.error(f"Failed to establish connections: {e}")
            return False
    
    def fetch_and_cache_all(self):
        """Fetch all data from Mist API and store in Redis progressively"""
        start_time = time.time()
        logger.info("=== Starting data refresh ===")
        
        try:
            self.cache.set_worker_status('running', {'started': datetime.now().isoformat()})
            
            # Phase 1: Organization and Sites
            self.cache.set_loading_phase('org_sites', 4, {'description': 'Fetching organization and sites...'})
            logger.info("Phase 1: Fetching organization and sites...")
            self._fetch_org_and_sites()
            
            # Phase 2: Gateway Stats - cache immediately so web can show data
            self.cache.set_loading_phase('gateways', 4, {'description': 'Fetching gateway statistics...'})
            logger.info("Phase 2: Fetching gateway statistics...")
            gateways = self._fetch_gateways()
            
            # Update timestamp after gateways so web knows data is available
            if gateways:
                self.cache.set_last_update(time.time())
            
            if gateways:
                # Phase 3: VPN Peer Paths
                self.cache.set_loading_phase('vpn_peers', 4, {'description': 'Fetching VPN peer paths...', 'gateways_loaded': len(gateways)})
                logger.info("Phase 3: Fetching VPN peer paths...")
                self._fetch_vpn_peers(gateways)
                
                # Phase 4: Traffic Insights
                self.cache.set_loading_phase('insights', 4, {'description': 'Fetching traffic insights...', 'gateways_loaded': len(gateways)})
                logger.info("Phase 4: Fetching traffic insights...")
                self._fetch_insights(gateways)
            
            # Mark as complete
            elapsed = time.time() - start_time
            self.cache.set_last_update(time.time())
            self.cache.set_loading_phase('complete', 4, {'description': 'All data loaded', 'duration_seconds': round(elapsed, 2)})
            self.cache.set_worker_status('idle', {
                'last_run': datetime.now().isoformat(),
                'duration_seconds': round(elapsed, 2),
                'gateways_count': len(gateways) if gateways else 0
            })
            
            logger.info(f"=== Data refresh complete in {elapsed:.1f}s ===")
            
        except Exception as e:
            logger.error(f"Error during data refresh: {e}")
            logger.error(traceback.format_exc())
            self.cache.set_worker_status('error', {
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
    
    def _fetch_org_and_sites(self):
        """Fetch organization info and sites list"""
        try:
            # Get organization info
            org_info = self.mist.get_organization_info()
            if org_info:
                self.cache.set_organization(org_info, ttl=self.cache_ttl)
                logger.info(f"Cached organization: {org_info.get('org_name')}")
            
            # Get sites
            sites = self.mist.get_sites()
            if sites:
                self.cache.set_sites(sites, ttl=self.cache_ttl)
                logger.info(f"Cached {len(sites)} sites")
                
        except Exception as e:
            logger.error(f"Error fetching org/sites: {e}")
    
    def _fetch_gateways(self):
        """Fetch all gateway statistics"""
        try:
            # Calculate time range (7 days)
            end = int(time.time())
            start = end - (7 * 24 * 60 * 60)
            
            gateways = self.mist.get_gateway_stats(start=start, end=end)
            
            if gateways:
                self.cache.set_gateways(gateways, ttl=self.cache_ttl)
                logger.info(f"Cached {len(gateways)} gateways")
                return gateways
            else:
                logger.warning("No gateways returned from API")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching gateways: {e}")
            return []
    
    def _fetch_vpn_peers(self, gateways: list):
        """Fetch VPN peer paths for all gateways"""
        try:
            all_peers = {}
            total = len(gateways)
            
            for i, gw in enumerate(gateways):
                gw_id = gw.get('id')
                gw_mac = gw.get('mac')
                
                if not gw_id or not gw_mac:
                    continue
                
                try:
                    peers = self.mist.get_vpn_peer_paths(gw_id)
                    if peers:
                        cache_key = f"{gw_id}-{gw_mac}"
                        all_peers[cache_key] = peers
                except Exception as e:
                    logger.warning(f"Error fetching VPN peers for {gw_id}: {e}")
                
                # Log progress every 50 gateways
                if (i + 1) % 50 == 0:
                    logger.info(f"VPN peers progress: {i + 1}/{total}")
            
            # Store all peers in one operation
            if all_peers:
                self.cache.set_all_vpn_peers(all_peers, ttl=self.cache_ttl)
            
            logger.info(f"Cached VPN peers for {len(all_peers)} gateways")
            
        except Exception as e:
            logger.error(f"Error in VPN peers fetch: {e}")
    
    def _fetch_insights(self, gateways: list):
        """Fetch traffic insights for all gateway ports"""
        try:
            import requests
            
            # Calculate time range (7 days)
            end = int(time.time())
            start = end - (7 * 24 * 60 * 60)
            interval = 3600  # 1 hour samples
            
            all_insights = {}
            total_ports = 0
            processed = 0
            
            # Count total ports
            for gw in gateways:
                total_ports += len(gw.get('ports', []))
            
            # Get current token for API calls
            current_token = self.mist.api_token.split(',')[0].strip()
            try:
                tokens = getattr(self.mist.apisession, '_apitoken', None)
                token_idx = getattr(self.mist.apisession, '_apitoken_index', 0)
                if tokens and len(tokens) > token_idx >= 0:
                    current_token = tokens[token_idx]
            except:
                pass
            
            headers = {
                'Authorization': f'Token {current_token}',
                'Content-Type': 'application/json'
            }
            
            for gw in gateways:
                site_id = gw.get('site_id')
                gateway_id = gw.get('id')
                ports = gw.get('ports', [])
                
                if not site_id or not gateway_id:
                    continue
                
                if gateway_id not in all_insights:
                    all_insights[gateway_id] = {}
                
                for port in ports:
                    port_id = port.get('name', '')
                    if not port_id:
                        continue
                    
                    try:
                        url = f'https://{self.mist.host}/api/v1/sites/{site_id}/insights/gateway/{gateway_id}/stats'
                        params = {
                            'interval': interval,
                            'start': start,
                            'end': end,
                            'port_id': port_id,
                            'metrics': 'rx_bps,tx_bps'
                        }
                        
                        response = requests.get(url, headers=headers, params=params, timeout=30)
                        
                        if response.status_code == 200:
                            data = response.json()
                            rx_bps_list = data.get('rx_bps', [])
                            tx_bps_list = data.get('tx_bps', [])
                            
                            # Calculate total bytes
                            rx_bytes = sum(bps * interval for bps in rx_bps_list if bps) // 8
                            tx_bytes = sum(bps * interval for bps in tx_bps_list if bps) // 8
                            
                            all_insights[gateway_id][port_id] = {
                                'rx_bytes': rx_bytes,
                                'tx_bytes': tx_bytes,
                                'rx_bps': rx_bps_list,
                                'tx_bps': tx_bps_list,
                                'timestamps': data.get('timestamps', [])
                            }
                        
                    except Exception as e:
                        logger.warning(f"Error fetching insights for {gateway_id}/{port_id}: {e}")
                    
                    processed += 1
                    
                    # Log progress every 100 ports
                    if processed % 100 == 0:
                        logger.info(f"Insights progress: {processed}/{total_ports}")
            
            # Store all insights
            if all_insights:
                self.cache.set_all_insights(all_insights, ttl=self.cache_ttl)
            
            logger.info(f"Cached insights for {processed} ports across {len(all_insights)} gateways")
            
        except Exception as e:
            logger.error(f"Error in insights fetch: {e}")
    
    def run(self):
        """Main worker loop"""
        if not self.connect():
            logger.error("Failed to connect, exiting")
            sys.exit(1)
        
        # Initial fetch
        self.fetch_and_cache_all()
        
        # Schedule periodic updates
        schedule.every(self.interval).seconds.do(self.fetch_and_cache_all)
        
        logger.info(f"Worker running, refreshing every {self.interval}s")
        
        # Main loop
        while self.running:
            schedule.run_pending()
            time.sleep(1)
        
        logger.info("Worker shutdown complete")


if __name__ == '__main__':
    worker = DataWorker()
    worker.run()
