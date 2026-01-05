"""
Background Worker for MistCircuitStats-Redis

Periodically fetches data from Mist API and stores in Redis cache.
This is the only component that consumes API tokens.

Optimized for large organizations:
- Phase 1: Quick load (org, sites, basic gateway list) - available immediately
- Phase 2: Port stats - fetched in batches with incremental caching
- Phase 3: VPN peers and insights - optional enrichment
"""

import os
import sys
import time
import signal
import logging
import schedule
import traceback
import threading
from datetime import datetime
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, List, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, List, Any

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
        
        # TTL settings - data persists in Redis for 31 days regardless of refresh interval
        # This ensures cached data is always available even if worker hasn't run recently
        self.ttl_31_days = 31 * 24 * 60 * 60  # 31 days in seconds (2,678,400)
        
        # Different TTLs based on data volatility:
        # - Organization info: rarely changes (31 days)
        # - Sites: rarely changes (31 days)  
        # - Gateways/Ports: changes moderately (31 days, refreshed every 5 min)
        # - VPN Peers: changes frequently (31 days, refreshed every 5 min)
        # - Insights: time-series data (31 days, refreshed every 15 min)
        self.ttl_org = self.ttl_31_days
        self.ttl_sites = self.ttl_31_days
        self.ttl_gateways = self.ttl_31_days
        self.ttl_vpn = self.ttl_31_days
        self.ttl_insights = self.ttl_31_days
        
        # Legacy cache_ttl for any remaining uses (31 days)
        self.cache_ttl = self.ttl_31_days
        
        # Large org mode - skip VPN peers and insights
        self.skip_enrichment = os.environ.get('SKIP_ENRICHMENT', 'false').lower() == 'true'
        
        # Optimization settings
        self.use_cached_on_startup = os.environ.get('USE_CACHED_ON_STARTUP', 'true').lower() == 'true'
        self.parallel_workers = int(os.environ.get('PARALLEL_WORKERS', '4'))
        self.incremental_refresh_enabled = os.environ.get('INCREMENTAL_REFRESH', 'true').lower() == 'true'
        self.stale_threshold = int(os.environ.get('STALE_THRESHOLD', '600'))  # 10 min default
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"Worker initialized with {self.interval}s refresh interval")
        logger.info(f"Cache TTL: 31 days ({self.ttl_31_days}s) - data persists across restarts")
        logger.info(f"Parallel workers: {self.parallel_workers}, Incremental refresh: {self.incremental_refresh_enabled}")
        logger.info(f"Use cached on startup: {self.use_cached_on_startup}, Stale threshold: {self.stale_threshold}s")
        if self.skip_enrichment:
            logger.info("SKIP_ENRICHMENT mode enabled - skipping VPN peers and insights")
    
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
            
            # Connect to Redis first (needed for template caching in MistConnection)
            self.cache = RedisCache()
            logger.info("Connected to Redis")
            
            # Connect to Mist (pass Redis cache for template persistence)
            self.mist = MistConnection(api_token=api_token, org_id=org_id, host=host, redis_cache=self.cache)
            logger.info(f"Connected to Mist API: {self.mist.host}")
            
            return True
        except Exception as e:
            logger.error(f"Failed to establish connections: {e}")
            return False
    
    def fetch_and_cache_all(self):
        """
        Fetch all data from Mist API using TRUE PARALLEL workers.
        
        Concurrent data streams:
        - Worker 1: Org + Sites
        - Worker 2: Gateway device list  
        - Worker 3: Port stats (starts after gateways available)
        - Worker 4: VPN peers (starts after gateways available)
        - Worker 5: Insights (starts after ports available)
        """
        start_time = time.time()
        logger.info("=== Starting PARALLEL data refresh ===")
        
        try:
            self.cache.set_worker_status('running', {'started': datetime.now().isoformat(), 'mode': 'parallel'})
            
            # Shared state for coordination between workers
            gateways_ready = threading.Event()
            ports_ready = threading.Event()
            gateways_data = {'list': []}
            
            def fetch_org_sites():
                """Worker 1: Fetch org and sites"""
                try:
                    logger.info("[Worker-OrgSites] Starting...")
                    self._fetch_org_and_sites()
                    logger.info("[Worker-OrgSites] Complete")
                except Exception as e:
                    logger.error(f"[Worker-OrgSites] Error: {e}")
            
            def fetch_gateways():
                """Worker 2: Fetch gateway list"""
                try:
                    logger.info("[Worker-Gateways] Starting...")
                    gws = self._fetch_gateway_list_quick()
                    if gws:
                        gateways_data['list'] = gws
                        self.cache.set_last_update(time.time())
                        logger.info(f"[Worker-Gateways] Complete: {len(gws)} gateways")
                    gateways_ready.set()  # Signal other workers
                except Exception as e:
                    logger.error(f"[Worker-Gateways] Error: {e}")
                    gateways_ready.set()  # Signal even on error
            
            def fetch_ports():
                """Worker 3: Fetch port stats (waits for gateways)"""
                try:
                    gateways_ready.wait(timeout=120)  # Wait up to 2 min
                    gws = gateways_data['list']
                    if not gws:
                        logger.warning("[Worker-Ports] No gateways available")
                        ports_ready.set()
                        return
                    logger.info(f"[Worker-Ports] Starting for {len(gws)} gateways...")
                    self._fetch_port_stats_incremental(gws)
                    # Update shared gateways with port data
                    gateways_data['list'] = self.cache.get_gateways() or gws
                    logger.info("[Worker-Ports] Complete")
                    ports_ready.set()
                except Exception as e:
                    logger.error(f"[Worker-Ports] Error: {e}")
                    ports_ready.set()
            
            def fetch_vpn():
                """Worker 4: Fetch VPN peers (waits for gateways)"""
                try:
                    if self.skip_enrichment:
                        logger.info("[Worker-VPN] Skipped (SKIP_ENRICHMENT=true)")
                        return
                    gateways_ready.wait(timeout=120)
                    gws = gateways_data['list']
                    if not gws:
                        logger.warning("[Worker-VPN] No gateways available")
                        return
                    logger.info(f"[Worker-VPN] Starting parallel fetch for {len(gws)} gateways...")
                    self._fetch_vpn_peers_parallel(gws)
                    logger.info("[Worker-VPN] Complete")
                except Exception as e:
                    logger.error(f"[Worker-VPN] Error: {e}")
            
            def fetch_insights():
                """Worker 5: Fetch insights (waits for gateways - runs in parallel with ports)"""
                try:
                    if self.skip_enrichment:
                        logger.info("[Worker-Insights] Skipped (SKIP_ENRICHMENT=true)")
                        return
                    gateways_ready.wait(timeout=120)  # Only need gateway list, not full port stats
                    gws = gateways_data['list']
                    if not gws:
                        logger.warning("[Worker-Insights] No gateways available")
                        return
                    logger.info(f"[Worker-Insights] Starting parallel fetch...")
                    self._fetch_insights_parallel(gws)
                    logger.info("[Worker-Insights] Complete")
                except Exception as e:
                    logger.error(f"[Worker-Insights] Error: {e}")
            
            # Launch all workers in parallel
            with ThreadPoolExecutor(max_workers=5, thread_name_prefix='MistWorker') as executor:
                futures = {
                    executor.submit(fetch_org_sites): 'org_sites',
                    executor.submit(fetch_gateways): 'gateways',
                    executor.submit(fetch_ports): 'ports',
                    executor.submit(fetch_vpn): 'vpn',
                    executor.submit(fetch_insights): 'insights',
                }
                
                # Wait for all to complete
                for future in as_completed(futures):
                    task_name = futures[future]
                    try:
                        future.result()
                        logger.info(f"[Coordinator] {task_name} finished")
                    except Exception as e:
                        logger.error(f"[Coordinator] {task_name} failed: {e}")
            
            # ===== COMPLETE =====
            elapsed = time.time() - start_time
            gws = gateways_data['list']
            self.cache.set_last_update(time.time())
            self.cache.set_loading_phase('complete', 5, {
                'description': 'All data loaded (parallel)',
                'duration_seconds': round(elapsed, 2),
                'gateways_count': len(gws) if gws else 0
            })
            self.cache.set_worker_status('idle', {
                'last_run': datetime.now().isoformat(),
                'duration_seconds': round(elapsed, 2),
                'gateways_count': len(gws) if gws else 0,
                'mode': 'parallel'
            })
            
            logger.info(f"=== PARALLEL data refresh complete in {elapsed:.1f}s ===")
            
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
    
    def _fetch_gateway_list_quick(self):
        """
        Fetch basic gateway device list (FAST).
        This gets device info without port statistics.
        Preserves existing port data from cache if available.
        """
        try:
            new_gateways = self.mist.get_gateway_device_list()
            
            if new_gateways:
                # Get existing cached gateways to preserve port data
                cached_gateways = self.cache.get_gateways() or []
                cached_by_mac = {gw.get('mac'): gw for gw in cached_gateways}
                
                # Merge: use new gateway data but preserve cached ports if available
                for gw in new_gateways:
                    mac = gw.get('mac')
                    cached_gw = cached_by_mac.get(mac)
                    if cached_gw:
                        # Preserve port data and enrichment flags from cache
                        cached_ports = cached_gw.get('ports', [])
                        if cached_ports and len(cached_ports) > 0:
                            gw['ports'] = cached_ports
                            gw['num_ports'] = len(cached_ports)
                            gw['_basic_only'] = False
                            
                self.cache.set_gateways(new_gateways, ttl=self.cache_ttl)
                
                # Count how many have port data preserved
                with_ports = len([gw for gw in new_gateways if gw.get('ports')])
                logger.info(f"Cached {len(new_gateways)} gateways ({with_ports} with preserved port data)")
                return new_gateways
            else:
                logger.warning("No gateways returned from API")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching gateway list: {e}")
            return []
    
    def _fetch_port_stats_incremental(self, gateways: list):
        """
        Fetch port statistics per-site and update cached gateways incrementally.
        
        Instead of one massive org-level call, this fetches ports site-by-site
        so data is cached and available as each site completes.
        Also enriches ports with config data (WAN name, IP, gateway, type, override).
        """
        try:
            # Build gateway lookup by MAC
            gateway_by_mac = {gw.get('mac'): gw for gw in gateways if gw.get('mac')}
            
            # Get sites list from cache
            sites = self.cache.get_sites() or []
            if not sites:
                logger.warning("No sites in cache, falling back to org-level fetch")
                # Fallback to org-level if no sites
                ports_by_device = self.mist.get_port_stats_paginated()
                for mac, ports in ports_by_device.items():
                    if mac in gateway_by_mac:
                        gateway_by_mac[mac]['ports'] = ports
                        gateway_by_mac[mac]['_basic_only'] = False
                self.cache.set_gateways(gateways, ttl=self.cache_ttl)
                return
            
            # Pre-fetch inventory map for all gateways (used for enrichment)
            logger.info("Fetching inventory data for port enrichment...")
            gateway_macs = set(gateway_by_mac.keys())
            inventory_map = self.mist._batch_fetch_inventory(gateway_macs)
            logger.info(f"Got inventory data for {len(inventory_map)} gateways")
            
            total_sites = len(sites)
            total_ports = 0
            devices_with_ports = 0
            
            logger.info(f"Fetching port stats for {total_sites} sites (with config enrichment)...")
            
            for i, site in enumerate(sites):
                site_id = site.get('id')
                site_name = site.get('name', 'Unknown')
                
                if not site_id:
                    continue
                
                try:
                    # Fetch ports for this site using site-level API
                    site_ports = self.mist.get_site_port_stats(site_id)
                    
                    if site_ports:
                        # Group WAN ports by device MAC (filter out non-WAN ports)
                        site_ports_by_mac = {}
                        for port in site_ports:
                            # Only include WAN ports
                            if port.get('port_usage') != 'wan':
                                continue
                            mac = port.get('mac')
                            if mac:
                                if mac not in site_ports_by_mac:
                                    site_ports_by_mac[mac] = []
                                site_ports_by_mac[mac].append(port)
                        
                        # Enrich and assign to gateways
                        for mac, raw_ports in site_ports_by_mac.items():
                            if mac in gateway_by_mac:
                                gateway = gateway_by_mac[mac]
                                # Enrich ports with config data
                                enriched_ports = self.mist.enrich_gateway_ports(
                                    gateway, raw_ports, inventory_map
                                )
                                gateway['ports'] = enriched_ports
                                gateway['num_ports'] = len(enriched_ports)  # For frontend display
                                gateway['_basic_only'] = False
                                total_ports += len(enriched_ports)
                                devices_with_ports += 1
                        
                        # Incremental cache update after each site
                        self.cache.set_gateways(gateways, ttl=self.cache_ttl)
                        
                        logger.debug(f"Site {i+1}/{total_sites} '{site_name}': {len(site_ports)} ports")
                    
                except Exception as e:
                    logger.warning(f"Error fetching ports for site {site_name}: {e}")
                
                # Progress update every 10 sites
                if (i + 1) % 10 == 0 or i == total_sites - 1:
                    self.cache.set_loading_phase('ports', 5, {
                        'description': f'Sites {i+1}/{total_sites} - {total_ports} ports loaded',
                        'sites_processed': i + 1,
                        'total_sites': total_sites,
                        'ports_loaded': total_ports,
                        'devices_with_ports': devices_with_ports
                    })
                    logger.info(f"Port stats progress: {i+1}/{total_sites} sites, {total_ports} ports")
            
            logger.info(f"Completed: {total_ports} ports for {devices_with_ports} gateways across {total_sites} sites")
            
        except Exception as e:
            logger.error(f"Error fetching port stats: {e}")
    
    def _update_gateways_with_ports(self, gateways: list, gateway_by_mac: dict):
        """Update cached gateways with current port data"""
        try:
            self.cache.set_gateways(gateways, ttl=self.cache_ttl)
            self.cache.set_last_update(time.time())
        except Exception as e:
            logger.warning(f"Error updating gateways cache: {e}")
    
    def _fetch_gateways(self):
        """
        Fetch all gateway statistics (legacy method - uses full get_gateway_stats).
        Kept for compatibility but prefer _fetch_gateway_list_quick + _fetch_port_stats_incremental.
        """
        try:
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
                    self.cache.set_loading_phase('vpn_peers', 5, {
                        'description': f'Fetching VPN peer paths... ({i + 1}/{total})',
                        'progress': i + 1,
                        'total': total
                    })
            
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
                    port_id = port.get('name', port.get('port_id', ''))
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
                        self.cache.set_loading_phase('insights', 5, {
                            'description': f'Fetching traffic insights... ({processed}/{total_ports})',
                            'progress': processed,
                            'total': total_ports
                        })
            
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
        
        # Check cache on startup - skip refresh if valid
        if self.use_cached_on_startup and self.check_and_use_cache():
            logger.info("Using cached data, scheduling next refresh")
        else:
            # No valid cache, do full refresh
            self.fetch_and_cache_all()
        
        # Schedule periodic refresh (uses incremental when possible)
        schedule.every(self.interval).seconds.do(self.do_incremental_refresh)
        
        logger.info(f"Worker running, refreshing every {self.interval}s")
        
        # Main loop
        while self.running:
            schedule.run_pending()
            time.sleep(1)
        
        logger.info("Worker shutdown complete")

    # ========== OPTIMIZATION METHODS ==========
    
    def check_and_use_cache(self) -> bool:
        """
        Check if cache has valid data. If so, skip full refresh.
        Returns True if cache is valid and usable.
        """
        try:
            if not self.use_cached_on_startup:
                logger.info("USE_CACHED_ON_STARTUP disabled, will refresh")
                return False
            
            stats = self.cache.get_cache_stats()
            last_update = self.cache.get_last_update()
            
            if not last_update:
                logger.info("No cached data found, will do full refresh")
                return False
            
            age = time.time() - last_update
            gateways_count = stats.get('gateways', 0)
            
            if gateways_count > 0 and age < self.stale_threshold:
                logger.info(f"✓ Cache valid: {gateways_count} gateways, {age:.0f}s old (threshold: {self.stale_threshold}s)")
                self.cache.set_worker_status('idle', {
                    'status': 'using_cache',
                    'cache_age': round(age, 1),
                    'gateways_count': gateways_count,
                    'last_check': datetime.now().isoformat()
                })
                return True
            
            logger.info(f"Cache stale: {age:.0f}s old (threshold: {self.stale_threshold}s)")
            return False
            
        except Exception as e:
            logger.warning(f"Error checking cache: {e}")
            return False
    
    def do_incremental_refresh(self):
        """
        Incremental refresh - check cache first, only update if stale.
        Much faster than full refresh for regular updates.
        """
        try:
            if not self.incremental_refresh_enabled:
                logger.info("Incremental refresh disabled, doing full refresh")
                self.fetch_and_cache_all()
                return
            
            # Check if we have valid cached data
            if self.check_and_use_cache():
                logger.info("Cache is fresh, skipping refresh")
                return
            
            # Get cached gateways
            gateways = self.cache.get_gateways()
            if not gateways:
                logger.info("No cached gateways, doing full refresh")
                self.fetch_and_cache_all()
                return
            
            # For now, do full refresh of port stats but keep existing structure
            # Future: implement per-gateway staleness tracking
            logger.info(f"Incremental: Updating {len(gateways)} gateways")
            self.cache.set_worker_status('running', {'mode': 'incremental', 'started': datetime.now().isoformat()})
            
            # Quick refresh - just update port stats
            self._fetch_port_stats_incremental(gateways)
            
            # Update timestamp
            self.cache.set_last_update(time.time())
            self.cache.set_worker_status('idle', {
                'mode': 'incremental',
                'last_run': datetime.now().isoformat(),
                'gateways_count': len(gateways)
            })
            
        except Exception as e:
            logger.error(f"Incremental refresh failed: {e}, falling back to full refresh")
            self.fetch_and_cache_all()
    
    def _fetch_vpn_peers_parallel(self, gateways: list):
        """
        Fetch VPN peer paths in parallel using thread pool.
        Much faster than sequential for large gateway counts.
        """
        try:
            all_peers = {}
            total = len(gateways)
            completed = [0]  # Use list to allow modification in nested function
            
            def fetch_single_gateway_vpn(gw):
                gw_id = gw.get('id')
                gw_mac = gw.get('mac')
                site_id = gw.get('site_id')
                if not gw_id or not gw_mac or not site_id:
                    return None, None
                try:
                    # Use correct method signature: get_vpn_peer_stats(site_id, device_mac)
                    peers = self.mist.get_vpn_peer_stats(site_id, gw_mac)
                    if peers:
                        return f"{gw_id}-{gw_mac}", peers
                    return None, None
                except Exception as e:
                    logger.debug(f"VPN peers error for {gw_mac}: {e}")
                    return None, None
            
            logger.info(f"Fetching VPN peers in parallel ({self.parallel_workers} workers)...")
            
            with ThreadPoolExecutor(max_workers=self.parallel_workers) as executor:
                futures = {executor.submit(fetch_single_gateway_vpn, gw): gw for gw in gateways}
                
                for future in as_completed(futures):
                    completed[0] += 1
                    cache_key, peers = future.result()
                    if cache_key and peers:
                        all_peers[cache_key] = peers
                    
                    # Progress update every 100 gateways
                    if completed[0] % 100 == 0:
                        self.cache.set_loading_phase('vpn_peers', 5, {
                            'description': f'VPN peers: {completed[0]}/{total}',
                            'progress': completed[0],
                            'total': total
                        })
                        logger.info(f"VPN peers progress: {completed[0]}/{total}")
            
            # Store all peers
            if all_peers:
                self.cache.set_all_vpn_peers(all_peers, ttl=self.cache_ttl)
            
            logger.info(f"Cached VPN peers for {len(all_peers)} gateways (parallel)")
            
        except Exception as e:
            logger.error(f"Parallel VPN fetch failed: {e}")
            # Fall back to sequential
            self._fetch_vpn_peers(gateways)
    
    def _analyze_port_data_needs(self, gw_id: str, port_id: str) -> dict:
        """
        Analyze existing cached data to determine what needs to be fetched.
        
        Returns dict with:
            - has_data: bool - whether any data exists
            - first_ts: int - earliest timestamp in cache
            - last_ts: int - latest timestamp in cache  
            - data_span_days: float - how many days of data we have
            - gap_at_start: int - seconds of missing data at start (older than our data)
            - gap_at_end: int - seconds of missing data at end (newer than our data)
            - priority: int - fetch priority (lower = higher priority)
                1 = no data (highest priority)
                2 = gaps at end (missing recent data)
                3 = less than 7 days of data
                4 = has 7 days (lowest priority, just refresh recent)
        """
        now = int(time.time())
        target_days = 7
        target_start = now - (target_days * 24 * 60 * 60)
        
        existing = self.cache.get_insights(gw_id, port_id)
        
        if not existing or not existing.get('timestamps'):
            return {
                'has_data': False,
                'first_ts': 0,
                'last_ts': 0,
                'data_span_days': 0,
                'gap_at_start': target_days * 24 * 60 * 60,
                'gap_at_end': 0,
                'priority': 1,  # Highest priority - no data at all
                'fetch_start': target_start,
                'fetch_end': now
            }
        
        timestamps = existing.get('timestamps', [])
        first_ts = min(ts for ts in timestamps if ts) if timestamps else 0
        last_ts = max(ts for ts in timestamps if ts) if timestamps else 0
        
        data_span = (last_ts - first_ts) if (last_ts and first_ts) else 0
        data_span_days = data_span / (24 * 60 * 60)
        
        # Gap at start: how much older data we're missing
        gap_at_start = max(0, first_ts - target_start) if first_ts else target_days * 24 * 60 * 60
        
        # Gap at end: how much recent data we're missing (data older than 15 min is stale)
        stale_threshold = 15 * 60  # 15 minutes
        gap_at_end = max(0, now - last_ts - stale_threshold) if last_ts else 0
        
        # Determine priority
        if gap_at_end > 60 * 60:  # More than 1 hour of missing recent data
            priority = 2
            fetch_start = last_ts  # Fetch from where we left off
            fetch_end = now
        elif data_span_days < 6.5:  # Less than ~7 days
            priority = 3
            # Fetch older data to fill gap at start
            fetch_start = target_start
            fetch_end = first_ts if first_ts else now
        else:
            priority = 4  # Have good coverage, just refresh recent
            fetch_start = now - (24 * 60 * 60)  # Last 24 hours
            fetch_end = now
        
        return {
            'has_data': True,
            'first_ts': first_ts,
            'last_ts': last_ts,
            'data_span_days': data_span_days,
            'gap_at_start': gap_at_start,
            'gap_at_end': gap_at_end,
            'priority': priority,
            'fetch_start': fetch_start,
            'fetch_end': fetch_end
        }
    
    def _merge_insights(self, existing: dict, new_data: dict) -> dict:
        """
        Merge new insights data with existing cached data.
        Combines timestamps and corresponding rx_bps/tx_bps values,
        removes duplicates, and sorts by timestamp.
        """
        if not existing or not existing.get('timestamps'):
            return new_data
        
        if not new_data or not new_data.get('timestamps'):
            return existing
        
        # Combine all data points
        combined = {}
        
        # Add existing data
        for i, ts in enumerate(existing.get('timestamps', [])):
            if ts:
                combined[ts] = {
                    'rx_bps': existing.get('rx_bps', [])[i] if i < len(existing.get('rx_bps', [])) else 0,
                    'tx_bps': existing.get('tx_bps', [])[i] if i < len(existing.get('tx_bps', [])) else 0
                }
        
        # Add/update with new data (new data takes precedence for same timestamp)
        for i, ts in enumerate(new_data.get('timestamps', [])):
            if ts:
                combined[ts] = {
                    'rx_bps': new_data.get('rx_bps', [])[i] if i < len(new_data.get('rx_bps', [])) else 0,
                    'tx_bps': new_data.get('tx_bps', [])[i] if i < len(new_data.get('tx_bps', [])) else 0
                }
        
        # Sort by timestamp and rebuild arrays
        sorted_timestamps = sorted(combined.keys())
        
        result = {
            'timestamps': sorted_timestamps,
            'rx_bps': [combined[ts]['rx_bps'] for ts in sorted_timestamps],
            'tx_bps': [combined[ts]['tx_bps'] for ts in sorted_timestamps],
            'interval': new_data.get('interval', existing.get('interval', 600))
        }
        
        # Recalculate bytes
        interval = result['interval']
        result['rx_bytes'] = sum(bps * interval for bps in result['rx_bps'] if bps) // 8
        result['tx_bytes'] = sum(bps * interval for bps in result['tx_bps'] if bps) // 8
        
        return result
    
    def _fetch_insights_parallel(self, gateways: list):
        """
        Fetch traffic insights in parallel using thread pool.
        
        Multi-resolution strategy:
        1. For each WAN port, fetch 4 different timeframes in parallel
        2. Each resolution provides ~similar data point count at different intervals
        3. Filter out null/zero values before storing
        4. Store each resolution separately for efficient retrieval
        
        Resolutions:
        - 1h: 1-minute intervals (~60 points)
        - 6h: 2-minute intervals (~180 points)  
        - 1d: 10-minute intervals (~144 points)
        - 7d: 1-hour intervals (~168 points)
        """
        try:
            import requests
            from redis_cache import RedisCache
            
            now = int(time.time())
            
            # Resolution configurations
            resolutions = {
                '1h': {'seconds': 3600, 'interval': 60},       # 1 hour, 1-min intervals
                '6h': {'seconds': 21600, 'interval': 120},    # 6 hours, 2-min intervals
                '1d': {'seconds': 86400, 'interval': 600},    # 24 hours, 10-min intervals
                '7d': {'seconds': 604800, 'interval': 3600}   # 7 days, 1-hour intervals
            }
            
            all_insights = {}
            processed = [0]
            
            # Build list of WAN ports
            wan_ports = []
            for gw in gateways:
                gw_id = gw.get('id')
                site_id = gw.get('site_id')
                for port in gw.get('ports', []):
                    port_usage = port.get('port_usage') or port.get('usage', '')
                    if port_usage == 'wan':
                        port_id = port.get('port_id')
                        if gw_id and port_id and site_id:
                            wan_ports.append((gw_id, site_id, port_id))
            
            total_requests = len(wan_ports) * len(resolutions)
            logger.info(f"Fetching multi-resolution insights for {len(wan_ports)} WAN ports "
                       f"({len(resolutions)} resolutions each = {total_requests} API calls)")
            
            def filter_null_zero(insights: dict) -> dict:
                """Filter out null and zero values from insights data."""
                if not insights or not insights.get('timestamps'):
                    return insights
                
                filtered_ts = []
                filtered_rx = []
                filtered_tx = []
                
                timestamps = insights.get('timestamps', [])
                rx_bps = insights.get('rx_bps', [])
                tx_bps = insights.get('tx_bps', [])
                
                for i, ts in enumerate(timestamps):
                    rx = rx_bps[i] if i < len(rx_bps) else 0
                    tx = tx_bps[i] if i < len(tx_bps) else 0
                    
                    # Keep only if at least one value is non-null and non-zero
                    if ts and (rx or tx):
                        filtered_ts.append(ts)
                        filtered_rx.append(rx or 0)
                        filtered_tx.append(tx or 0)
                
                if not filtered_ts:
                    return None
                
                interval = insights.get('interval', 600)
                return {
                    'timestamps': filtered_ts,
                    'rx_bps': filtered_rx,
                    'tx_bps': filtered_tx,
                    'rx_bytes': sum(bps * interval for bps in filtered_rx if bps) // 8,
                    'tx_bytes': sum(bps * interval for bps in filtered_tx if bps) // 8,
                    'interval': interval
                }
            
            def fetch_port_all_resolutions(gw_id: str, site_id: str, port_id: str):
                """Fetch all resolutions for a single port."""
                results = {}
                
                for res_key, res_config in resolutions.items():
                    try:
                        start = now - res_config['seconds']
                        end = now
                        interval = res_config['interval']
                        
                        insights = self.mist._get_port_insights(
                            site_id, gw_id, port_id, start, end, interval
                        )
                        
                        if insights:
                            # Filter out null/zero values
                            filtered = filter_null_zero(insights)
                            if filtered:
                                results[res_key] = filtered
                    except Exception as e:
                        logger.debug(f"Error fetching {res_key} for {gw_id}/{port_id}: {e}")
                
                return (gw_id, port_id, results) if results else None
            
            logger.info(f"Starting parallel fetch with {self.parallel_workers} workers...")
            
            with ThreadPoolExecutor(max_workers=self.parallel_workers) as executor:
                futures = {
                    executor.submit(fetch_port_all_resolutions, gw_id, site_id, port_id): (gw_id, port_id) 
                    for gw_id, site_id, port_id in wan_ports
                }
                
                for future in as_completed(futures):
                    processed[0] += 1
                    result = future.result()
                    
                    if result:
                        gw_id, port_id, port_resolutions = result
                        
                        # Store each resolution immediately
                        for res_key, res_data in port_resolutions.items():
                            self.cache.set_insights_by_resolution(
                                gw_id, port_id, res_key, res_data, ttl=self.cache_ttl
                            )
                        
                        # Also store 7d as the default (backward compatibility)
                        if '7d' in port_resolutions:
                            self.cache.set_insights(gw_id, port_id, port_resolutions['7d'], ttl=self.cache_ttl)
                        
                        # Track for bulk storage
                        if gw_id not in all_insights:
                            all_insights[gw_id] = {}
                        all_insights[gw_id][port_id] = port_resolutions
                    
                    if processed[0] % 50 == 0:
                        self.cache.set_loading_phase('insights', 5, {
                            'description': f'Multi-res insights: {processed[0]}/{len(wan_ports)} ports',
                            'progress': processed[0],
                            'total': len(wan_ports)
                        })
                        logger.info(f"Insights progress: {processed[0]}/{len(wan_ports)} ports")
            
            # Store summary
            total_stored = sum(
                len(resolutions) 
                for ports in all_insights.values() 
                for resolutions in ports.values()
            )
            logger.info(f"Cached multi-resolution insights: {len(wan_ports)} ports, {total_stored} total entries")
            
        except Exception as e:
            logger.error(f"Parallel multi-resolution insights fetch failed: {e}")


if __name__ == '__main__':
    worker = DataWorker()
    worker.run()
