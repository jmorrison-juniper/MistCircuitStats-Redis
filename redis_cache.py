"""
Redis Cache Manager for MistCircuitStats

Handles all Redis operations for caching gateway data, VPN peer paths,
and traffic insights.
"""

import os
import json
import time
import logging
from typing import Any, Optional

import redis

logger = logging.getLogger(__name__)


class RedisCache:
    """Redis cache manager for Mist gateway data"""
    
    # Cache key prefixes
    PREFIX_GATEWAYS = "mist:gateways"
    PREFIX_SITES = "mist:sites"
    PREFIX_ORG = "mist:org"
    PREFIX_VPN_PEERS = "mist:vpn_peers"
    PREFIX_INSIGHTS = "mist:insights"
    PREFIX_METADATA = "mist:metadata"
    
    # Default TTL (15 minutes)
    DEFAULT_TTL = 900
    
    def __init__(self, redis_url: Optional[str] = None):
        """
        Initialize Redis connection
        
        Args:
            redis_url: Redis connection URL (default: redis://localhost:6379)
        """
        self.redis_url = redis_url or os.environ.get('REDIS_URL', 'redis://localhost:6379')
        self.client = redis.from_url(self.redis_url, decode_responses=True)
        
        # Test connection
        try:
            self.client.ping()
            logger.info(f"Connected to Redis at {self.redis_url}")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def _serialize(self, data: Any) -> str:
        """Serialize data to JSON string"""
        return json.dumps(data)
    
    def _deserialize(self, data: Optional[str]) -> Any:
        """Deserialize JSON string to data"""
        if data is None:
            return None
        return json.loads(data)
    
    # ==================== Organization Data ====================
    
    def set_organization(self, org_data: dict[str, Any], ttl: Optional[int] = None) -> bool:
        """Store organization data"""
        try:
            self.client.setex(
                self.PREFIX_ORG,
                ttl or self.DEFAULT_TTL,
                self._serialize(org_data)
            )
            return True
        except Exception as e:
            logger.error(f"Error storing organization data: {e}")
            return False
    
    def get_organization(self) -> Optional[dict[str, Any]]:
        """Retrieve organization data"""
        try:
            data = self.client.get(self.PREFIX_ORG)
            return self._deserialize(data)
        except Exception as e:
            logger.error(f"Error retrieving organization data: {e}")
            return None
    
    # ==================== Sites Data ====================
    
    def set_sites(self, sites: list[dict[str, Any]], ttl: Optional[int] = None) -> bool:
        """Store sites list"""
        try:
            self.client.setex(
                self.PREFIX_SITES,
                ttl or self.DEFAULT_TTL,
                self._serialize(sites)
            )
            return True
        except Exception as e:
            logger.error(f"Error storing sites data: {e}")
            return False
    
    def get_sites(self) -> Optional[list[dict[str, Any]]]:
        """Retrieve sites list"""
        try:
            data = self.client.get(self.PREFIX_SITES)
            return self._deserialize(data)
        except Exception as e:
            logger.error(f"Error retrieving sites data: {e}")
            return None
    
    # ==================== Device Profiles (Hub Templates) ====================
    
    def set_device_profile(self, profile_id: str, profile_data: dict[str, Any], ttl: Optional[int] = None) -> bool:
        """Store a device profile (Hub template)"""
        try:
            key = f"mist:profiles:{profile_id}"
            self.client.setex(
                key,
                ttl or self.DEFAULT_TTL,
                self._serialize(profile_data)
            )
            logger.debug(f"Cached device profile {profile_id}")
            return True
        except Exception as e:
            logger.error(f"Error storing device profile {profile_id}: {e}")
            return False
    
    def get_device_profile(self, profile_id: str) -> Optional[dict[str, Any]]:
        """Retrieve a device profile (Hub template)"""
        try:
            key = f"mist:profiles:{profile_id}"
            data = self.client.get(key)
            return self._deserialize(data)
        except Exception as e:
            logger.error(f"Error retrieving device profile {profile_id}: {e}")
            return None
    
    # ==================== Gateway Templates (Edge/Branch Templates) ====================
    
    def set_gateway_template(self, template_id: str, template_data: dict[str, Any], ttl: Optional[int] = None) -> bool:
        """Store a gateway template (Edge/Branch template)"""
        try:
            key = f"mist:templates:{template_id}"
            self.client.setex(
                key,
                ttl or self.DEFAULT_TTL,
                self._serialize(template_data)
            )
            logger.debug(f"Cached gateway template {template_id}")
            return True
        except Exception as e:
            logger.error(f"Error storing gateway template {template_id}: {e}")
            return False
    
    def get_gateway_template(self, template_id: str) -> Optional[dict[str, Any]]:
        """Retrieve a gateway template (Edge/Branch template)"""
        try:
            key = f"mist:templates:{template_id}"
            data = self.client.get(key)
            return self._deserialize(data)
        except Exception as e:
            logger.error(f"Error retrieving gateway template {template_id}: {e}")
            return None
    
    def get_all_gateway_templates(self) -> dict[str, dict[str, Any]]:
        """Retrieve all gateway templates"""
        try:
            templates = {}
            keys = self.client.keys("mist:templates:*")
            for key in keys:
                # Handle both bytes and str (depends on redis client version)
                key_str = key.decode() if isinstance(key, bytes) else key
                template_id = key_str.replace("mist:templates:", "")
                data = self.client.get(key)
                if data:
                    templates[template_id] = self._deserialize(data)
            return templates
        except Exception as e:
            logger.error(f"Error retrieving all gateway templates: {e}")
            return {}
    
    # ==================== Device Configs (per-gateway configuration) ====================
    
    def set_device_config(self, device_id: str, config_data: dict[str, Any], ttl: Optional[int] = None) -> bool:
        """Store a device configuration (full gateway config with port_config)"""
        try:
            key = f"mist:device_config:{device_id}"
            self.client.setex(
                key,
                ttl or self.DEFAULT_TTL,
                self._serialize(config_data)
            )
            logger.debug(f"Cached device config {device_id}")
            return True
        except Exception as e:
            logger.error(f"Error storing device config {device_id}: {e}")
            return False
    
    def get_device_config(self, device_id: str) -> Optional[dict[str, Any]]:
        """Retrieve a device configuration"""
        try:
            key = f"mist:device_config:{device_id}"
            data = self.client.get(key)
            return self._deserialize(data)
        except Exception as e:
            logger.error(f"Error retrieving device config {device_id}: {e}")
            return None
    
    # ==================== Inventory Data (device profile IDs, site IDs) ====================
    
    def set_inventory(self, inventory_data: dict[str, dict[str, Any]], ttl: Optional[int] = None) -> bool:
        """Store inventory data (MAC -> device info including deviceprofile_id)"""
        try:
            key = "mist:inventory"
            self.client.setex(
                key,
                ttl or self.DEFAULT_TTL,
                self._serialize(inventory_data)
            )
            logger.info(f"Cached inventory data for {len(inventory_data)} devices")
            return True
        except Exception as e:
            logger.error(f"Error storing inventory data: {e}")
            return False
    
    def get_inventory(self) -> Optional[dict[str, dict[str, Any]]]:
        """Retrieve inventory data"""
        try:
            key = "mist:inventory"
            data = self.client.get(key)
            return self._deserialize(data)
        except Exception as e:
            logger.error(f"Error retrieving inventory data: {e}")
            return None
    
    # ==================== Raw API Responses (for debugging) ====================
    
    def set_raw_api_response(self, endpoint_name: str, device_id: str, response_data: dict[str, Any], ttl: Optional[int] = None) -> bool:
        """Store raw API response for debugging/analysis"""
        try:
            key = f"mist:raw:{endpoint_name}:{device_id}"
            self.client.setex(
                key,
                ttl or self.DEFAULT_TTL,
                self._serialize(response_data)
            )
            logger.debug(f"Cached raw API response {endpoint_name} for {device_id}")
            return True
        except Exception as e:
            logger.error(f"Error storing raw API response: {e}")
            return False
    
    def get_raw_api_response(self, endpoint_name: str, device_id: str) -> Optional[dict[str, Any]]:
        """Retrieve raw API response"""
        try:
            key = f"mist:raw:{endpoint_name}:{device_id}"
            data = self.client.get(key)
            return self._deserialize(data)
        except Exception as e:
            logger.error(f"Error retrieving raw API response: {e}")
            return None
    
    # ==================== Gateway Data ====================
    
    def set_gateways(self, gateways: list[dict[str, Any]], ttl: Optional[int] = None) -> bool:
        """Store all gateway data"""
        try:
            self.client.setex(
                self.PREFIX_GATEWAYS,
                ttl or self.DEFAULT_TTL,
                self._serialize(gateways)
            )
            logger.info(f"Stored {len(gateways)} gateways in cache")
            return True
        except Exception as e:
            logger.error(f"Error storing gateway data: {e}")
            return False
    
    def get_gateways(self) -> Optional[list[dict[str, Any]]]:
        """Retrieve all gateway data"""
        try:
            data = self.client.get(self.PREFIX_GATEWAYS)
            return self._deserialize(data)
        except Exception as e:
            logger.error(f"Error retrieving gateway data: {e}")
            return None
    
    # ==================== VPN Peer Paths ====================
    
    def set_vpn_peers(self, gateway_id: str, mac: str, peers_by_port: dict[str, Any], ttl: Optional[int] = None) -> bool:
        """Store VPN peer paths for a gateway"""
        try:
            key = f"{self.PREFIX_VPN_PEERS}:{gateway_id}:{mac}"
            self.client.setex(
                key,
                ttl or self.DEFAULT_TTL,
                self._serialize(peers_by_port)
            )
            return True
        except Exception as e:
            logger.error(f"Error storing VPN peers for {gateway_id}: {e}")
            return False
    
    def get_vpn_peers(self, gateway_id: str, mac: str) -> Optional[dict[str, Any]]:
        """Retrieve VPN peer paths for a gateway"""
        try:
            key = f"{self.PREFIX_VPN_PEERS}:{gateway_id}-{mac}"
            data = self.client.get(key)
            return self._deserialize(data)
        except Exception as e:
            logger.error(f"Error retrieving VPN peers for {gateway_id}: {e}")
            return None
    
    def set_all_vpn_peers(self, all_peers: dict[str, dict[str, Any]], ttl: Optional[int] = None) -> bool:
        """Store all VPN peer paths in a single operation"""
        try:
            pipe = self.client.pipeline()
            for cache_key, peers_by_port in all_peers.items():
                key = f"{self.PREFIX_VPN_PEERS}:{cache_key}"
                pipe.setex(key, ttl or self.DEFAULT_TTL, self._serialize(peers_by_port))
            pipe.execute()
            logger.info(f"Stored VPN peers for {len(all_peers)} gateways")
            return True
        except Exception as e:
            logger.error(f"Error storing all VPN peers: {e}")
            return False
    
    def get_all_vpn_peers(self) -> dict[str, dict[str, Any]]:
        """Retrieve all VPN peer paths"""
        try:
            pattern = f"{self.PREFIX_VPN_PEERS}:*"
            keys = self.client.keys(pattern)
            
            if not keys:
                return {}
            
            result = {}
            pipe = self.client.pipeline()
            for key in keys:
                pipe.get(key)
            
            values = pipe.execute()
            
            for key, value in zip(keys, values):
                # Extract cache_key from redis key
                cache_key = key.replace(f"{self.PREFIX_VPN_PEERS}:", "")
                if value:
                    result[cache_key] = self._deserialize(value)
            
            return result
        except Exception as e:
            logger.error(f"Error retrieving all VPN peers: {e}")
            return {}
    
    # ==================== Traffic Insights ====================
    
    def set_insights(self, gateway_id: str, port_id: str, insights: dict[str, Any], ttl: Optional[int] = None) -> bool:
        """Store traffic insights for a port"""
        try:
            key = f"{self.PREFIX_INSIGHTS}:{gateway_id}:{port_id}"
            self.client.setex(
                key,
                ttl or self.DEFAULT_TTL,
                self._serialize(insights)
            )
            return True
        except Exception as e:
            logger.error(f"Error storing insights for {gateway_id}/{port_id}: {e}")
            return False
    
    def get_insights(self, gateway_id: str, port_id: str) -> Optional[dict[str, Any]]:
        """Retrieve traffic insights for a port"""
        try:
            key = f"{self.PREFIX_INSIGHTS}:{gateway_id}:{port_id}"
            data = self.client.get(key)
            return self._deserialize(data)
        except Exception as e:
            logger.error(f"Error retrieving insights for {gateway_id}/{port_id}: {e}")
            return None
    
    def set_all_insights(self, all_insights: dict[str, dict[str, dict[str, Any]]], ttl: Optional[int] = None) -> bool:
        """
        Store all insights in a single operation
        
        Args:
            all_insights: Dict of gateway_id -> {port_id -> insights_data}
        """
        try:
            pipe = self.client.pipeline()
            count = 0
            for gateway_id, ports in all_insights.items():
                for port_id, insights in ports.items():
                    key = f"{self.PREFIX_INSIGHTS}:{gateway_id}:{port_id}"
                    pipe.setex(key, ttl or self.DEFAULT_TTL, self._serialize(insights))
                    count += 1
            pipe.execute()
            logger.info(f"Stored insights for {count} ports")
            return True
        except Exception as e:
            logger.error(f"Error storing all insights: {e}")
            return False
    
    def get_all_insights(self) -> dict[str, dict[str, dict[str, Any]]]:
        """Retrieve all traffic insights"""
        try:
            pattern = f"{self.PREFIX_INSIGHTS}:*"
            keys = self.client.keys(pattern)
            
            if not keys:
                return {}
            
            result = {}
            pipe = self.client.pipeline()
            for key in keys:
                pipe.get(key)
            
            values = pipe.execute()
            
            for key, value in zip(keys, values):
                # Parse key: mist:insights:gateway_id:port_id
                parts = key.split(":", 3)
                if len(parts) >= 4:
                    gateway_id = parts[2]
                    port_id = parts[3]
                    
                    if gateway_id not in result:
                        result[gateway_id] = {}
                    
                    if value:
                        result[gateway_id][port_id] = self._deserialize(value)
            
            return result
        except Exception as e:
            logger.error(f"Error retrieving all insights: {e}")
            return {}
    
    # ==================== Metadata ====================
    
    def set_last_update(self, timestamp: float) -> bool:
        """Store last data update timestamp"""
        try:
            self.client.set(f"{self.PREFIX_METADATA}:last_update", str(timestamp))
            return True
        except Exception as e:
            logger.error(f"Error storing last update timestamp: {e}")
            return False
    
    def get_last_update(self) -> Optional[float]:
        """Retrieve last data update timestamp"""
        try:
            data = self.client.get(f"{self.PREFIX_METADATA}:last_update")
            return float(data) if data else None
        except Exception as e:
            logger.error(f"Error retrieving last update timestamp: {e}")
            return None
    
    def set_worker_status(self, status: str, details: Optional[dict[str, Any]] = None) -> bool:
        """Store worker status"""
        try:
            status_data = {
                'status': status,
                'details': details or {}
            }
            self.client.set(f"{self.PREFIX_METADATA}:worker_status", self._serialize(status_data))
            return True
        except Exception as e:
            logger.error(f"Error storing worker status: {e}")
            return False
    
    def set_loading_phase(self, phase: str, total_phases: int = 4, details: Optional[dict[str, Any]] = None) -> bool:
        """Store current loading phase for progressive status display"""
        try:
            phase_data = {
                'current_phase': phase,
                'total_phases': total_phases,
                'details': details or {},
                'timestamp': time.time()
            }
            self.client.set(f"{self.PREFIX_METADATA}:loading_phase", self._serialize(phase_data))
            return True
        except Exception as e:
            logger.error(f"Error storing loading phase: {e}")
            return False
    
    def get_loading_phase(self) -> Optional[dict[str, Any]]:
        """Retrieve current loading phase"""
        try:
            data = self.client.get(f"{self.PREFIX_METADATA}:loading_phase")
            return self._deserialize(data)
        except Exception as e:
            logger.error(f"Error retrieving loading phase: {e}")
            return None
    
    def get_worker_status(self) -> Optional[dict[str, Any]]:
        """Retrieve worker status"""
        try:
            data = self.client.get(f"{self.PREFIX_METADATA}:worker_status")
            return self._deserialize(data)
        except Exception as e:
            logger.error(f"Error retrieving worker status: {e}")
            return None
    
    def set_rate_limit_status(self, is_limited: bool, reset_time: Optional[int] = None, 
                               tokens_exhausted: int = 0, total_tokens: int = 0) -> bool:
        """Store rate limit status. Tokens reset at the top of each clock hour."""
        try:
            if is_limited and reset_time is None:
                # Calculate next hour reset time
                import math
                current_time = time.time()
                reset_time = int(math.ceil(current_time / 3600) * 3600)
            
            status_data = {
                'is_limited': is_limited,
                'reset_time': reset_time,
                'tokens_exhausted': tokens_exhausted,
                'total_tokens': total_tokens,
                'timestamp': time.time()
            }
            # Set with TTL of 1 hour max (will auto-clear if we forget)
            self.client.setex(
                f"{self.PREFIX_METADATA}:rate_limit", 
                3600,  # 1 hour TTL
                self._serialize(status_data)
            )
            return True
        except Exception as e:
            logger.error(f"Error storing rate limit status: {e}")
            return False
    
    def get_rate_limit_status(self) -> Optional[dict[str, Any]]:
        """Retrieve rate limit status"""
        try:
            data = self.client.get(f"{self.PREFIX_METADATA}:rate_limit")
            if data:
                status = self._deserialize(data)
                # Check if rate limit has expired
                if status and status.get('reset_time'):
                    if time.time() >= status['reset_time']:
                        # Rate limit expired, clear it
                        self.clear_rate_limit_status()
                        return {'is_limited': False}
                return status
            return {'is_limited': False}
        except Exception as e:
            logger.error(f"Error retrieving rate limit status: {e}")
            return {'is_limited': False}
    
    def clear_rate_limit_status(self) -> bool:
        """Clear rate limit status"""
        try:
            self.client.delete(f"{self.PREFIX_METADATA}:rate_limit")
            return True
        except Exception as e:
            logger.error(f"Error clearing rate limit status: {e}")
            return False
    
    # ==================== Utility ====================
    
    def clear_all(self) -> bool:
        """Clear all cached data"""
        try:
            patterns = [
                f"{self.PREFIX_GATEWAYS}*",
                f"{self.PREFIX_SITES}*",
                f"{self.PREFIX_ORG}*",
                f"{self.PREFIX_VPN_PEERS}*",
                f"{self.PREFIX_INSIGHTS}*",
                f"{self.PREFIX_METADATA}*"
            ]
            
            for pattern in patterns:
                keys = self.client.keys(pattern)
                if keys:
                    self.client.delete(*keys)
            
            logger.info("Cleared all cached data")
            return True
        except Exception as e:
            logger.error(f"Error clearing cache: {e}")
            return False
    
    def get_cache_stats(self) -> dict[str, Any]:
        """Get cache statistics with actual counts"""
        try:
            # Get actual gateway counts
            gateways = self.get_gateways()
            gateway_count = len(gateways) if gateways else 0
            connected_count = len([gw for gw in gateways if gw.get('status') == 'connected']) if gateways else 0
            
            # Count total WAN ports and active ports
            total_ports = 0
            active_ports = 0
            if gateways:
                for gw in gateways:
                    ports = gw.get('ports', [])
                    total_ports += len(ports)
                    active_ports += len([p for p in ports if p.get('up')])
            
            # Get sites count
            sites = self.get_sites()
            sites_count = len(sites) if sites else 0
            
            # Get key counts using string literals
            vpn_keys = self.client.keys(f"{self.PREFIX_VPN_PEERS}:*")
            insights_keys = self.client.keys(f"{self.PREFIX_INSIGHTS}:*")
            profiles_keys = self.client.keys("mist:profiles:*")
            templates_keys = self.client.keys("mist:templates:*")
            
            # Count total VPN peer paths (not just number of gateways with peers)
            total_vpn_peers = 0
            if vpn_keys:
                for key in vpn_keys:
                    try:
                        data = self.client.get(key)
                        if data:
                            peer_data = json.loads(data)
                            peers_by_port = peer_data.get('peers_by_port', {})
                            for port_peers in peers_by_port.values():
                                total_vpn_peers += len(port_peers)
                    except Exception:
                        pass  # Skip malformed entries
            
            stats = {
                'gateways_count': gateway_count,
                'connected_count': connected_count,
                'total_ports': total_ports,
                'active_ports': active_ports,
                'sites_count': sites_count,
                'vpn_peers_count': total_vpn_peers,
                'insights_count': len(insights_keys) if insights_keys else 0,
                'profiles_count': len(profiles_keys) if profiles_keys else 0,
                'templates_count': len(templates_keys) if templates_keys else 0,
                'has_org': bool(self.client.exists(self.PREFIX_ORG)),
                'has_sites': bool(self.client.exists(self.PREFIX_SITES)),
                'has_gateways': bool(self.client.exists(self.PREFIX_GATEWAYS)),
                'last_update': self.get_last_update(),
                'worker_status': self.get_worker_status()
            }
            return stats
        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {}
    
    def is_cache_valid(self) -> bool:
        """Check if cache has valid data"""
        try:
            return self.client.exists(self.PREFIX_GATEWAYS) > 0
        except Exception as e:
            logger.error(f"Error checking cache validity: {e}")
            return False
