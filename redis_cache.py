"""
Redis Cache Manager for MistCircuitStats

Handles all Redis operations for caching gateway data, VPN peer paths,
and traffic insights.
"""

import os
import json
import logging
import redis
from typing import Dict, List, Optional, Any

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
    
    def __init__(self, redis_url: str = None):
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
    
    def _deserialize(self, data: str) -> Any:
        """Deserialize JSON string to data"""
        if data is None:
            return None
        return json.loads(data)
    
    # ==================== Organization Data ====================
    
    def set_organization(self, org_data: Dict, ttl: int = None) -> bool:
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
    
    def get_organization(self) -> Optional[Dict]:
        """Retrieve organization data"""
        try:
            data = self.client.get(self.PREFIX_ORG)
            return self._deserialize(data)
        except Exception as e:
            logger.error(f"Error retrieving organization data: {e}")
            return None
    
    # ==================== Sites Data ====================
    
    def set_sites(self, sites: List[Dict], ttl: int = None) -> bool:
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
    
    def get_sites(self) -> Optional[List[Dict]]:
        """Retrieve sites list"""
        try:
            data = self.client.get(self.PREFIX_SITES)
            return self._deserialize(data)
        except Exception as e:
            logger.error(f"Error retrieving sites data: {e}")
            return None
    
    # ==================== Gateway Data ====================
    
    def set_gateways(self, gateways: List[Dict], ttl: int = None) -> bool:
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
    
    def get_gateways(self) -> Optional[List[Dict]]:
        """Retrieve all gateway data"""
        try:
            data = self.client.get(self.PREFIX_GATEWAYS)
            return self._deserialize(data)
        except Exception as e:
            logger.error(f"Error retrieving gateway data: {e}")
            return None
    
    # ==================== VPN Peer Paths ====================
    
    def set_vpn_peers(self, gateway_id: str, mac: str, peers_by_port: Dict, ttl: int = None) -> bool:
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
    
    def get_vpn_peers(self, gateway_id: str, mac: str) -> Optional[Dict]:
        """Retrieve VPN peer paths for a gateway"""
        try:
            key = f"{self.PREFIX_VPN_PEERS}:{gateway_id}:{mac}"
            data = self.client.get(key)
            return self._deserialize(data)
        except Exception as e:
            logger.error(f"Error retrieving VPN peers for {gateway_id}: {e}")
            return None
    
    def set_all_vpn_peers(self, all_peers: Dict[str, Dict], ttl: int = None) -> bool:
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
    
    def get_all_vpn_peers(self) -> Dict[str, Dict]:
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
    
    def set_insights(self, gateway_id: str, port_id: str, insights: Dict, ttl: int = None) -> bool:
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
    
    def get_insights(self, gateway_id: str, port_id: str) -> Optional[Dict]:
        """Retrieve traffic insights for a port"""
        try:
            key = f"{self.PREFIX_INSIGHTS}:{gateway_id}:{port_id}"
            data = self.client.get(key)
            return self._deserialize(data)
        except Exception as e:
            logger.error(f"Error retrieving insights for {gateway_id}/{port_id}: {e}")
            return None
    
    def set_all_insights(self, all_insights: Dict[str, Dict[str, Dict]], ttl: int = None) -> bool:
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
    
    def get_all_insights(self) -> Dict[str, Dict[str, Dict]]:
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
    
    def set_worker_status(self, status: str, details: Dict = None) -> bool:
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
    
    def get_worker_status(self) -> Optional[Dict]:
        """Retrieve worker status"""
        try:
            data = self.client.get(f"{self.PREFIX_METADATA}:worker_status")
            return self._deserialize(data)
        except Exception as e:
            logger.error(f"Error retrieving worker status: {e}")
            return None
    
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
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics"""
        try:
            stats = {
                'gateways': self.client.exists(self.PREFIX_GATEWAYS),
                'sites': self.client.exists(self.PREFIX_SITES),
                'org': self.client.exists(self.PREFIX_ORG),
                'vpn_peers_count': len(self.client.keys(f"{self.PREFIX_VPN_PEERS}:*")),
                'insights_count': len(self.client.keys(f"{self.PREFIX_INSIGHTS}:*")),
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
