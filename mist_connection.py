"""
Mist API Connection Wrapper
Handles all interactions with the Juniper Mist API using mistapi SDK

The mistapi SDK natively supports:
- Multiple API tokens (comma-separated in MIST_APITOKEN)
- Automatic 429 rate limit detection
- Automatic token rotation on rate limit
- Token validation (all tokens must have same org privileges)
"""
import copy
import logging
import os
import time
from typing import List, Dict, Optional
import mistapi

logger = logging.getLogger(__name__)


class MistConnection:
    """Wrapper class for Mist API operations"""
    
    # Class-level caches to reduce API calls across requests
    _sites_cache: Optional[List[Dict]] = None
    _sites_cache_time: float = 0
    _device_profile_cache: Dict[str, Dict] = {}
    _gateway_template_cache: Dict[str, Dict] = {}
    
    # Cache TTLs (in seconds)
    SITES_CACHE_TTL = 300  # 5 minutes
    PROFILE_CACHE_TTL = 600  # 10 minutes
    TEMPLATE_CACHE_TTL = 2678400  # 31 days for Redis persistence
    
    # API rate limiting delay (set via API_DELAY_MS env var, default 2ms)
    API_DELAY_SECONDS = float(os.environ.get('API_DELAY_MS', '2')) / 1000.0
    
    def __init__(self, api_token: str, org_id: Optional[str] = None, host: str = 'api.mist.com', redis_cache=None):
        """
        Initialize Mist API connection with support for multiple tokens
        
        The mistapi SDK natively handles comma-separated tokens with automatic
        rotation on 429 rate limit responses.
        
        Args:
            api_token: Mist API token(s) - can be comma-separated for multiple tokens
            org_id: Organization ID (optional, will auto-detect if not provided)
            host: Mist API host (default: api.mist.com)
        """
        import os
        
        if not api_token:
            raise ValueError("MIST_APITOKEN environment variable is required")
        
        # Count tokens for logging (SDK parses them internally)
        token_list = [t.strip() for t in api_token.split(',') if t.strip()]
        token_count = len(token_list)
        if token_count == 0:
            raise ValueError("No valid API tokens provided")
        
        logger.info(f"Initialized with {token_count} API token(s)")
        
        self.host = host
        self.org_id = org_id
        self.api_token = api_token  # Store original token string
        self._token_count = token_count  # Store token count
        self._self_data = None  # Cache for /api/v1/self response
        self._redis_cache = redis_cache  # Optional Redis cache for template persistence
        
        # Temporarily unset MIST_APITOKEN to prevent SDK from auto-loading and
        # validating all tokens (which can fail if rate limited during validation)
        saved_token = os.environ.pop('MIST_APITOKEN', None)
        
        try:
            # Try to initialize with each token until one succeeds
            # The SDK has a bug where its 429 retry logic doesn't work correctly
            # (the finally block overrides the return from the recursive retry call)
            # So we implement our own token fallback during initialization
            last_error = None
            
            for idx, token in enumerate(token_list):
                try:
                    logger.info(f"Trying to initialize with token {idx + 1}/{len(token_list)}")
                    self.apisession = mistapi.APISession(
                        host=self.host,
                        apitoken=token,
                        console_log_level=30,  # WARNING  # type: ignore[arg-type]
                        logging_log_level=20   # INFO  # type: ignore[arg-type]
                    )
                    
                    # Test if the token actually works with an API call
                    test_response = mistapi.api.v1.self.self.getSelf(self.apisession)
                    time.sleep(self.API_DELAY_SECONDS)  # Rate limiting delay
                    if test_response.status_code == 200:
                        logger.info(f"Token {idx + 1}/{len(token_list)} working")
                        # Store all tokens for potential future manual rotation
                        self._token_list = token_list
                        self._current_token_idx = idx
                        # Cache the self response for org_id detection
                        self._self_data = test_response.data
                        
                        # IMPORTANT: Inject ALL tokens into the SDK for automatic rotation
                        # The SDK stores tokens in _apitoken list and rotates on 429
                        self.apisession._apitoken = token_list
                        self.apisession._apitoken_index = idx
                        # Update the session headers with current token
                        self.apisession._session.headers.update(
                            {"Authorization": "Token " + token_list[idx]}
                        )
                        logger.info(f"Injected {len(token_list)} tokens into SDK, starting at index {idx}")
                        break
                    elif test_response.status_code == 429:
                        logger.warning(f"Token {idx + 1}/{len(token_list)} rate limited, trying next...")
                        last_error = Exception(f"Token {idx + 1} rate limited (429)")
                        # Report incremental rate limit
                        if self._redis_cache:
                            try:
                                self._redis_cache.set_rate_limit_status(
                                    is_limited=True,
                                    tokens_exhausted=idx + 1,
                                    total_tokens=self._token_count
                                )
                            except Exception:
                                pass  # Best effort
                        continue
                    else:
                        logger.warning(f"Token {idx + 1}/{len(token_list)} returned {test_response.status_code}")
                        last_error = Exception(f"Token {idx + 1} returned {test_response.status_code}")
                        continue
                        
                except SystemExit:
                    logger.warning(f"Token {idx + 1}/{len(token_list)} caused SDK to exit")
                    last_error = Exception(f"Token {idx + 1} caused SDK exit")
                    # Report rate limit for this token
                    if self._redis_cache:
                        try:
                            self._redis_cache.set_rate_limit_status(
                                is_limited=True,
                                tokens_exhausted=idx + 1,
                                total_tokens=self._token_count
                            )
                        except Exception:
                            pass  # Best effort
                    continue
                except Exception as e:
                    logger.warning(f"Token {idx + 1}/{len(token_list)} failed: {e}")
                    last_error = e
                    continue
            else:
                # No token worked - report rate limit to Redis for GUI display
                if self._redis_cache:
                    try:
                        self._redis_cache.set_rate_limit_status(
                            is_limited=True,
                            tokens_exhausted=self._token_count,
                            total_tokens=self._token_count
                        )
                        logger.warning(f"All {self._token_count} tokens exhausted - rate limit reported")
                    except Exception as rl_err:
                        logger.debug(f"Could not report rate limit: {rl_err}")
                raise last_error or Exception("All tokens failed to initialize")
                
        finally:
            # Restore the environment variable
            if saved_token is not None:
                os.environ['MIST_APITOKEN'] = saved_token
        
        # Auto-detect org_id if not provided (uses cached _self_data)
        if not self.org_id:
            self._auto_detect_org()
        
        logger.info(f"Initialized Mist connection to {self.host} for org {self.org_id}")
    
    def _get_token_count(self) -> int:
        """Get the number of configured API tokens"""
        return self._token_count
    
    def _get_current_token_index(self) -> int:
        """Get the current token index (0-indexed) from SDK session"""
        try:
            if hasattr(self.apisession, '_apitoken_index'):
                return self.apisession._apitoken_index
        except Exception:
            pass
        return getattr(self, '_current_token_idx', 0)
    
    def _report_rate_limit(self, tokens_exhausted: int = 0):
        """Report rate limit status to Redis cache for GUI display"""
        if self._redis_cache:
            try:
                self._redis_cache.set_rate_limit_status(
                    is_limited=True,
                    tokens_exhausted=tokens_exhausted,
                    total_tokens=self._token_count
                )
                logger.warning(f"Rate limit reported: {tokens_exhausted}/{self._token_count} tokens exhausted")
            except Exception as e:
                logger.debug(f"Could not report rate limit to Redis: {e}")
    
    def _clear_rate_limit(self):
        """Clear rate limit status in Redis cache"""
        if self._redis_cache:
            try:
                self._redis_cache.clear_rate_limit_status()
            except Exception as e:
                logger.debug(f"Could not clear rate limit in Redis: {e}")
    
    def _auto_detect_org(self):
        """Auto-detect organization ID from user privileges"""
        try:
            # Use cached self data if available (from init token validation)
            if self._self_data:
                data = self._self_data
            else:
                response = mistapi.api.v1.self.self.getSelf(self.apisession)
                time.sleep(self.API_DELAY_SECONDS)  # Rate limiting delay
                if response.status_code != 200:
                    raise Exception(f"Failed to get self info: {response.status_code}")
                data = response.data
            
            # Get first org from privileges
            if 'privileges' in data and len(data['privileges']) > 0:
                self.org_id = data['privileges'][0].get('org_id')
                logger.info(f"Auto-detected org_id: {self.org_id}")
            else:
                raise ValueError("No organizations found in user privileges")
        except Exception as e:
            logger.error(f"Error auto-detecting org_id: {str(e)}")
            raise
    
    def get_organization_info(self) -> Dict:
        """Get current organization information"""
        try:
            if not self.org_id:
                raise ValueError("Organization ID is required")
            response = mistapi.api.v1.orgs.orgs.getOrg(self.apisession, self.org_id)
            time.sleep(self.API_DELAY_SECONDS)  # Rate limiting delay
            if response.status_code == 200:
                data = response.data
                return {
                    'org_id': data.get('id'),
                    'org_name': data.get('name', 'Unknown Organization'),
                    'created_time': data.get('created_time', 0),
                    'updated_time': data.get('updated_time', 0)
                }
            else:
                raise Exception(f"API error: {response.status_code}")
        except Exception as e:
            logger.error(f"Error getting organization info: {str(e)}")
            raise
    
    def get_organizations(self) -> List[Dict]:
        """Get list of organizations the user has access to"""
        try:
            response = mistapi.api.v1.self.self.getSelf(self.apisession)
            time.sleep(self.API_DELAY_SECONDS)
            if response.status_code == 200:
                data = response.data
                orgs = []
                if 'privileges' in data:
                    for priv in data['privileges']:
                        if 'org_id' in priv and 'org_name' in priv:
                            orgs.append({
                                'org_id': priv['org_id'],
                                'org_name': priv['org_name'],
                                'role': priv.get('role', 'unknown')
                            })
                return orgs
            else:
                raise Exception(f"API error: {response.status_code}")
        except Exception as e:
            logger.error(f"Error getting organizations: {str(e)}")
            raise

    def get_api_usage(self) -> Dict:
        """
        Get API usage statistics for the current token.
        
        Note: The SDK handles 429 rate limiting automatically by rotating
        to the next available token. This returns usage for the current token.
        
        Returns:
            Dict with 'requests', 'request_limit', 'seconds' (until reset),
            'token_count' (number of configured tokens), and 'current_token' (1-indexed)
        """
        token_count = self._get_token_count()
        current_token_idx = self._get_current_token_index() + 1  # 1-indexed for display
        try:
            response = mistapi.api.v1.self.usage.getSelfApiUsage(self.apisession)
            time.sleep(self.API_DELAY_SECONDS)
            if response.status_code == 200:
                data = response.data
                return {
                    'requests': data.get('requests', 0),
                    'request_limit': data.get('request_limit', 5000),
                    'seconds_until_reset': data.get('seconds', 0),
                    'token_count': token_count,
                    'current_token': current_token_idx
                }
            else:
                logger.warning(f"Failed to get API usage: {response.status_code}")
                return {
                    'requests': 0,
                    'request_limit': 5000,
                    'seconds_until_reset': 0,
                    'token_count': token_count,
                    'current_token': current_token_idx
                }
        except Exception as e:
            logger.error(f"Error getting API usage: {str(e)}")
            return {
                'requests': 0,
                'request_limit': 5000,
                'seconds_until_reset': 0,
                'token_count': token_count,
                'current_token': current_token_idx,
                'error': str(e)
            }
    
    def get_sites(self) -> List[Dict]:
        """Get list of sites in the organization (cached with pagination)"""
        try:
            if not self.org_id:
                raise ValueError("Organization ID is required")
            
            # Check class-level cache
            current_time = time.time()
            if (MistConnection._sites_cache is not None and 
                current_time - MistConnection._sites_cache_time < self.SITES_CACHE_TTL):
                logger.debug("Using cached sites data")
                return MistConnection._sites_cache
            
            # Fetch all sites with automatic pagination
            response = mistapi.api.v1.orgs.sites.listOrgSites(
                self.apisession,
                self.org_id,
                limit=1000
            )
            time.sleep(self.API_DELAY_SECONDS)
            if response.status_code == 200:
                # Use get_all to handle pagination automatically
                # Store full site data - no filtering to ensure all fields available
                sites = mistapi.get_all(self.apisession, response)
                
                # Update cache with full site data
                MistConnection._sites_cache = sites
                MistConnection._sites_cache_time = current_time
                logger.debug(f"Cached {len(sites)} sites (full data)")
                
                return sites
            else:
                raise Exception(f"API error: {response.status_code}")
        except Exception as e:
            logger.error(f"Error getting sites: {str(e)}")
            raise

    def _get_site_by_id(self, site_id: str) -> Optional[Dict]:
        """
        Get site data by ID from cached sites (includes gatewaytemplate_id).
        Uses the class-level sites cache for efficiency.
        
        Args:
            site_id: Site ID to look up
            
        Returns:
            Site dict with id, name, gatewaytemplate_id, etc. or None if not found
        """
        # Get sites from cache (will fetch if needed)
        sites = self.get_sites()
        for site in sites:
            if site.get('id') == site_id:
                return site
        return None

    def _get_device_config(self, site_id: str, device_id: str) -> Dict:
        """
        Get device configuration (port_config, gatewaytemplate_id, etc.).
        Uses Redis cache (persistent) with in-memory fallback.
        
        Args:
            site_id: Site ID where device is located
            device_id: Device ID
            
        Returns:
            Device configuration dictionary
        """
        # Check Redis cache first (persistent across restarts)
        if self._redis_cache:
            cached = self._redis_cache.get_device_config(device_id)
            if cached:
                logger.debug(f"Using Redis cached device config {device_id}")
                return cached
        
        # Fetch from API
        try:
            config_response = mistapi.api.v1.sites.devices.getSiteDevice(
                self.apisession,
                site_id,
                device_id
            )
            time.sleep(self.API_DELAY_SECONDS)
            if config_response.status_code == 200:
                config_data = config_response.data
                # Store in Redis for persistence
                if self._redis_cache:
                    self._redis_cache.set_device_config(device_id, config_data, ttl=self.TEMPLATE_CACHE_TTL)
                logger.debug(f"Fetched and cached device config {device_id}")
                return config_data
            else:
                logger.warning(f"Could not fetch device config {device_id}: {config_response.status_code}")
        except Exception as e:
            logger.warning(f"Error fetching device config {device_id}: {str(e)}")
        
        return {}

    def _batch_fetch_inventory(self, gateway_macs: set) -> Dict[str, Dict]:
        """
        Batch fetch device inventory data (device profile IDs, site IDs) using org-level inventory.
        This provides profile/template IDs without per-device API calls.
        Uses Redis cache (persistent) for the full inventory data.
        
        Args:
            gateway_macs: Set of gateway MAC addresses
            
        Returns:
            Dictionary keyed by MAC with inventory data (deviceprofile_id, site_id, etc.)
        """
        # Check Redis cache first (persistent across restarts)
        if self._redis_cache:
            cached = self._redis_cache.get_inventory()
            if cached:
                # Filter to only requested MACs
                inventory_data = {mac: data for mac, data in cached.items() if mac in gateway_macs}
                if inventory_data:
                    logger.debug(f"Using Redis cached inventory for {len(inventory_data)} gateways")
                    return inventory_data
        
        inventory_data = {}
        
        try:
            # Use org-level inventory to get device profile IDs for all gateways (with pagination)
            response = mistapi.api.v1.orgs.inventory.getOrgInventory(
                self.apisession,
                self.org_id,  # type: ignore[arg-type]
                type='gateway',
                limit=1000
            )
            time.sleep(self.API_DELAY_SECONDS)
            
            if response.status_code == 200:
                # Use get_all to handle pagination automatically
                # Store FULL inventory data - no filtering
                results = mistapi.get_all(self.apisession, response)
                
                all_inventory = {}
                for device in results:
                    mac = device.get('mac', '')
                    if not mac:
                        continue
                    
                    # Store full device data from inventory
                    all_inventory[mac] = device
                
                # Store full inventory in Redis for persistence
                if self._redis_cache and all_inventory:
                    self._redis_cache.set_inventory(all_inventory, ttl=self.TEMPLATE_CACHE_TTL)
                    logger.info(f"Cached full inventory data for {len(all_inventory)} devices")
                
                # Filter to only requested MACs for return value
                inventory_data = {mac: data for mac, data in all_inventory.items() if mac in gateway_macs}
                logger.debug(f"Batch fetched inventory for {len(inventory_data)} gateways")
            else:
                logger.warning(f"Org inventory returned {response.status_code}")
                
        except Exception as e:
            logger.warning(f"Error in batch inventory fetch: {str(e)}")
        
        return inventory_data

    def _get_device_profile(self, deviceprofile_id: str) -> Dict:
        """
        Get device profile configuration (for Hub devices).
        Uses Redis cache (persistent) with in-memory fallback.
        
        Args:
            deviceprofile_id: Device profile ID
            
        Returns:
            Device profile data dictionary
        """
        # Check in-memory cache first (fastest)
        cache_key = f"profile:{deviceprofile_id}"
        if cache_key in MistConnection._device_profile_cache:
            logger.debug(f"Using in-memory cached device profile {deviceprofile_id}")
            return MistConnection._device_profile_cache[cache_key]
        
        # Check Redis cache (persistent across restarts)
        if self._redis_cache:
            cached = self._redis_cache.get_device_profile(deviceprofile_id)
            if cached:
                logger.debug(f"Using Redis cached device profile {deviceprofile_id}")
                MistConnection._device_profile_cache[cache_key] = cached  # Warm in-memory cache
                return cached
        
        # Fetch from API
        try:
            response = mistapi.api.v1.orgs.deviceprofiles.getOrgDeviceProfile(
                self.apisession,
                self.org_id,  # type: ignore[arg-type]
                deviceprofile_id
            )
            time.sleep(self.API_DELAY_SECONDS)
            if response.status_code == 200:
                profile_data = response.data
                MistConnection._device_profile_cache[cache_key] = profile_data
                # Store in Redis for persistence
                if self._redis_cache:
                    self._redis_cache.set_device_profile(deviceprofile_id, profile_data, ttl=self.TEMPLATE_CACHE_TTL)
                logger.info(f"Fetched and cached device profile {deviceprofile_id}: {profile_data.get('name', 'unknown')}")
                return profile_data
            else:
                logger.warning(f"Could not fetch device profile {deviceprofile_id}: {response.status_code}")
        except Exception as e:
            logger.warning(f"Error fetching device profile {deviceprofile_id}: {str(e)}")
        
        MistConnection._device_profile_cache[cache_key] = {}
        return {}

    def _get_gateway_template(self, gatewaytemplate_id: str) -> Dict:
        """
        Get gateway template configuration (for Spoke/Branch devices).
        Uses Redis cache (persistent) with in-memory fallback.
        
        Args:
            gatewaytemplate_id: Gateway template ID
            
        Returns:
            Gateway template data dictionary
        """
        # Check in-memory cache first (fastest)
        cache_key = f"template:{gatewaytemplate_id}"
        if cache_key in MistConnection._gateway_template_cache:
            logger.debug(f"Using in-memory cached gateway template {gatewaytemplate_id}")
            return MistConnection._gateway_template_cache[cache_key]
        
        # Check Redis cache (persistent across restarts)
        if self._redis_cache:
            cached = self._redis_cache.get_gateway_template(gatewaytemplate_id)
            if cached:
                logger.debug(f"Using Redis cached gateway template {gatewaytemplate_id}")
                MistConnection._gateway_template_cache[cache_key] = cached  # Warm in-memory cache
                return cached
        
        # Fetch from API
        try:
            response = mistapi.api.v1.orgs.gatewaytemplates.getOrgGatewayTemplate(
                self.apisession,
                self.org_id,  # type: ignore[arg-type]
                gatewaytemplate_id
            )
            time.sleep(self.API_DELAY_SECONDS)
            if response.status_code == 200:
                template_data = response.data
                MistConnection._gateway_template_cache[cache_key] = template_data
                # Store in Redis for persistence
                if self._redis_cache:
                    self._redis_cache.set_gateway_template(gatewaytemplate_id, template_data, ttl=self.TEMPLATE_CACHE_TTL)
                logger.info(f"Fetched and cached gateway template {gatewaytemplate_id}: {template_data.get('name', 'unknown')}")
                return template_data
            else:
                logger.warning(f"Could not fetch gateway template {gatewaytemplate_id}: {response.status_code}")
        except Exception as e:
            logger.warning(f"Error fetching gateway template {gatewaytemplate_id}: {str(e)}")
        
        MistConnection._gateway_template_cache[cache_key] = {}
        return {}

    def get_gateway_stats(self, site_id: Optional[str] = None, start: Optional[int] = None, end: Optional[int] = None) -> List[Dict]:
        """
        Get gateway statistics including WAN port information
        
        API call optimization strategy:
        1. Sites: Cached at class level (1 call per 5 min)
        2. Device stats: Single org-level call
        3. Port stats: Single org-level call (includes traffic data)
        4. Device configs: Batch fetch using org-level inventory search
        5. Profiles/Templates: Cached at class level (1 call per unique profile)
        
        Args:
            site_id: Optional site ID to filter gateways
            start: Start time as Unix epoch timestamp
            end: End time as Unix epoch timestamp
            
        Returns:
            List of gateways with their WAN port statistics and configuration
        """
        try:
            if not self.org_id:
                raise ValueError("Organization ID is required")
            
            # Get site data mapping (cached) - includes gatewaytemplate_id for Branch devices
            sites = self.get_sites()
            site_map = {s['id']: s['name'] for s in sites}
            site_data_map = {s['id']: s for s in sites}  # Full site data including gatewaytemplate_id
            
            # Get gateway device stats for basic info (with pagination)
            device_response = mistapi.api.v1.orgs.stats.listOrgDevicesStats(
                self.apisession,
                self.org_id,
                type='gateway',
                limit=1000
            )
            time.sleep(self.API_DELAY_SECONDS)
            
            if device_response.status_code != 200:
                raise Exception(f"API error getting device stats: {device_response.status_code}")
            
            # Use get_all to handle pagination automatically
            gateways = mistapi.get_all(self.apisession, device_response)
            
            # Get ALL port statistics using org-level endpoint
            # This returns physical port status for all gateways
            wan_ports_by_device = {}
            all_ports_by_device = {}  # Track all ports for physical status
            
            # Build a set of gateway MACs for filtering port results
            gateway_macs = {gw.get('mac') for gw in gateways if gw.get('mac')}
            
            # Use type=gateway to only fetch gateway ports (not switches)
            # Use default 1d time range for faster initial load
            port_params = {'limit': 1000, 'type': 'gateway'}
            
            port_response = mistapi.api.v1.orgs.stats.searchOrgSwOrGwPorts(
                self.apisession,
                self.org_id,  # type: ignore[arg-type]
                **port_params  # type: ignore[arg-type]
            )
            time.sleep(self.API_DELAY_SECONDS)
            
            if port_response.status_code == 200:
                # Use get_all to handle pagination automatically
                all_ports = mistapi.get_all(self.apisession, port_response)
                for port in all_ports:
                    device_mac = port.get('mac')
                    port_id = port.get('port_id', '')
                    
                    # Only process ports belonging to gateways we care about
                    if device_mac not in gateway_macs:
                        continue
                    
                    # Store all ports for physical status lookup
                    if device_mac not in all_ports_by_device:
                        all_ports_by_device[device_mac] = {}
                    all_ports_by_device[device_mac][port_id] = port
                    
                    # Also track WAN ports separately
                    if port.get('port_usage') == 'wan':
                        if device_mac not in wan_ports_by_device:
                            wan_ports_by_device[device_mac] = []
                        wan_ports_by_device[device_mac].append(port)
            
            # Batch fetch inventory data for profile IDs (1 API call)
            inventory_map = self._batch_fetch_inventory(gateway_macs)
            
            gateway_stats = []
            
            for gw in gateways:
                # Filter by site if specified
                if site_id and gw.get('site_id') != site_id:
                    continue
                
                gw_site_id = gw.get('site_id')
                gw_id = gw.get('id')
                gw_mac = gw.get('mac')
                
                # Get site name - fallback is unlikely with proper caching
                gw_site_name = site_map.get(gw_site_id, '')
                if not gw_site_name and gw_site_id:
                    # Only log warning, don't make extra API call
                    logger.warning(f"Site {gw_site_id} not found in cached site map")
                
                # Get WAN ports for this gateway
                wan_ports = wan_ports_by_device.get(gw_mac, [])
                
                # Get all port physical status for this gateway (from org-level port stats)
                device_port_stats = all_ports_by_device.get(gw_mac, {})
                
                # Get device configuration
                port_configs = []
                wan_port_config_by_name = {}  # Map by port name (e.g., 'ge-0/0/1.30') for matching
                runtime_ips_by_port = {}  # Map of port_id -> actual runtime IP/netmask from if_stat
                
                # Get profile ID from batch inventory
                inventory_data = inventory_map.get(gw_mac, {})
                deviceprofile_id = inventory_data.get('deviceprofile_id')
                
                try:
                    # Get device configuration for port_config and gatewaytemplate_id
                    # Uses Redis cache (persistent) to avoid per-device API calls
                    device_config = {}
                    gatewaytemplate_id = None
                    if gw_site_id and gw_id:
                        device_config = self._get_device_config(gw_site_id, gw_id)
                        # Use device config's gatewaytemplate_id if not a Hub device
                        if not deviceprofile_id:
                            gatewaytemplate_id = device_config.get('gatewaytemplate_id')
                            # For Branch devices, gatewaytemplate_id is on the site object (not device config)
                            if not gatewaytemplate_id and gw_site_id in site_data_map:
                                gatewaytemplate_id = site_data_map[gw_site_id].get('gatewaytemplate_id')
                    
                    # Start with device profile OR gateway template port_config
                    # Hub devices use deviceprofile_id, Branch/Spoke devices use gatewaytemplate_id
                    merged_port_config = {}
                    if deviceprofile_id:
                        # Hub device - get config from device profile (class-level cache)
                        profile_data = self._get_device_profile(deviceprofile_id)
                        if profile_data and 'port_config' in profile_data:
                            # IMPORTANT: Use deepcopy to avoid mutating the cached template data
                            # when device-level overrides are merged below
                            merged_port_config = copy.deepcopy(profile_data.get('port_config', {}))
                            logger.debug(f"Gateway {gw_id} (Hub) using device profile {deviceprofile_id} with {len(merged_port_config)} ports")
                    elif gatewaytemplate_id:
                        # Branch/Spoke device - get config from gateway template (class-level cache)
                        template_data = self._get_gateway_template(gatewaytemplate_id)
                        if template_data and 'port_config' in template_data:
                            # IMPORTANT: Use deepcopy to avoid mutating the cached template data
                            # when device-level overrides are merged below
                            merged_port_config = copy.deepcopy(template_data.get('port_config', {}))
                            logger.debug(f"Gateway {gw_id} (Branch) using gateway template {gatewaytemplate_id} with {len(merged_port_config)} ports")
                    
                    # Merge device-level port_config (overrides profile settings)
                    # Track which ports have device-level overrides
                    device_port_config = device_config.get('port_config', {})
                    device_override_ports = set()  # Ports that exist at device level = override
                    if device_port_config:
                        for port_name, port_cfg in device_port_config.items():
                            device_override_ports.add(port_name)  # This port has device-level config
                            if port_name in merged_port_config:
                                # Merge: device config overrides profile
                                merged_port_config[port_name].update(port_cfg)
                            else:
                                merged_port_config[port_name] = port_cfg
                    
                    # Extract WAN port configurations keyed by port name
                    for port_name, port_cfg in merged_port_config.items():
                        if port_cfg.get('usage') == 'wan':
                            ip_cfg = port_cfg.get('ip_config', {})
                            description = port_cfg.get('description', '').strip()
                            vlan_id = port_cfg.get('vlan_id', '')
                            
                            # Get template type from ip_config (this is what the template says)
                            template_ip_type = ip_cfg.get('type', 'dhcp')
                            
                            wan_port_config_by_name[port_name] = {
                                'name': port_cfg.get('name', ''),
                                'description': description,
                                'ip': ip_cfg.get('ip', ''),
                                'netmask': ip_cfg.get('netmask', ''),
                                'gateway': ip_cfg.get('gateway', ''),
                                'template_type': template_ip_type,  # Store as template_type for clarity
                                'vlan_id': str(vlan_id) if vlan_id else '',
                                'disabled': port_cfg.get('disabled', False)
                            }
                    
                    # Get runtime IPs from searchSiteDevices (needed for DHCP IP addresses)
                    # SDK handles 429 rate limiting automatically with token rotation
                    if gw_site_id:
                        device_search_response = mistapi.api.v1.sites.devices.searchSiteDevices(
                            self.apisession,
                            gw_site_id,
                            type='gateway',
                            mac=gw_mac,
                            stats=True
                        )
                        time.sleep(self.API_DELAY_SECONDS)
                        
                        if device_search_response.status_code == 200:
                            search_results = device_search_response.data.get('results', [])
                            
                            # Store raw API response for debugging
                            if self._redis_cache and search_results:
                                self._redis_cache.set_raw_api_response(
                                    'searchSiteDevices', 
                                    f"{gw_id}-{gw_mac}", 
                                    device_search_response.data,
                                    ttl=self.TEMPLATE_CACHE_TTL
                                )
                            
                            if search_results and 'if_stat' in search_results[0]:
                                if_stat = search_results[0]['if_stat']
                                
                                for if_name, if_data in if_stat.items():
                                    if if_data.get('port_usage') == 'wan':
                                        port_id = if_data.get('port_id', '')
                                        ips = if_data.get('ips', [])
                                        address_mode = if_data.get('address_mode', '')
                                        
                                        # Store FULL if_data for this port (all fields from API)
                                        runtime_entry = dict(if_data)  # Copy all fields
                                        
                                        # Also parse IP/CIDR for convenience
                                        if ips and len(ips) > 0 and '/' in ips[0]:
                                            ip_cidr = ips[0]
                                            ip, cidr = ip_cidr.split('/')
                                            
                                            # Convert CIDR to netmask
                                            cidr_int = int(cidr)
                                            mask = (0xffffffff >> (32 - cidr_int)) << (32 - cidr_int)
                                            netmask = f"{(mask >> 24) & 0xff}.{(mask >> 16) & 0xff}.{(mask >> 8) & 0xff}.{mask & 0xff}"
                                            
                                            runtime_entry['ip'] = ip
                                            runtime_entry['netmask'] = netmask
                                        else:
                                            runtime_entry['ip'] = ''
                                            runtime_entry['netmask'] = ''
                                        
                                        # Always store runtime data for WAN ports
                                        runtime_ips_by_port[port_id] = runtime_entry
                except Exception as e:
                    logger.warning(f"Could not process config for gateway {gw_id}: {str(e)}")
                
                # Combine WAN port stats with IP configuration
                for port in wan_ports:
                    port_id = port.get('port_id')  # e.g., 'ge-0/0/1' or 'ge-0/0/1.30'
                    port_desc = port.get('port_desc', '').strip()
                    
                    # Match by port_id (exact match first)
                    port_config = wan_port_config_by_name.get(port_id, {})
                    
                    # If no exact match, try to find VLAN-tagged config for base interface
                    # e.g., port_id='ge-0/0/3' should match config key 'ge-0/0/3.301'
                    if not port_config:
                        for cfg_name, cfg in wan_port_config_by_name.items():
                            # Check if config key starts with port_id and has VLAN suffix
                            if cfg_name.startswith(port_id + '.'):
                                port_config = cfg
                                break
                    
                    # If still no match, try by description (fallback)
                    if not port_config and port_desc:
                        for cfg_name, cfg in wan_port_config_by_name.items():
                            if cfg.get('description') == port_desc:
                                port_config = cfg
                                break
                    
                    # If still no match, use defaults
                    if not port_config:
                        port_config = {
                            'name': '',
                            'description': port_desc,
                            'ip': '',
                            'netmask': '',
                            'gateway': '',
                            'template_type': 'dhcp',  # Default to DHCP for unconfigured ports
                            'vlan_id': '',
                            'disabled': False
                        }
                    
                    # Get runtime data
                    runtime_ip_data = runtime_ips_by_port.get(port_id, {})
                    
                    # Get template type (what the template says this port should be)
                    template_type = port_config.get('template_type', 'dhcp')
                    
                    # Get runtime type from API address_mode
                    # API returns: 'Dynamic' or 'Static' (capitalized)
                    # We normalize to: 'dhcp' or 'static' (lowercase)
                    raw_address_mode = runtime_ip_data.get('address_mode', '') if runtime_ip_data else ''
                    address_mode_map = {'dynamic': 'dhcp', 'static': 'static'}
                    runtime_type = address_mode_map.get(raw_address_mode.lower(), template_type)
                    
                    # OVERRIDE DETECTION:
                    # 1. If runtime type != template type, it's an override (e.g., template=DHCP but device=Static)
                    # 2. If port has device-level config, it's an override (site-level IP/gateway override)
                    type_override = (runtime_type != template_type)
                    device_config_override = port_id in device_override_ports
                    is_overridden = type_override or device_config_override
                    
                    # Get IP address (from runtime for DHCP, from config for static)
                    if runtime_ip_data and runtime_ip_data.get('ip'):
                        ip_addr = runtime_ip_data.get('ip', '')
                        netmask_str = runtime_ip_data.get('netmask', '')
                        # Convert dotted-decimal netmask to CIDR
                        if netmask_str and '.' in netmask_str:
                            parts = netmask_str.split('.')
                            binary = ''.join([bin(int(x)+256)[3:] for x in parts])
                            netmask = str(binary.count('1'))
                        else:
                            netmask = netmask_str
                    else:
                        # Fallback to configured IP (template or device)
                        ip_addr = port_config.get('ip', '').strip()
                        netmask = port_config.get('netmask', '').strip()
                        
                        # Remove leading slash from netmask if present (CIDR notation)
                        if netmask.startswith('/'):
                            netmask = netmask[1:]
                    
                    # Use cumulative stats from org-level port search (no per-port API calls)
                    # The searchOrgSwOrGwPorts endpoint provides traffic data for the time range
                    rx_bytes = port.get('rx_bytes', 0)
                    tx_bytes = port.get('tx_bytes', 0)
                    
                    # Build port object
                    port_obj = {
                        'name': port_id,
                        'port_id': port_id,
                        'wan_name': port_config.get('name', ''),
                        'description': port_config.get('description', port.get('port_desc', '')),
                        'enabled': not port_config.get('disabled', False),  # Admin status from config
                        'usage': 'wan',
                        'ip': ip_addr,
                        'netmask': netmask,
                        'gateway': port_config.get('gateway', ''),
                        'type': runtime_type,  # What it's actually running as
                        'template_type': template_type,  # What the template says
                        'vlan_id': port_config.get('vlan_id', ''),
                        'override': 'yes' if is_overridden else 'no',
                        'up': port.get('up', False),
                        'rx_bytes': rx_bytes,
                        'tx_bytes': tx_bytes,
                        'rx_pkts': port.get('rx_pkts', 0),
                        'tx_pkts': port.get('tx_pkts', 0),
                        'rx_errors': port.get('rx_errors', 0),
                        'tx_errors': port.get('tx_errors', 0),
                        'speed': port.get('speed', 0),
                        'mac': port.get('port_mac', '')
                    }
                    
                    # Merge ALL runtime data from searchSiteDevices if_stat (full API response)
                    if runtime_ip_data:
                        for key, value in runtime_ip_data.items():
                            if key not in port_obj:  # Don't overwrite our computed fields
                                port_obj[key] = value
                    
                    port_configs.append(port_obj)
                
                # Add WAN ports from config that don't have stats yet
                # These are configured WAN ports that may be down or not reporting stats
                ports_with_stats = set()
                for pc in port_configs:
                    ports_with_stats.add(pc.get('name', ''))
                    # Also track base interface for VLAN-tagged ports
                    port_name = pc.get('name', '')
                    if '.' in port_name:
                        base_port = port_name.split('.')[0]
                        ports_with_stats.add(base_port)
                
                for cfg_port_name, cfg in wan_port_config_by_name.items():
                    # Skip Jinja template variables (unresolved template placeholders)
                    if '{{' in cfg_port_name or '}}' in cfg_port_name:
                        continue
                    
                    # Extract base port name (e.g., 'ge-0/0/1' from 'ge-0/0/1.30')
                    base_port_name = cfg_port_name.split('.')[0] if '.' in cfg_port_name else cfg_port_name
                    
                    # Skip if we already have stats for this port
                    if cfg_port_name in ports_with_stats or base_port_name in ports_with_stats:
                        continue
                    
                    # Get template type and runtime data
                    template_type = cfg.get('template_type', 'dhcp')
                    runtime_ip_data = runtime_ips_by_port.get(base_port_name, {})
                    
                    # Get runtime type from address_mode
                    raw_address_mode = runtime_ip_data.get('address_mode', '') if runtime_ip_data else ''
                    address_mode_map = {'dynamic': 'dhcp', 'static': 'static'}
                    runtime_type = address_mode_map.get(raw_address_mode.lower(), template_type)
                    
                    # OVERRIDE DETECTION:
                    # 1. If runtime type != template type, it's an override
                    # 2. If port has device-level config, it's an override
                    type_override = (runtime_type != template_type)
                    device_config_override = cfg_port_name in device_override_ports or base_port_name in device_override_ports
                    is_overridden = type_override or device_config_override
                    
                    # Get IP address
                    if runtime_ip_data and runtime_ip_data.get('ip'):
                        ip_addr = runtime_ip_data.get('ip', '')
                        netmask_str = runtime_ip_data.get('netmask', '')
                        if netmask_str and '.' in netmask_str:
                            parts = netmask_str.split('.')
                            binary = ''.join([bin(int(x)+256)[3:] for x in parts])
                            netmask = str(binary.count('1'))
                        else:
                            netmask = netmask_str
                    else:
                        ip_addr = cfg.get('ip', '').strip()
                        netmask = cfg.get('netmask', '').strip()
                        if netmask.startswith('/'):
                            netmask = netmask[1:]
                    
                    # Get physical port status from org-level port stats
                    port_stats = device_port_stats.get(base_port_name, {})
                    physical_up = port_stats.get('up', False)
                    port_speed = port_stats.get('speed', 0)
                    
                    port_configs.append({
                        'name': base_port_name,
                        'wan_name': cfg.get('name', ''),
                        'description': cfg.get('description', ''),
                        'enabled': not cfg.get('disabled', False),  # Admin status from config
                        'usage': 'wan',
                        'ip': ip_addr,
                        'netmask': netmask,
                        'gateway': cfg.get('gateway', ''),
                        'type': runtime_type,
                        'template_type': template_type,
                        'vlan_id': cfg.get('vlan_id', ''),
                        'override': 'yes' if is_overridden else 'no',
                        'up': physical_up,
                        'rx_bytes': port_stats.get('rx_bytes', 0),
                        'tx_bytes': port_stats.get('tx_bytes', 0),
                        'rx_pkts': port_stats.get('rx_pkts', 0),
                        'tx_pkts': port_stats.get('tx_pkts', 0),
                        'rx_errors': port_stats.get('rx_errors', 0),
                        'tx_errors': port_stats.get('tx_errors', 0),
                        'speed': port_speed,
                        'mac': port_stats.get('port_mac', '')
                    })
                
                # Sort ports by name for consistent display
                port_configs.sort(key=lambda p: p.get('name', ''))
                
                gateway_stats.append({
                    'id': gw_id,
                    'name': gw.get('name', 'Unknown'),
                    'site_id': gw_site_id,
                    'site_name': gw_site_name,
                    'model': gw.get('model', ''),
                    'version': gw.get('version', ''),
                    'status': gw.get('status', 'unknown'),
                    'uptime': gw.get('uptime', 0),
                    'ip': gw.get('ip', ''),
                    'mac': gw_mac,
                    'ports': port_configs,
                    'num_ports': len(port_configs)
                })
            
            return gateway_stats
        except Exception as e:
            logger.error(f"Error getting gateway stats: {str(e)}")
            raise
    
    def get_gateway_port_stats(self, gateway_id: str) -> Dict:
        """
        Get detailed port statistics for a specific gateway
        
        Args:
            gateway_id: Gateway device ID
            
        Returns:
            Detailed port statistics
        """
        try:
            # Get gateway stats using search devices endpoint
            if not self.org_id:
                raise ValueError("Organization ID is required")
            response = mistapi.api.v1.orgs.devices.searchOrgDevices(
                self.apisession,
                self.org_id,
                type='gateway',
                mac=gateway_id
            )
            time.sleep(self.API_DELAY_SECONDS)
            
            if response.status_code == 200:
                gw = response.data
                port_stats = {}
                
                if 'port_stat' in gw:
                    for port_name, port_data in gw['port_stat'].items():
                        port_stats[port_name] = {
                            'up': port_data.get('up', False),
                            'rx_bytes': port_data.get('rx_bytes', 0),
                            'tx_bytes': port_data.get('tx_bytes', 0),
                            'rx_pkts': port_data.get('rx_pkts', 0),
                            'tx_pkts': port_data.get('tx_pkts', 0),
                            'rx_errors': port_data.get('rx_errors', 0),
                            'tx_errors': port_data.get('tx_errors', 0),
                            'rx_bps': port_data.get('rx_bps', 0),
                            'tx_bps': port_data.get('tx_bps', 0),
                            'speed': port_data.get('speed', 0),
                            'mac': port_data.get('mac', ''),
                            'full_duplex': port_data.get('full_duplex', True)
                        }
                
                return {
                    'gateway_id': gw.get('id'),
                    'gateway_name': gw.get('name', 'Unknown'),
                    'ports': port_stats,
                    'timestamp': gw.get('last_seen', 0)
                }
            else:
                raise Exception(f"API error: {response.status_code}")
        except Exception as e:
            logger.error(f"Error getting gateway port stats: {str(e)}")
            raise
    
    def get_vpn_peer_stats(self, site_id: str, device_mac: str) -> Dict:
        """
        Get VPN peer path statistics for a gateway using the mistapi SDK.
        
        Args:
            site_id: Site ID
            device_mac: Device MAC address
            
        Returns:
            Dictionary with peer path statistics grouped by port_id
        """
        try:
            if not self.org_id:
                raise ValueError("Organization ID is required")
            
            # Use SDK method: mistapi.api.v1.orgs.stats.searchOrgPeerPathStats
            response = mistapi.api.v1.orgs.stats.searchOrgPeerPathStats(
                self.apisession,
                self.org_id,
                site_id=site_id,
                mac=device_mac
            )
            time.sleep(self.API_DELAY_SECONDS)
            
            if response.status_code == 429:
                # Rate limited - report to Redis and return gracefully
                self._report_rate_limit(tokens_exhausted=self._token_count)
                logger.warning(f"VPN peer stats rate limited for device {device_mac}")
                return {
                    'success': False,
                    'rate_limited': True,
                    'peers_by_port': {},
                    'total_peers': 0
                }
            
            if response.status_code == 200:
                results = response.data.get('results', [])
                
                # Group peer paths by port_id
                peers_by_port = {}
                for peer in results:
                    port_id = peer.get('port_id', '')
                    if port_id not in peers_by_port:
                        peers_by_port[port_id] = []
                    peers_by_port[port_id].append({
                        'vpn_name': peer.get('vpn_name', ''),
                        'peer_router_name': peer.get('peer_router_name', ''),
                        'peer_port_id': peer.get('peer_port_id', ''),
                        'up': peer.get('up', False),
                        'is_active': peer.get('is_active', False),
                        'latency': peer.get('latency', 0),
                        'loss': peer.get('loss', 0),
                        'jitter': peer.get('jitter', 0),
                        'mos': peer.get('mos', 0),
                        'uptime': peer.get('uptime', 0),
                        'mtu': peer.get('mtu', 0),
                        'type': peer.get('type', ''),
                        'hop_count': peer.get('hop_count', 0)
                    })
                
                return {
                    'success': True,
                    'peers_by_port': peers_by_port,
                    'total_peers': len(results)
                }
            else:
                logger.warning(f"VPN peer stats API error {response.status_code} for device {device_mac}")
                return {
                    'success': False,
                    'peers_by_port': {},
                    'total_peers': 0
                }
        except Exception as e:
            logger.warning(f"Error fetching VPN peer stats for device {device_mac}: {str(e)}")
            return {
                'success': False,
                'peers_by_port': {},
                'total_peers': 0
            }

    def _get_port_insights(self, site_id: str, gateway_id: str, port_id: str, 
                           start: int, end: int, interval: int = 600) -> Optional[Dict]:
        """
        Fetch traffic insights for a specific gateway port.
        
        Uses direct HTTP request as SDK doesn't support this endpoint yet.
        Endpoint: /api/v1/sites/{site_id}/insights/gateway/{gateway_id}/stats
        
        Args:
            site_id: Site ID
            gateway_id: Gateway device ID (UUID format, not MAC)
            port_id: Port identifier (e.g., 'ge-0/0/0')
            start: Start timestamp (Unix epoch)
            end: End timestamp (Unix epoch)
            interval: Sample interval in seconds (default 600 = 10 minutes)
            
        Returns:
            Dict with timestamps, rx_bps, tx_bps, rx_bytes, tx_bytes or None on error
        """
        import requests
        
        try:
            # Get current token from the session
            current_token = self.api_token.split(',')[0].strip()
            try:
                tokens = getattr(self.apisession, '_apitoken', None)
                token_idx = getattr(self.apisession, '_apitoken_index', 0)
                if tokens and len(tokens) > token_idx >= 0:
                    current_token = tokens[token_idx]
            except Exception:
                pass
            
            headers = {
                'Authorization': f'Token {current_token}',
                'Content-Type': 'application/json'
            }
            
            # Correct endpoint: /api/v1/sites/{site_id}/insights/gateway/{gateway_id}/stats
            url = f'https://{self.host}/api/v1/sites/{site_id}/insights/gateway/{gateway_id}/stats'
            params = {
                'interval': interval,
                'start': start,
                'end': end,
                'port_id': port_id,
                'metrics': 'rx_bps,tx_bps'
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=30)
            time.sleep(self.API_DELAY_SECONDS)
            
            if response.status_code == 200:
                data = response.json()
                
                # API returns rx_bps and tx_bps arrays directly (values can be null)
                rx_bps_list = data.get('rx_bps', [])
                tx_bps_list = data.get('tx_bps', [])
                timestamps = data.get('rt', [])
                response_interval = data.get('interval', interval)
                
                # Convert null values to 0
                rx_bps_list = [v if v is not None else 0 for v in rx_bps_list]
                tx_bps_list = [v if v is not None else 0 for v in tx_bps_list]
                
                # Convert ISO timestamps to Unix timestamps if needed
                unix_timestamps = []
                for ts in timestamps:
                    if isinstance(ts, str):
                        # Parse ISO format: "2025-12-21T18:20:00Z"
                        from datetime import datetime
                        try:
                            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                            unix_timestamps.append(int(dt.timestamp()))
                        except Exception:
                            unix_timestamps.append(0)
                    else:
                        unix_timestamps.append(ts)
                
                # Calculate total bytes transferred from bps values
                # bytes = bps * interval_seconds / 8
                rx_bytes = sum(bps * response_interval for bps in rx_bps_list if bps) // 8
                tx_bytes = sum(bps * response_interval for bps in tx_bps_list if bps) // 8
                
                return {
                    'timestamps': unix_timestamps,
                    'rx_bps': rx_bps_list,
                    'tx_bps': tx_bps_list,
                    'rx_bytes': rx_bytes,
                    'tx_bytes': tx_bytes,
                    'interval': response_interval
                }
            elif response.status_code == 429:
                self._report_rate_limit(tokens_exhausted=self._token_count)
                logger.warning(f"Rate limited on gateway insights for {gateway_id}/{port_id}")
                return None
            else:
                logger.debug(f"Gateway insights API returned {response.status_code} for {gateway_id}/{port_id}")
                return None
                
        except Exception as e:
            logger.warning(f"Error fetching gateway insights for {gateway_id}/{port_id}: {e}")
            return None

    def get_gateway_device_list(self) -> List[Dict]:
        """
        Get basic gateway device list without port stats (FAST for large orgs).
        
        This returns just the device info without fetching port statistics,
        which can take hours for large organizations.
        
        Returns:
            List of gateway devices with basic info (id, mac, name, site_id, status)
        """
        try:
            if not self.org_id:
                raise ValueError("Organization ID is required")
            
            # Get site names mapping (cached)
            sites = self.get_sites()
            site_map = {s['id']: s['name'] for s in sites}
            
            # Get gateway device stats for basic info (with pagination)
            device_response = mistapi.api.v1.orgs.stats.listOrgDevicesStats(
                self.apisession,
                self.org_id,
                type='gateway',
                limit=1000
            )
            time.sleep(self.API_DELAY_SECONDS)
            
            if device_response.status_code != 200:
                raise Exception(f"API error getting device stats: {device_response.status_code}")
            
            # Use get_all to handle pagination automatically
            gateways = mistapi.get_all(self.apisession, device_response)
            
            # Store full gateway data - add site_name and enrichment flags but keep all API fields
            for gw in gateways:
                gw_site_id = gw.get('site_id')
                gw['site_name'] = site_map.get(gw_site_id, '')
                if 'ports' not in gw:
                    gw['ports'] = []  # Will be populated later
                gw['_basic_only'] = True  # Flag indicating port data not yet loaded
            
            logger.info(f"Retrieved full data for {len(gateways)} gateways")
            return gateways
            
        except Exception as e:
            logger.error(f"Error getting gateway device list: {str(e)}")
            raise


    def get_port_stats_batch(self, callback=None, batch_size: int = 1000) -> Dict[str, List[Dict]]:
        """
        Get port statistics in batches with progress callback.
        
        Args:
            callback: Optional function to call with progress (current, total, ports_batch)
            batch_size: Number of ports per batch (default 1000)
            
        Returns:
            Dict mapping device MAC to list of port stats
        """
        try:
            if not self.org_id:
                raise ValueError("Organization ID is required")
            
            ports_by_device = {}
            total_ports = 0
            
            # Use type=gateway to only fetch gateway ports
            port_params = {'limit': batch_size, 'type': 'gateway'}
            
            port_response = mistapi.api.v1.orgs.stats.searchOrgSwOrGwPorts(
                self.apisession,
                self.org_id,
                **port_params
            )
            time.sleep(self.API_DELAY_SECONDS)
            
            if port_response.status_code != 200:
                raise Exception(f"API error getting port stats: {port_response.status_code}")
            
            # Manual pagination to allow callbacks
            while True:
                if port_response.status_code != 200:
                    break
                    
                ports = port_response.data.get('results', [])
                if not ports:
                    break
                
                # Process this batch
                for port in ports:
                    device_mac = port.get('mac')
                    if device_mac:
                        if device_mac not in ports_by_device:
                            ports_by_device[device_mac] = []
                        ports_by_device[device_mac].append(port)
                        total_ports += 1
                
                # Call progress callback
                if callback:
                    callback(total_ports, None, ports)
                
                # Check for next page
                next_page = port_response.data.get('next')
                if not next_page:
                    break
                
                # Fetch next page
                search_after = port_response.data.get('search_after')
                if search_after:
                    port_params['search_after'] = search_after
                    port_response = mistapi.api.v1.orgs.stats.searchOrgSwOrGwPorts(
                        self.apisession,
                        self.org_id,
                        **port_params
                    )
                    time.sleep(self.API_DELAY_SECONDS)
                else:
                    break
            
            logger.info(f"Retrieved {total_ports} port stats for {len(ports_by_device)} devices")
            return ports_by_device
            
        except Exception as e:
            logger.error(f"Error getting port stats batch: {str(e)}")
            raise


    def get_port_stats_paginated(self, callback=None) -> Dict[str, List[Dict]]:
        """
        Get ALL port statistics with proper pagination using mistapi.get_all.
        
        Args:
            callback: Optional function to call with progress (ports_count, devices_count)
            
        Returns:
            Dict mapping device MAC to list of port stats
        """
        try:
            if not self.org_id:
                raise ValueError("Organization ID is required")
            
            ports_by_device = {}
            
            # Use type=gateway to only fetch gateway ports
            port_response = mistapi.api.v1.orgs.stats.searchOrgSwOrGwPorts(
                self.apisession,
                self.org_id,
                type='gateway',
                limit=1000
            )
            time.sleep(self.API_DELAY_SECONDS)
            
            if port_response.status_code != 200:
                raise Exception(f"API error getting port stats: {port_response.status_code}")
            
            # Use mistapi.get_all to handle pagination automatically
            all_ports = mistapi.get_all(self.apisession, port_response)
            
            # Group ports by device MAC
            for port in all_ports:
                device_mac = port.get('mac')
                if device_mac:
                    if device_mac not in ports_by_device:
                        ports_by_device[device_mac] = []
                    ports_by_device[device_mac].append(port)
            
            total_ports = len(all_ports)
            logger.info(f"Retrieved {total_ports} port stats for {len(ports_by_device)} devices")
            
            if callback:
                callback(total_ports, len(ports_by_device))
            
            return ports_by_device
            
        except Exception as e:
            logger.error(f"Error getting port stats: {str(e)}")
            raise

    def get_site_port_stats(self, site_id: str) -> List[Dict]:
        """
        Get port statistics for a single site using site-level API.
        
        This is faster than org-level search when processing sites incrementally,
        as it only queries ports for one site at a time.
        
        Args:
            site_id: Site ID to fetch port stats for
            
        Returns:
            List of port stats for gateways at this site
        """
        try:
            if not site_id:
                raise ValueError("Site ID is required")
            
            # Use site-level API with device_type=gateway
            port_response = mistapi.api.v1.sites.stats.searchSiteSwOrGwPorts(
                self.apisession,
                site_id,
                device_type='gateway',
                limit=1000
            )
            time.sleep(self.API_DELAY_SECONDS)
            
            if port_response.status_code != 200:
                logger.warning(f"API error getting port stats for site {site_id}: {port_response.status_code}")
                return []
            
            # Use mistapi.get_all to handle pagination automatically
            all_ports = mistapi.get_all(self.apisession, port_response)
            
            logger.debug(f"Retrieved {len(all_ports)} ports for site {site_id}")
            return all_ports
            
        except Exception as e:
            logger.error(f"Error getting site port stats for {site_id}: {str(e)}")
            return []

    def enrich_gateway_ports(self, gateway: Dict, wan_ports: List[Dict], inventory_map: Dict) -> List[Dict]:
        """
        Enrich raw WAN port stats with device config data (WAN name, IP, gateway, type, override).
        
        This fetches device/profile config and merges it with raw port stats to get
        the full port details like WAN name, static IP, gateway IP, config type, override status.
        
        Args:
            gateway: Gateway dict with id, site_id, mac
            wan_ports: List of raw WAN port stats from searchSiteSwOrGwPorts
            inventory_map: Dict of MAC -> inventory data (deviceprofile_id, etc.)
            
        Returns:
            List of enriched port dicts with all config fields
        """
        gw_id = gateway.get('id')
        gw_site_id = gateway.get('site_id')
        gw_mac = gateway.get('mac')
        
        if not gw_id or not gw_site_id:
            # Return minimally enriched ports if we can't get full config
            return self._minimal_port_enrichment(wan_ports)
        
        port_configs = []
        wan_port_config_by_name = {}
        runtime_ips_by_port = {}
        
        try:
            # Get profile ID from inventory map
            inventory_data = inventory_map.get(gw_mac, {})
            deviceprofile_id = inventory_data.get('deviceprofile_id')
            
            # Get device configuration for port_config and gatewaytemplate_id
            # Uses Redis cache (persistent) to avoid per-device API calls
            device_config = self._get_device_config(gw_site_id, gw_id)
            gatewaytemplate_id = None
            
            if not deviceprofile_id:
                gatewaytemplate_id = device_config.get('gatewaytemplate_id')
                # For Branch devices, gatewaytemplate_id is on the site object (not device config)
                if not gatewaytemplate_id:
                    site_data = self._get_site_by_id(gw_site_id)
                    if site_data:
                        gatewaytemplate_id = site_data.get('gatewaytemplate_id')
            
            # Start with device profile OR gateway template port_config
            merged_port_config = {}
            if deviceprofile_id:
                profile_data = self._get_device_profile(deviceprofile_id)
                if profile_data and 'port_config' in profile_data:
                    # IMPORTANT: Use deepcopy to avoid mutating the cached template data
                    # when device-level overrides are merged below
                    merged_port_config = copy.deepcopy(profile_data.get('port_config', {}))
            elif gatewaytemplate_id:
                template_data = self._get_gateway_template(gatewaytemplate_id)
                if template_data and 'port_config' in template_data:
                    # IMPORTANT: Use deepcopy to avoid mutating the cached template data
                    # when device-level overrides are merged below
                    merged_port_config = copy.deepcopy(template_data.get('port_config', {}))
            
            # Merge device-level port_config (overrides profile settings)
            device_port_config = device_config.get('port_config', {})
            overridden_ports = set()
            if device_port_config:
                for port_name, port_cfg in device_port_config.items():
                    overridden_ports.add(port_name)
                    if port_name in merged_port_config:
                        merged_port_config[port_name].update(port_cfg)
                    else:
                        merged_port_config[port_name] = port_cfg
            
            # Extract WAN port configurations keyed by port name
            for port_name, port_cfg in merged_port_config.items():
                if port_cfg.get('usage') == 'wan':
                    ip_cfg = port_cfg.get('ip_config', {})
                    template_ip_type = ip_cfg.get('type', 'dhcp')
                    
                    wan_port_config_by_name[port_name] = {
                        'name': port_cfg.get('name', ''),
                        'description': port_cfg.get('description', '').strip(),
                        'ip': ip_cfg.get('ip', ''),
                        'netmask': ip_cfg.get('netmask', ''),
                        'gateway': ip_cfg.get('gateway', ''),
                        'template_type': template_ip_type,
                        'vlan_id': str(port_cfg.get('vlan_id', '')) if port_cfg.get('vlan_id') else '',
                        'disabled': port_cfg.get('disabled', False)
                    }
            
            # Get runtime IPs from searchSiteDevices (needed for DHCP IP addresses)
            device_search_response = mistapi.api.v1.sites.devices.searchSiteDevices(
                self.apisession,
                gw_site_id,
                type='gateway',
                mac=gw_mac,
                stats=True
            )
            time.sleep(self.API_DELAY_SECONDS)
            
            if device_search_response.status_code == 200:
                search_results = device_search_response.data.get('results', [])
                
                # Store raw API response for debugging
                if self._redis_cache and search_results:
                    self._redis_cache.set_raw_api_response(
                        'searchSiteDevices_enrich', 
                        f"{gw_id}-{gw_mac}", 
                        device_search_response.data,
                        ttl=self.TEMPLATE_CACHE_TTL
                    )
                
                if search_results and 'if_stat' in search_results[0]:
                    if_stat = search_results[0]['if_stat']
                    
                    for if_name, if_data in if_stat.items():
                        if if_data.get('port_usage') == 'wan':
                            port_id = if_data.get('port_id', '')
                            ips = if_data.get('ips', [])
                            
                            # Store FULL if_data for this port (all fields from API)
                            runtime_entry = dict(if_data)  # Copy all fields
                            
                            # Also parse IP/CIDR for convenience
                            if ips and len(ips) > 0 and '/' in ips[0]:
                                ip_cidr = ips[0]
                                ip, cidr = ip_cidr.split('/')
                                cidr_int = int(cidr)
                                mask = (0xffffffff >> (32 - cidr_int)) << (32 - cidr_int)
                                netmask = f"{(mask >> 24) & 0xff}.{(mask >> 16) & 0xff}.{(mask >> 8) & 0xff}.{mask & 0xff}"
                                
                                runtime_entry['ip'] = ip
                                runtime_entry['netmask'] = netmask
                            else:
                                runtime_entry['ip'] = ''
                                runtime_entry['netmask'] = ''
                            
                            # Always store runtime data for WAN ports
                            runtime_ips_by_port[port_id] = runtime_entry
                                
        except Exception as e:
            logger.warning(f"Could not get config for gateway {gw_id}: {str(e)}")
            return self._minimal_port_enrichment(wan_ports)
        
        # Combine WAN port stats with IP configuration
        for port in wan_ports:
            port_id = port.get('port_id', '')
            port_desc = port.get('port_desc', '').strip()
            
            # Match by port_id (exact match first)
            port_config = wan_port_config_by_name.get(port_id, {})
            
            # If no exact match, try VLAN-tagged config for base interface
            if not port_config:
                for cfg_name, cfg in wan_port_config_by_name.items():
                    if cfg_name.startswith(port_id + '.'):
                        port_config = cfg
                        break
            
            # If still no match, try by description
            if not port_config and port_desc:
                for cfg_name, cfg in wan_port_config_by_name.items():
                    if cfg.get('description') == port_desc:
                        port_config = cfg
                        break
            
            # Get runtime data
            runtime_ip_data = runtime_ips_by_port.get(port_id, {})
            
            # Get template type (what the template says this port should be)
            template_type = port_config.get('template_type', 'dhcp') if port_config else 'dhcp'
            
            # Get runtime type from API address_mode
            # API returns: 'Dynamic' or 'Static' (capitalized)
            # We normalize to: 'dhcp' or 'static' (lowercase)
            raw_address_mode = runtime_ip_data.get('address_mode', '') if runtime_ip_data else ''
            address_mode_map = {'dynamic': 'dhcp', 'static': 'static'}
            runtime_type = address_mode_map.get(raw_address_mode.lower(), template_type)
            
            # SIMPLE OVERRIDE DETECTION:
            # If runtime type != template type, it's an override
            is_overridden = (runtime_type != template_type)
            
            # Get IP address
            if runtime_ip_data and runtime_ip_data.get('ip'):
                ip_addr = runtime_ip_data.get('ip', '')
                netmask_str = runtime_ip_data.get('netmask', '')
                if netmask_str and '.' in netmask_str:
                    parts = netmask_str.split('.')
                    binary = ''.join([bin(int(x)+256)[3:] for x in parts])
                    netmask = str(binary.count('1'))
                else:
                    netmask = netmask_str
            else:
                ip_addr = port_config.get('ip', '').strip() if port_config else ''
                netmask = port_config.get('netmask', '').strip() if port_config else ''
                if netmask.startswith('/'):
                    netmask = netmask[1:]
            
            # Build port object
            port_obj = {
                'name': port_id,
                'port_id': port_id,
                'wan_name': port_config.get('name', '') if port_config else '',
                'description': port_config.get('description', port_desc) if port_config else port_desc,
                'enabled': not (port_config.get('disabled', False) if port_config else False),  # Admin status from config
                'usage': 'wan',
                'ip': ip_addr,
                'netmask': netmask,
                'gateway': port_config.get('gateway', '') if port_config else '',
                'type': runtime_type,  # What it's actually running as
                'template_type': template_type,  # What the template says
                'vlan_id': port_config.get('vlan_id', '') if port_config else '',
                'override': 'yes' if is_overridden else 'no',
                'up': port.get('up', False),
                'rx_bytes': port.get('rx_bytes', 0),
                'tx_bytes': port.get('tx_bytes', 0),
                'rx_pkts': port.get('rx_pkts', 0),
                'tx_pkts': port.get('tx_pkts', 0),
                'speed': port.get('speed', 0),
                'mac': port.get('port_mac', '')
            }
            
            # Merge ALL runtime data from searchSiteDevices if_stat (full API response)
            if runtime_ip_data:
                for key, value in runtime_ip_data.items():
                    if key not in port_obj:  # Don't overwrite our computed fields
                        port_obj[key] = value
            
            port_configs.append(port_obj)
        
        # Sort ports by name
        port_configs.sort(key=lambda p: p.get('name', ''))
        return port_configs
    
    def _minimal_port_enrichment(self, wan_ports: List[Dict]) -> List[Dict]:
        """
        Minimal enrichment when we can't get full config data.
        Maps raw API fields to frontend-expected fields.
        """
        enriched = []
        for port in wan_ports:
            port_id = port.get('port_id', '')
            enriched.append({
                'name': port_id,
                'port_id': port_id,
                'wan_name': '',
                'description': port.get('port_desc', ''),
                'enabled': True,  # Assume enabled when no config available
                'usage': 'wan',
                'ip': '',
                'netmask': '',
                'gateway': '',
                'type': 'unknown',
                'vlan_id': '',
                'override': '',
                'up': port.get('up', False),
                'rx_bytes': port.get('rx_bytes', 0),
                'tx_bytes': port.get('tx_bytes', 0),
                'rx_pkts': port.get('rx_pkts', 0),
                'tx_pkts': port.get('tx_pkts', 0),
                'speed': port.get('speed', 0),
                'mac': port.get('port_mac', '')
            })
        return enriched
