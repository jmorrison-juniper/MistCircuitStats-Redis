"""
Mist API Connection Wrapper
Handles all interactions with the Juniper Mist API using mistapi SDK

The mistapi SDK natively supports:
- Multiple API tokens (comma-separated in MIST_APITOKEN)
- Automatic 429 rate limit detection
- Automatic token rotation on rate limit
- Token validation (all tokens must have same org privileges)
"""
import logging
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
    
    def __init__(self, api_token: str, org_id: Optional[str] = None, host: str = 'api.mist.com'):
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
                        continue
                    else:
                        logger.warning(f"Token {idx + 1}/{len(token_list)} returned {test_response.status_code}")
                        last_error = Exception(f"Token {idx + 1} returned {test_response.status_code}")
                        continue
                        
                except SystemExit:
                    logger.warning(f"Token {idx + 1}/{len(token_list)} caused SDK to exit")
                    last_error = Exception(f"Token {idx + 1} caused SDK exit")
                    continue
                except Exception as e:
                    logger.warning(f"Token {idx + 1}/{len(token_list)} failed: {e}")
                    last_error = e
                    continue
            else:
                # No token worked
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
    
    def _auto_detect_org(self):
        """Auto-detect organization ID from user privileges"""
        try:
            # Use cached self data if available (from init token validation)
            if self._self_data:
                data = self._self_data
            else:
                response = mistapi.api.v1.self.self.getSelf(self.apisession)
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
            if response.status_code == 200:
                # Use get_all to handle pagination automatically
                sites = mistapi.get_all(self.apisession, response)
                result = [{
                    'id': site.get('id'),
                    'name': site.get('name'),
                    'address': site.get('address', ''),
                    'timezone': site.get('timezone', 'UTC'),
                    'num_devices': site.get('num_devices', 0)
                } for site in sites]
                
                # Update cache
                MistConnection._sites_cache = result
                MistConnection._sites_cache_time = current_time
                logger.debug(f"Cached {len(result)} sites")
                
                return result
            else:
                raise Exception(f"API error: {response.status_code}")
        except Exception as e:
            logger.error(f"Error getting sites: {str(e)}")
            raise

    def _batch_fetch_inventory(self, gateway_macs: set) -> Dict[str, Dict]:
        """
        Batch fetch device inventory data (device profile IDs, site IDs) using org-level inventory.
        This provides profile/template IDs without per-device API calls.
        
        Args:
            gateway_macs: Set of gateway MAC addresses
            
        Returns:
            Dictionary keyed by MAC with inventory data (deviceprofile_id, site_id, etc.)
        """
        inventory_data = {}
        
        try:
            # Use org-level inventory to get device profile IDs for all gateways (with pagination)
            response = mistapi.api.v1.orgs.inventory.getOrgInventory(
                self.apisession,
                self.org_id,  # type: ignore[arg-type]
                type='gateway',
                limit=1000
            )
            
            if response.status_code == 200:
                # Use get_all to handle pagination automatically
                results = mistapi.get_all(self.apisession, response)
                
                for device in results:
                    mac = device.get('mac', '')
                    if mac not in gateway_macs:
                        continue
                    
                    # Extract inventory data
                    inventory_data[mac] = {
                        'device_id': device.get('id'),
                        'site_id': device.get('site_id'),
                        'deviceprofile_id': device.get('deviceprofile_id'),
                        'name': device.get('name', '')
                    }
                
                logger.debug(f"Batch fetched inventory for {len(inventory_data)} gateways")
            else:
                logger.warning(f"Org inventory returned {response.status_code}")
                
        except Exception as e:
            logger.warning(f"Error in batch inventory fetch: {str(e)}")
        
        return inventory_data

    def _get_device_profile(self, deviceprofile_id: str) -> Dict:
        """
        Get device profile configuration (for Hub devices), using class-level cache
        
        Args:
            deviceprofile_id: Device profile ID
            
        Returns:
            Device profile data dictionary
        """
        cache_key = f"profile:{deviceprofile_id}"
        if cache_key in MistConnection._device_profile_cache:
            logger.debug(f"Using cached device profile {deviceprofile_id}")
            return MistConnection._device_profile_cache[cache_key]
        
        try:
            response = mistapi.api.v1.orgs.deviceprofiles.getOrgDeviceProfile(
                self.apisession,
                self.org_id,  # type: ignore[arg-type]
                deviceprofile_id
            )
            if response.status_code == 200:
                MistConnection._device_profile_cache[cache_key] = response.data
                logger.debug(f"Fetched and cached device profile {deviceprofile_id}: {response.data.get('name', 'unknown')}")
                return response.data
            else:
                logger.warning(f"Could not fetch device profile {deviceprofile_id}: {response.status_code}")
        except Exception as e:
            logger.warning(f"Error fetching device profile {deviceprofile_id}: {str(e)}")
        
        MistConnection._device_profile_cache[cache_key] = {}
        return {}

    def _get_gateway_template(self, gatewaytemplate_id: str) -> Dict:
        """
        Get gateway template configuration (for Spoke/Branch devices), using class-level cache
        
        Args:
            gatewaytemplate_id: Gateway template ID
            
        Returns:
            Gateway template data dictionary
        """
        cache_key = f"template:{gatewaytemplate_id}"
        if cache_key in MistConnection._gateway_template_cache:
            logger.debug(f"Using cached gateway template {gatewaytemplate_id}")
            return MistConnection._gateway_template_cache[cache_key]
        
        try:
            response = mistapi.api.v1.orgs.gatewaytemplates.getOrgGatewayTemplate(
                self.apisession,
                self.org_id,  # type: ignore[arg-type]
                gatewaytemplate_id
            )
            if response.status_code == 200:
                MistConnection._gateway_template_cache[cache_key] = response.data
                logger.debug(f"Fetched and cached gateway template {gatewaytemplate_id}: {response.data.get('name', 'unknown')}")
                return response.data
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
            
            port_params = {'limit': 1000}
            if start is not None:
                port_params['start'] = start
            if end is not None:
                port_params['end'] = end
            
            port_response = mistapi.api.v1.orgs.stats.searchOrgSwOrGwPorts(
                self.apisession,
                self.org_id,  # type: ignore[arg-type]
                **port_params  # type: ignore[arg-type]
            )
            
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
                    # This is needed per-device but profile fetches are cached
                    # SDK handles 429 rate limiting automatically with token rotation
                    device_config = {}
                    gatewaytemplate_id = None
                    if gw_site_id and gw_id:
                        config_response = mistapi.api.v1.sites.devices.getSiteDevice(
                            self.apisession,
                            gw_site_id,
                            gw_id
                        )
                        if config_response.status_code == 200:
                            device_config = config_response.data
                            # Use device config's gatewaytemplate_id if not a Hub device
                            if not deviceprofile_id:
                                gatewaytemplate_id = device_config.get('gatewaytemplate_id')
                    
                    # Start with device profile OR gateway template port_config
                    # Hub devices use deviceprofile_id, Branch/Spoke devices use gatewaytemplate_id
                    merged_port_config = {}
                    if deviceprofile_id:
                        # Hub device - get config from device profile (class-level cache)
                        profile_data = self._get_device_profile(deviceprofile_id)
                        if profile_data and 'port_config' in profile_data:
                            merged_port_config = dict(profile_data.get('port_config', {}))
                            logger.debug(f"Gateway {gw_id} (Hub) using device profile {deviceprofile_id} with {len(merged_port_config)} ports")
                    elif gatewaytemplate_id:
                        # Branch/Spoke device - get config from gateway template (class-level cache)
                        template_data = self._get_gateway_template(gatewaytemplate_id)
                        if template_data and 'port_config' in template_data:
                            merged_port_config = dict(template_data.get('port_config', {}))
                            logger.debug(f"Gateway {gw_id} (Branch) using gateway template {gatewaytemplate_id} with {len(merged_port_config)} ports")
                    
                    # Merge device-level port_config (overrides profile settings)
                    # Track which ports have device-level overrides
                    device_port_config = device_config.get('port_config', {})
                    overridden_ports = set()  # Ports that exist at device level = override
                    if device_port_config:
                        for port_name, port_cfg in device_port_config.items():
                            overridden_ports.add(port_name)  # This port has device-level config
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
                            
                            # Port is overridden if it exists in device-level config
                            is_overridden = port_name in overridden_ports
                            
                            wan_port_config_by_name[port_name] = {
                                'name': port_cfg.get('name', ''),
                                'description': description,
                                'ip': ip_cfg.get('ip', ''),
                                'netmask': ip_cfg.get('netmask', ''),
                                'gateway': ip_cfg.get('gateway', ''),
                                'type': ip_cfg.get('type', 'dhcp'),
                                'vlan_id': str(vlan_id) if vlan_id else '',
                                'override': 'yes' if is_overridden else 'no',
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
                        
                        if device_search_response.status_code == 200:
                            search_results = device_search_response.data.get('results', [])
                            if search_results and 'if_stat' in search_results[0]:
                                if_stat = search_results[0]['if_stat']
                                
                                for if_name, if_data in if_stat.items():
                                    if if_data.get('port_usage') == 'wan':
                                        port_id = if_data.get('port_id', '')
                                        ips = if_data.get('ips', [])
                                        
                                        # Parse IP/CIDR notation (e.g., "192.168.20.2/24")
                                        if ips and len(ips) > 0 and '/' in ips[0]:
                                            ip_cidr = ips[0]
                                            ip, cidr = ip_cidr.split('/')
                                            
                                            # Convert CIDR to netmask
                                            cidr_int = int(cidr)
                                            mask = (0xffffffff >> (32 - cidr_int)) << (32 - cidr_int)
                                            netmask = f"{(mask >> 24) & 0xff}.{(mask >> 16) & 0xff}.{(mask >> 8) & 0xff}.{mask & 0xff}"
                                            
                                            runtime_ips_by_port[port_id] = {
                                                'ip': ip,
                                                'netmask': netmask,
                                                'address_mode': if_data.get('address_mode', 'Unknown')
                                            }
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
                            'type': 'dhcp',  # Default to DHCP for unconfigured ports
                            'vlan_id': '',
                            'override': 'no',
                            'disabled': False
                        }
                    
                    # Get actual runtime IP (prioritize runtime IP for DHCP ports)
                    runtime_ip_data = runtime_ips_by_port.get(port_id, {})
                    
                    # Use runtime IP if available (for DHCP ports)
                    if runtime_ip_data and port_config.get('type') == 'dhcp':
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
                        # Use configured static IP
                        ip_addr = port_config.get('ip', '').strip()
                        netmask = port_config.get('netmask', '').strip()
                        
                        # Remove leading slash from netmask if present (CIDR notation)
                        if netmask.startswith('/'):
                            netmask = netmask[1:]
                    
                    # Use cumulative stats from org-level port search (no per-port API calls)
                    # The searchOrgSwOrGwPorts endpoint provides traffic data for the time range
                    rx_bytes = port.get('rx_bytes', 0)
                    tx_bytes = port.get('tx_bytes', 0)
                    
                    port_configs.append({
                        'name': port_id,
                        'wan_name': port_config.get('name', ''),  # WAN name from config (e.g., 'BB-Hub-1')
                        'description': port_config.get('description', port.get('port_desc', '')),
                        'enabled': port.get('up', False) and not port_config.get('disabled', False),
                        'usage': 'wan',
                        # IP Configuration (runtime for DHCP, configured for static)
                        'ip': ip_addr,
                        'netmask': netmask,
                        'gateway': port_config.get('gateway', ''),
                        'type': port_config.get('type', 'unknown'),
                        'vlan_id': port_config.get('vlan_id', ''),
                        'override': port_config.get('override', 'no'),
                        # Statistics from org-level port search
                        'up': port.get('up', False),
                        'rx_bytes': rx_bytes,
                        'tx_bytes': tx_bytes,
                        'rx_pkts': port.get('rx_pkts', 0),
                        'tx_pkts': port.get('tx_pkts', 0),
                        'rx_errors': port.get('rx_errors', 0),
                        'tx_errors': port.get('tx_errors', 0),
                        'speed': port.get('speed', 0),
                        'mac': port.get('port_mac', '')
                    })
                
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
                    
                    # Get runtime IP if available
                    runtime_ip_data = runtime_ips_by_port.get(base_port_name, {})
                    if runtime_ip_data and cfg.get('type') == 'dhcp':
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
                        'enabled': physical_up and not cfg.get('disabled', False),
                        'usage': 'wan',
                        'ip': ip_addr,
                        'netmask': netmask,
                        'gateway': cfg.get('gateway', ''),
                        'type': cfg.get('type', 'unknown'),
                        'vlan_id': cfg.get('vlan_id', ''),
                        'override': cfg.get('override', 'no'),
                        'up': physical_up,  # Physical status from org port stats
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
        Get VPN peer path statistics for a gateway
        
        Note: This uses direct requests as mistapi doesn't have a dedicated
        VPN peer search method. The SDK's rate limiting doesn't apply here,
        but 429 responses are handled gracefully.
        
        Args:
            site_id: Site ID
            device_mac: Device MAC address
            
        Returns:
            Dictionary with peer path statistics grouped by port_id
        """
        import requests as http_requests
        
        try:
            if not self.org_id:
                raise ValueError("Organization ID is required")
            
            # Get the first token for direct API calls
            # For multi-token, SDK handles rotation but we only have access to the session's current token
            current_token = self.apisession._apitoken[self.apisession._apitoken_index] if hasattr(self.apisession, '_apitoken') and self.apisession._apitoken else self.api_token.split(',')[0].strip()
            
            headers = {
                'Authorization': f'Token {current_token}',
                'Content-Type': 'application/json'
            }
            
            url = f'https://{self.host}/api/v1/orgs/{self.org_id}/stats/vpn_peers/search'
            params = {
                'site_id': site_id,
                'mac': device_mac
            }
            
            response = http_requests.get(url, headers=headers, params=params)
            
            if response.status_code == 429:
                # Rate limited - return gracefully
                logger.warning(f"VPN peer stats rate limited for device {device_mac}")
                return {
                    'success': False,
                    'rate_limited': True,
                    'peers_by_port': {},
                    'total_peers': 0
                }
            
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                
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
