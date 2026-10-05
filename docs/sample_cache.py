"""Fictional cache data for screenshots; never connects to Mist or Redis."""

from copy import deepcopy
from typing import Any

import app as web


class SampleCache:
    """Read-only cache substitute used only by the documentation preview."""

    def __init__(self) -> None:
        self.sites = [
            {
                "id": "sample-site-1",
                "name": "Sample Seattle",
                "gatewaytemplate_id": "sample",
            },
            {
                "id": "sample-site-2",
                "name": "Sample Denver",
                "gatewaytemplate_id": "sample",
            },
        ]
        self.gateways = [
            self._gateway(1, "Sample Seattle", True),
            self._gateway(2, "Sample Denver", False),
        ]
        self.peers = {
            "sample-gw-1-020000000001": {
                "peers_by_port": {
                    "wan0": [
                        {
                            "up": True,
                            "is_active": True,
                            "vpn_name": "Sample Hub VPN",
                            "peer_router_name": "Sample Hub",
                            "peer_port_id": "wan0",
                            "type": "ipsec",
                            "latency": 12.4,
                            "loss": 0.02,
                            "jitter": 2.1,
                            "mos": 4.4,
                            "uptime": 172800,
                            "mtu": 1500,
                            "hop_count": 3,
                        }
                    ]
                }
            }
        }
        self.traffic = {
            "timestamps": [1791158400 + i * 3600 for i in range(168)],
            "rx_bps": [8000000 + (i % 12) * 600000 for i in range(168)],
            "tx_bps": [2000000 + (i % 8) * 300000 for i in range(168)],
            "rx_bytes": 850000000000,
            "tx_bytes": 210000000000,
            "interval": 3600,
        }

    @staticmethod
    def _gateway(number: int, site_name: str, connected: bool) -> dict[str, Any]:
        return {
            "id": f"sample-gw-{number}",
            "mac": f"02000000000{number}",
            "name": f"Sample Branch {number}",
            "site_id": f"sample-site-{number}",
            "site_name": site_name,
            "model": "SSR120",
            "status": "connected" if connected else "disconnected",
            "ip": f"192.0.2.{number}",
            "uptime": 172800 if connected else 0,
            "ports": [
                {
                    "name": "wan0",
                    "wan_name": "Sample Internet",
                    "description": "Fictional documentation circuit",
                    "enabled": True,
                    "up": connected,
                    "gateway": "198.51.100.1",
                    "ip": f"198.51.100.{number + 1}",
                    "netmask": 24,
                    "type": "dhcp",
                }
            ],
        }

    def get_organization(self) -> dict[str, str]:
        return {"id": "sample-org", "org_name": "Offline Sample Organization"}

    def get_sites(self) -> list[dict[str, str]]:
        return deepcopy(self.sites)

    def get_gateways(self) -> list[dict[str, Any]]:
        return deepcopy(self.gateways)

    def get_all_gateway_templates(self) -> dict[str, Any]:
        return {"sample": {"port_config": {"wan0": {"ip_config": {"type": "dhcp"}}}}}

    def get_all_vpn_peers(self) -> dict[str, Any]:
        return deepcopy(self.peers)

    def get_all_insights(self) -> dict[str, Any]:
        return {"sample-gw-1": {"wan0": deepcopy(self.traffic)}}

    def get_insights_by_resolution(
        self, gateway_id: str, port_id: str, resolution: str
    ) -> dict[str, Any]:
        if gateway_id != "sample-gw-1" or port_id != "wan0":
            return {}
        data = deepcopy(self.traffic)
        count = {"1h": 60, "6h": 180, "1d": 144, "7d": 168}[resolution]
        interval = {"1h": 60, "6h": 120, "1d": 600, "7d": 3600}[resolution]
        data["timestamps"] = [1791759600 - (count - i) * interval for i in range(count)]
        data["rx_bps"] = [8000000 + (i % 12) * 600000 for i in range(count)]
        data["tx_bps"] = [2000000 + (i % 8) * 300000 for i in range(count)]
        data["interval"] = interval
        return data

    def get_insights(self, gateway_id: str, port_id: str) -> dict[str, Any]:
        return self.get_insights_by_resolution(gateway_id, port_id, "7d")

    def get_loading_phase(self) -> None:
        return None

    def is_cache_valid(self) -> bool:
        return True

    def get_worker_status(self) -> dict[str, str]:
        return {"status": "idle"}

    def get_last_update(self) -> int:
        return 1791759600

    def get_rate_limit_status(self) -> dict[str, bool]:
        return {"is_limited": False}

    def get_cache_stats(self) -> dict[str, Any]:
        return {
            "gateways_count": 2,
            "connected_count": 1,
            "sites_count": 2,
            "total_ports": 2,
            "active_ports": 1,
            "vpn_peers_count": 1,
            "insights_count": 1,
            "profiles_count": 0,
            "templates_count": 1,
            "worker_status": self.get_worker_status(),
            "last_update": self.get_last_update(),
        }


def create_preview() -> Any:
    """Serve the real Flask routes and template with a sample cache."""
    web.cache = SampleCache()
    return web.app


if __name__ == "__main__":
    create_preview().run(host="127.0.0.1", port=5055, debug=False)
