"""Offline tests for Redis serialization and cache behavior."""

import fnmatch
import json

from redis_cache import RedisCache


class InMemoryRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.ttls: dict[str, int] = {}
        self.connected = True

    def setex(self, key: str, ttl: int, value: str) -> None:
        self.values[key] = value
        self.ttls[key] = ttl

    def set(self, key: str, value: str) -> None:
        self.values[key] = value

    def get(self, key: str | bytes) -> bytes | None:
        normalized_key = key.decode() if isinstance(key, bytes) else key
        value = self.values.get(normalized_key)
        return value.encode() if value is not None else None

    def keys(self, pattern: str) -> list[bytes]:
        return [key.encode() for key in self.values if fnmatch.fnmatch(key, pattern)]

    def exists(self, key: str) -> int:
        return int(key in self.values)

    def delete(self, *keys: bytes | str) -> int:
        deleted = 0
        for key in keys:
            normalized_key = key.decode() if isinstance(key, bytes) else key
            if normalized_key in self.values:
                del self.values[normalized_key]
                self.ttls.pop(normalized_key, None)
                deleted += 1
        return deleted

    def ping(self) -> bool:
        return self.connected


def make_cache() -> tuple[RedisCache, InMemoryRedis]:
    cache = RedisCache.__new__(RedisCache)
    client = InMemoryRedis()
    cache.client = client
    return cache, client


def test_gateway_data_round_trips_json_and_sets_ttl() -> None:
    cache, client = make_cache()
    gateways = [{"id": "gw-1", "ports": [{"name": "wan0", "up": True}]}]

    assert cache.set_gateways(gateways, ttl=120)

    assert client.ttls[cache.PREFIX_GATEWAYS] == 120
    assert client.get(cache.PREFIX_GATEWAYS) == json.dumps(gateways).encode()
    assert cache.get_gateways() == gateways
    assert cache.is_cache_valid()


def test_insights_unknown_resolution_uses_seven_day_cache_key() -> None:
    cache, client = make_cache()
    insights = {"timestamps": [1, 2], "rx_bps": [3, 4]}

    assert cache.set_insights_by_resolution("gw-1", "wan0", "invalid", insights)

    assert cache.get_insights_by_resolution("gw-1", "wan0", "invalid") == insights
    assert "mist:insights:gw-1:wan0:7d" in client.values


def test_vpn_peers_setter_and_getter_share_worker_key_format() -> None:
    cache, client = make_cache()
    peers = {"peers_by_port": {"wan0": [{"peer": "hub-1"}]}}

    assert cache.set_vpn_peers("gw-1", "aabbccddeeff", peers, ttl=60)

    assert list(client.values) == ["mist:vpn_peers:gw-1-aabbccddeeff"]
    assert client.ttls["mist:vpn_peers:gw-1-aabbccddeeff"] == 60
    assert cache.get_vpn_peers("gw-1", "aabbccddeeff") == peers
    assert cache.get_vpn_peers("gw-1", "001122334455") is None


def test_cache_stats_counts_ports_peers_and_skips_malformed_entries() -> None:
    cache, client = make_cache()
    cache.set_gateways(
        [
            {"status": "connected", "ports": [{"up": True}, {"up": False}]},
            {"status": "disconnected", "ports": [{"up": True}]},
            {},
        ]
    )
    cache.set_sites([{"id": "site"}])
    cache.set_organization({"id": "org"})
    cache.set_last_update(123.0)
    cache.set_worker_status("idle")
    cache.set_vpn_peers(
        "gw", "mac", {"peers_by_port": {"wan0": [{}, {}], "wan1": [{}]}}
    )
    client.values["mist:vpn_peers:bad"] = "not json"
    client.values["mist:vpn_peers:wrong-shape"] = "[]"
    cache.set_insights("gw", "wan0", {"timestamps": [1]})
    cache.set_device_profile("p", {})
    cache.set_gateway_template("t", {})
    stats = cache.get_cache_stats()
    assert {
        key: stats[key]
        for key in (
            "gateways_count",
            "connected_count",
            "total_ports",
            "active_ports",
            "sites_count",
            "vpn_peers_count",
            "insights_count",
            "profiles_count",
            "templates_count",
        )
    } == {
        "gateways_count": 3,
        "connected_count": 1,
        "total_ports": 3,
        "active_ports": 2,
        "sites_count": 1,
        "vpn_peers_count": 3,
        "insights_count": 1,
        "profiles_count": 1,
        "templates_count": 1,
    }
    assert stats["has_org"] and stats["has_sites"] and stats["has_gateways"]
    assert stats["last_update"] == 123.0
    assert stats["worker_status"]["status"] == "idle"


def test_cache_stats_empty_and_client_failure() -> None:
    cache, client = make_cache()
    stats = cache.get_cache_stats()
    assert (
        stats["gateways_count"] == stats["total_ports"] == stats["vpn_peers_count"] == 0
    )
    assert stats["last_update"] is None
    assert stats["worker_status"] is None
    client.values[cache.PREFIX_GATEWAYS] = "{}"
    client.keys = lambda _pattern: (_ for _ in ()).throw(
        RuntimeError("offline failure")
    )
    assert cache.get_cache_stats() == {}
