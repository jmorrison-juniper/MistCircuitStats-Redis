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

    def get(self, key: str) -> bytes | None:
        value = self.values.get(key)
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
