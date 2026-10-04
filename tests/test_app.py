"""Offline tests for the Redis-backed web application."""

from collections.abc import Iterator

import pytest
from flask.testing import FlaskClient

import app as web


class StubCache:
    def __init__(self) -> None:
        self.gateways = [
            {"id": "gw-1", "site_id": "site-1"},
            {"id": "gw-2", "site_id": "site-2"},
        ]
        self.organization = {"id": "org-1"}
        self.sites = [{"id": "site-1"}]
        self.resolution_data: dict[str, object] = {}
        self.default_insights: dict[str, object] = {}
        self.fail_on: set[str] = set()
        self.ping_result = True

    def _fail_if_requested(self, method: str) -> None:
        if method in self.fail_on:
            raise RuntimeError(f"{method} unavailable")

    def get_gateways(self) -> list[dict[str, str]]:
        self._fail_if_requested("get_gateways")
        return self.gateways

    @property
    def client(self) -> "StubCache":
        return self

    def ping(self) -> bool:
        return self.ping_result

    def get_organization(self) -> dict[str, str] | None:
        self._fail_if_requested("get_organization")
        return self.organization

    def get_sites(self) -> list[dict[str, str]] | None:
        self._fail_if_requested("get_sites")
        return self.sites

    def get_insights_by_resolution(
        self, _gateway_id: str, _port_id: str, _resolution: str
    ) -> dict[str, object]:
        self._fail_if_requested("get_insights_by_resolution")
        return self.resolution_data

    def get_insights(self, _gateway_id: str, _port_id: str) -> dict[str, object]:
        self._fail_if_requested("get_insights")
        return self.default_insights

    def is_cache_valid(self) -> bool:
        self._fail_if_requested("is_cache_valid")
        return True


@pytest.fixture
def client_and_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[FlaskClient, StubCache]]:
    cache = StubCache()
    monkeypatch.setattr(web, "get_cache", lambda: cache)
    was_testing = web.app.config["TESTING"]
    web.app.config["TESTING"] = True
    try:
        yield web.app.test_client(), cache
    finally:
        web.app.config["TESTING"] = was_testing


def test_gateway_list_and_site_filter(
    client_and_cache: tuple[FlaskClient, StubCache],
) -> None:
    client, _cache = client_and_cache

    all_gateways = client.get("/api/gateways")
    site_gateways = client.get("/api/gateways?site_id=site-2")

    assert all_gateways.status_code == 200
    assert [gateway["id"] for gateway in all_gateways.json["data"]] == ["gw-1", "gw-2"]
    assert site_gateways.status_code == 200
    assert site_gateways.json["data"] == [{"id": "gw-2", "site_id": "site-2"}]


@pytest.mark.parametrize(
    ("endpoint", "cache_attribute"),
    (("/api/organization", "organization"), ("/api/sites", "sites")),
)
def test_missing_cached_data_returns_not_found(
    client_and_cache: tuple[FlaskClient, StubCache],
    endpoint: str,
    cache_attribute: str,
) -> None:
    client, cache = client_and_cache
    setattr(cache, cache_attribute, None)

    response = client.get(endpoint)

    assert response.status_code == 404
    assert response.json["success"] is False
    assert "error" in response.json


def test_cache_errors_are_reported_to_api_clients(
    client_and_cache: tuple[FlaskClient, StubCache],
) -> None:
    client, cache = client_and_cache
    cache.fail_on.add("get_organization")

    response = client.get("/api/organization")

    assert response.status_code == 500
    assert response.json == {
        "success": False,
        "error": "get_organization unavailable",
    }


@pytest.mark.parametrize(
    ("duration", "expected_resolution"),
    (("1h", "1h"), ("6h", "6h"), ("1d", "1d"), ("24h", "1d"), ("7d", "7d")),
)
def test_traffic_duration_selects_resolution(
    client_and_cache: tuple[FlaskClient, StubCache],
    duration: str,
    expected_resolution: str,
) -> None:
    client, cache = client_and_cache
    cache.resolution_data = {
        "timestamps": [100],
        "rx_bps": [10],
        "tx_bps": [20],
        "interval": 60,
    }

    response = client.get(f"/api/gateway/gw-1/port/wan0/traffic?duration={duration}")

    assert response.status_code == 200
    assert response.json["data"]["resolution"] == expected_resolution
    assert response.json["data"]["timestamps"] == [100]


def test_unknown_duration_uses_seven_day_data_and_falls_back(
    client_and_cache: tuple[FlaskClient, StubCache],
) -> None:
    client, cache = client_and_cache
    cache.default_insights = {"timestamps": [200], "rx_bps": [30], "tx_bps": [40]}

    response = client.get("/api/gateway/gw-1/port/wan0/traffic?duration=unknown")

    assert response.status_code == 200
    assert response.json["data"]["resolution"] == "7d"
    assert response.json["data"]["timestamps"] == [200]
    assert response.json["data"]["rx_bytes"] == 0


def test_traffic_without_cached_data_returns_empty_series(
    client_and_cache: tuple[FlaskClient, StubCache],
) -> None:
    client, _cache = client_and_cache

    response = client.get("/api/gateway/gw-1/port/wan0/traffic?duration=1h")

    assert response.status_code == 200
    assert response.json["data"] == {
        "timestamps": [],
        "rx_bps": [],
        "tx_bps": [],
        "rx_bytes": 0,
        "tx_bytes": 0,
        "resolution": "1h",
        "interval": 0,
    }


def test_health_check_reports_redis_unavailable(
    client_and_cache: tuple[FlaskClient, StubCache],
) -> None:
    client, cache = client_and_cache
    cache.ping_result = False

    response = client.get("/health")

    assert response.status_code == 503
    assert response.json["status"] == "unhealthy"
    assert response.json["redis"] == "disconnected"
