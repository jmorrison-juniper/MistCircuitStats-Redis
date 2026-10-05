"""Characterization of Mist operations using only fake SDK/HTTP responses."""

import copy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import requests

import mist_connection
from mist_connection import MistConnection


def response(status: int = 200, data=None):
    return SimpleNamespace(status_code=status, data=data)


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setattr(mist_connection.time, "sleep", Mock())
    monkeypatch.setattr(requests, "get", Mock(side_effect=AssertionError("Live HTTP")))
    monkeypatch.setattr(
        mist_connection.mistapi,
        "APISession",
        Mock(side_effect=AssertionError("Live SDK")),
    )
    endpoints = [
        (mist_connection.mistapi.api.v1.self.self, "getSelf"),
        (mist_connection.mistapi.api.v1.orgs.stats, "listOrgDevicesStats"),
        (mist_connection.mistapi.api.v1.orgs.stats, "searchOrgSwOrGwPorts"),
        (mist_connection.mistapi.api.v1.sites.devices, "searchSiteDevices"),
        (
            mist_connection.mistapi.api.v1.orgs.gatewaytemplates,
            "listOrgGatewayTemplates",
        ),
        (mist_connection.mistapi.api.v1.orgs.deviceprofiles, "listOrgDeviceProfiles"),
    ]
    for module, name in endpoints:
        monkeypatch.setattr(module, name, Mock(side_effect=AssertionError("Live SDK")))
    for name in (
        "_sites_cache",
        "_all_gateway_templates",
        "_all_device_profiles",
        "_all_device_configs",
        "_all_runtime_stats",
    ):
        monkeypatch.setattr(MistConnection, name, None)
    monkeypatch.setattr(MistConnection, "_bulk_cache_time", 0)


@pytest.fixture
def connection():
    conn = MistConnection.__new__(MistConnection)
    conn.apisession = SimpleNamespace(
        _apitoken=["fake-first", "fake-active"], _apitoken_index=1
    )
    conn.org_id = "org"
    conn.host = "example.invalid"
    conn.api_token = "fake-first,fake-active"
    conn._token_count = 2
    conn._redis_cache = Mock()
    return conn


@pytest.mark.parametrize(
    "failure", [429, 403, SystemExit(), RuntimeError("SDK failure")]
)
def test_initialization_falls_back_and_restores_environment(monkeypatch, failure):
    monkeypatch.setenv("MIST_APITOKEN", "saved-placeholder")
    sessions = []

    def make_session(**kwargs):
        assert "MIST_APITOKEN" not in mist_connection.os.environ
        session = SimpleNamespace(_session=SimpleNamespace(headers={}))
        sessions.append((kwargs["apitoken"], session))
        return session

    get_self = Mock(
        side_effect=[
            response(failure) if isinstance(failure, int) else failure,
            response(data={"privileges": [{"org_id": "org"}]}),
        ]
    )
    monkeypatch.setattr(mist_connection.mistapi, "APISession", make_session)
    monkeypatch.setattr(mist_connection.mistapi.api.v1.self.self, "getSelf", get_self)
    cache = Mock()
    conn = MistConnection(" fake-first ,fake-active ", redis_cache=cache)
    assert conn.org_id == "org"
    assert conn._token_list == ["fake-first", "fake-active"]
    assert conn.apisession._apitoken_index == 1
    assert conn.apisession._session.headers == {"Authorization": "Token fake-active"}
    assert [token for token, _ in sessions] == ["fake-first", "fake-active"]
    assert get_self.call_count == 2
    assert mist_connection.os.environ["MIST_APITOKEN"] == "saved-placeholder"
    if failure == 429 or isinstance(failure, SystemExit):
        cache.set_rate_limit_status.assert_called_once_with(
            is_limited=True, tokens_exhausted=1, total_tokens=2
        )


def test_initialization_all_tokens_fail_even_if_cache_reporting_fails(monkeypatch):
    monkeypatch.delenv("MIST_APITOKEN", raising=False)
    monkeypatch.setattr(
        mist_connection.mistapi, "APISession", Mock(side_effect=SystemExit)
    )
    cache = Mock()
    cache.set_rate_limit_status.side_effect = RuntimeError("cache unavailable")
    with pytest.raises(RuntimeError, match="Token 2 caused SDK exit"):
        MistConnection("fake-first,fake-second", org_id="org", redis_cache=cache)
    assert cache.set_rate_limit_status.call_count == 3
    assert "MIST_APITOKEN" not in mist_connection.os.environ


@pytest.mark.parametrize("token", ["", " , "])
def test_initialization_rejects_empty_tokens(token):
    with pytest.raises(ValueError):
        MistConnection(token)


@pytest.fixture
def port_setup(connection, monkeypatch):
    template = {
        "port_config": {
            "wan0.30": {
                "usage": "wan",
                "name": "Internet",
                "description": " ISP ",
                "vlan_id": 30,
                "ip_config": {"type": "dhcp", "gateway": "192.0.2.1"},
            },
            "wan1": {
                "usage": "wan",
                "description": "Backup",
                "disabled": True,
                "ip_config": {
                    "type": "static",
                    "ip": " 198.51.100.2 ",
                    "netmask": " /28 ",
                },
            },
            "{{unresolved}}": {"usage": "wan"},
            "lan0": {"usage": "lan"},
        }
    }
    config = {"port_config": {"wan0.30": {"name": "Override name"}}}
    runtime = {
        "if_stat": {
            "a": {
                "port_usage": "wan",
                "port_id": "wan0",
                "ips": ["192.0.2.10/24"],
                "address_mode": "Static",
                "rx_bytes": 999,
                "extra": "preserved",
            },
            "b": {"port_usage": "lan", "ips": ["203.0.113.2/24"]},
        }
    }
    gateway = {"id": "gw", "site_id": "site", "mac": "mac", "name": "Gateway"}
    ports = [
        {
            "mac": "mac",
            "port_id": "wan0",
            "port_usage": "wan",
            "rx_bytes": 8,
            "up": True,
        },
        {"mac": "mac", "port_id": "other", "port_usage": "wan", "port_desc": "Backup"},
        {"mac": "mac", "port_id": "unknown", "port_usage": "wan", "port_desc": " raw "},
    ]
    monkeypatch.setattr(
        connection,
        "get_sites",
        Mock(return_value=[{"id": "site", "name": "Site", "gatewaytemplate_id": "t"}]),
    )
    monkeypatch.setattr(
        connection, "_get_site_by_id", Mock(return_value={"gatewaytemplate_id": "t"})
    )
    monkeypatch.setattr(connection, "_get_device_config", Mock(return_value=config))
    monkeypatch.setattr(
        connection, "_get_gateway_template", Mock(return_value=template)
    )
    monkeypatch.setattr(connection, "_get_device_profile", Mock(return_value=template))
    monkeypatch.setattr(connection, "_batch_fetch_inventory", Mock(return_value={}))
    monkeypatch.setattr(
        mist_connection.mistapi, "get_all", lambda _session, res: res.data
    )
    monkeypatch.setattr(
        mist_connection.mistapi.api.v1.orgs.stats,
        "listOrgDevicesStats",
        Mock(return_value=response(data=[gateway])),
    )
    monkeypatch.setattr(
        mist_connection.mistapi.api.v1.orgs.stats,
        "searchOrgSwOrGwPorts",
        Mock(
            return_value=response(
                data=ports
                + [{"mac": "foreign", "port_id": "ignored", "port_usage": "wan"}]
            )
        ),
    )
    monkeypatch.setattr(
        mist_connection.mistapi.api.v1.sites.devices,
        "searchSiteDevices",
        Mock(return_value=response(data={"results": [runtime]})),
    )
    return gateway, ports, template, config, runtime


def assert_runtime_enrichment(port: dict) -> None:
    assert port["wan_name"] == "Override name"
    assert port["ip"] == "192.0.2.10"
    assert port["netmask"] == "24"
    assert port["template_type"] == "dhcp"
    assert port["type"] == "static"
    assert port["override"] == "yes"
    assert port["vlan_id"] == "30"
    assert port["rx_bytes"] == 8
    assert port["extra"] == "preserved"


@pytest.mark.parametrize("optimized", [False, True])
@pytest.mark.parametrize("hub", [False, True])
def test_port_enrichment_template_profile_runtime_and_matching(
    connection, port_setup, optimized, hub
):
    gateway, ports, template, config, runtime = port_setup
    original = copy.deepcopy(template)
    inventory = {"mac": {"deviceprofile_id": "p"}} if hub else {}
    site_data = {"configs": {"gw": config}, "runtime": {"mac": runtime}}
    if optimized:
        enriched = connection.enrich_gateway_ports_optimized(
            gateway, ports, inventory, site_data
        )
    else:
        enriched = connection.enrich_gateway_ports(gateway, ports, inventory)
    by_name = {port["name"]: port for port in enriched}
    assert list(by_name) == ["other", "unknown", "wan0"]
    assert_runtime_enrichment(by_name["wan0"])
    assert by_name["other"]["ip"] == "198.51.100.2"
    assert by_name["other"]["netmask"] == "28"
    assert by_name["other"]["enabled"] is False
    assert by_name["unknown"]["type"] == "dhcp"
    assert template == original
    if not optimized:
        connection._redis_cache.set_raw_api_response.assert_called_once()


def test_gateway_stats_config_only_ports_and_site_filter(connection, port_setup):
    _, _, template, _, _ = port_setup
    original = copy.deepcopy(template)
    gateways = connection.get_gateway_stats()
    assert gateways[0]["site_name"] == "Site"
    assert gateways[0]["num_ports"] == 4
    assert [port["name"] for port in gateways[0]["ports"]] == [
        "other",
        "unknown",
        "wan0",
        "wan1",
    ]
    assert gateways[0]["ports"][-1]["override"] == "no"
    assert gateways[0]["ports"][-1]["up"] is False
    assert gateways[0]["ports"][-1]["netmask"] == "28"
    assert "port_id" not in gateways[0]["ports"][-1]
    assert gateways[0]["ports"][2]["rx_errors"] == 0
    assert template == original
    assert connection.get_gateway_stats(site_id="different") == []


@pytest.mark.parametrize("optimized", [False, True])
def test_enrichment_missing_identity_and_config_failure_use_minimal(
    connection, port_setup, optimized
):
    gateway, ports, _, _, _ = port_setup
    enrich = lambda gw: (
        connection.enrich_gateway_ports_optimized(
            gw, ports, {}, {"configs": {"gw": {"port_config": {"wan0": None}}}}
        )
        if optimized
        else connection.enrich_gateway_ports(gw, ports, {})
    )
    assert enrich({}) == connection._minimal_port_enrichment(ports)
    connection._get_device_config.side_effect = RuntimeError("bad config")
    assert enrich(gateway) == connection._minimal_port_enrichment(ports)


@pytest.mark.parametrize("status", [403, 429])
def test_gateway_stats_device_failure_is_not_swallowed(connection, monkeypatch, status):
    monkeypatch.setattr(connection, "get_sites", Mock(return_value=[]))
    monkeypatch.setattr(
        mist_connection.mistapi.api.v1.orgs.stats,
        "listOrgDevicesStats",
        Mock(return_value=response(status)),
    )
    with pytest.raises(RuntimeError, match=str(status)):
        connection.get_gateway_stats()


def test_gateway_stats_early_config_failure_is_propagated(connection, port_setup):
    connection._get_device_config.side_effect = RuntimeError("bad config")
    with pytest.raises(UnboundLocalError):
        connection.get_gateway_stats()


@pytest.mark.parametrize("invalid_config", [None, {"port_config": None}])
def test_gateway_stats_malformed_config_retains_failure_behavior(
    connection, port_setup, invalid_config
):
    connection._get_device_config.return_value = invalid_config
    if isinstance(invalid_config, dict):
        connection._get_gateway_template.return_value = invalid_config
    with pytest.raises(UnboundLocalError):
        connection.get_gateway_stats()


def test_gateway_stats_runtime_failure_retains_previously_parsed_runtime_ports(
    connection, port_setup
):
    _, _, _, _, runtime = port_setup
    runtime["if_stat"]["invalid"] = {
        "port_usage": "wan",
        "port_id": "wan1",
        "ips": ["invalid/cidr"],
    }
    ports = connection.get_gateway_stats()[0]["ports"]
    by_name = {port["name"]: port for port in ports}
    assert by_name["wan0"]["ip"] == "192.0.2.10"
    assert by_name["wan1"]["ip"] == "198.51.100.2"
    enriched = connection.enrich_gateway_ports_optimized(
        port_setup[0],
        port_setup[1],
        {},
        {"configs": {"gw": port_setup[3]}, "runtime": {"mac": runtime}},
    )
    assert enriched == connection._minimal_port_enrichment(port_setup[1])


@pytest.mark.parametrize("optimized", [False, True])
def test_enrichment_device_override_only_is_not_type_override(
    connection, port_setup, optimized
):
    gateway, ports, _, config, runtime = port_setup
    runtime["if_stat"]["a"]["address_mode"] = "Dynamic"
    ports[0]["port_id"] = "wan0.30"
    runtime["if_stat"]["a"]["port_id"] = "wan0.30"
    if optimized:
        enriched = connection.enrich_gateway_ports_optimized(
            gateway, ports, {}, {"configs": {"gw": config}, "runtime": {"mac": runtime}}
        )
    else:
        enriched = connection.enrich_gateway_ports(gateway, ports, {})
    assert enriched[-1]["override"] == "no"
    stats = connection.get_gateway_stats()[0]["ports"]
    assert (
        next(port for port in stats if port["name"] == "wan0.30")["override"] == "yes"
    )


def test_gateway_stats_hub_profile_wins_over_site_template(connection, port_setup):
    connection._batch_fetch_inventory.return_value = {"mac": {"deviceprofile_id": "p"}}
    connection._get_device_profile.return_value = {
        "port_config": {"wan0": {"usage": "wan", "name": "Hub uplink"}}
    }
    ports = connection.get_gateway_stats()[0]["ports"]
    assert ports[-1]["wan_name"] == "Hub uplink"
    connection._get_device_profile.assert_called_once_with("p")
    connection._get_gateway_template.assert_not_called()


def test_gateway_stats_port_api_failure_still_adds_configured_ports(
    connection, port_setup, monkeypatch
):
    monkeypatch.setattr(
        mist_connection.mistapi.api.v1.orgs.stats,
        "searchOrgSwOrGwPorts",
        Mock(return_value=response(429)),
    )
    ports = connection.get_gateway_stats()[0]["ports"]
    assert [port["name"] for port in ports] == ["wan0", "wan1"]
    assert ports[0]["up"] is False
    assert ports[0]["rx_bytes"] == 0
    assert ports[0]["ip"] == "192.0.2.10"
    assert ports[0]["override"] == "yes"


@pytest.mark.parametrize("status", [200, 429, 403])
def test_insights_active_token_conversion_and_failures(connection, monkeypatch, status):
    request = Mock(
        return_value=SimpleNamespace(
            status_code=status,
            json=lambda: {
                "rx_bps": [None, 8, 0],
                "tx_bps": [16, None, 8],
                "rt": ["2025-01-01T00:00:00Z", "invalid", 42],
                "interval": 60,
            },
        )
    )
    monkeypatch.setattr(requests, "get", request)
    result = connection._get_port_insights("site", "gw", "wan0", 1, 2)
    assert request.call_args.kwargs["headers"]["Authorization"] == "Token fake-active"
    assert request.call_args.kwargs["timeout"] == 30
    if status == 200:
        assert result == {
            "timestamps": [1735689600, 0, 42],
            "rx_bps": [0, 8, 0],
            "tx_bps": [16, 0, 8],
            "rx_bytes": 60,
            "tx_bytes": 180,
            "interval": 60,
        }
    else:
        assert result is None
    if status == 429:
        connection._redis_cache.set_rate_limit_status.assert_called_once_with(
            is_limited=True, tokens_exhausted=2, total_tokens=2
        )


def test_insights_http_exception_returns_none_and_invalid_token_index_falls_back(
    connection, monkeypatch
):
    connection.apisession._apitoken_index = -1
    request = Mock(side_effect=RuntimeError("offline timeout"))
    monkeypatch.setattr(requests, "get", request)
    assert connection._get_port_insights("site", "gw", "wan0", 1, 2) is None
    assert request.call_args.kwargs["headers"]["Authorization"] == "Token fake-first"


@pytest.mark.parametrize("status", [200, 403])
def test_bulk_prefetch_persists_cache_and_skips_recent_fetch(
    connection, monkeypatch, status
):
    templates = Mock(return_value=response(status, [{"id": "t"}, {}]))
    profiles = Mock(return_value=response(status, [{"id": "p"}, {}]))
    monkeypatch.setattr(
        mist_connection.mistapi.api.v1.orgs.gatewaytemplates,
        "listOrgGatewayTemplates",
        templates,
    )
    monkeypatch.setattr(
        mist_connection.mistapi.api.v1.orgs.deviceprofiles,
        "listOrgDeviceProfiles",
        profiles,
    )
    monkeypatch.setattr(
        mist_connection.mistapi, "get_all", lambda _session, res: res.data
    )
    connection.prefetch_all_templates_and_profiles()
    connection.prefetch_all_templates_and_profiles()
    assert templates.call_count == profiles.call_count == 1
    assert MistConnection._all_gateway_templates == (
        {"t": {"id": "t"}} if status == 200 else {}
    )
    assert MistConnection._all_device_profiles == (
        {"p": {"id": "p"}} if status == 200 else {}
    )
    if status == 200:
        connection._redis_cache.set_gateway_template.assert_called_once_with(
            "t", {"id": "t"}, ttl=connection.TEMPLATE_CACHE_TTL
        )
        connection._redis_cache.set_device_profile.assert_called_once_with(
            "p", {"id": "p"}, ttl=connection.TEMPLATE_CACHE_TTL
        )


def test_bulk_prefetch_independent_errors_and_expired_refresh(connection, monkeypatch):
    templates = Mock(side_effect=RuntimeError("templates unavailable"))
    profiles = Mock(side_effect=RuntimeError("profiles unavailable"))
    monkeypatch.setattr(
        mist_connection.mistapi.api.v1.orgs.gatewaytemplates,
        "listOrgGatewayTemplates",
        templates,
    )
    monkeypatch.setattr(
        mist_connection.mistapi.api.v1.orgs.deviceprofiles,
        "listOrgDeviceProfiles",
        profiles,
    )
    connection.prefetch_all_templates_and_profiles()
    monkeypatch.setattr(MistConnection, "_bulk_cache_time", 0)
    connection.prefetch_all_templates_and_profiles()
    assert templates.call_count == profiles.call_count == 2
    assert (
        MistConnection._all_gateway_templates
        == MistConnection._all_device_profiles
        == {}
    )
