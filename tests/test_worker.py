"""Offline characterization of refresh, insight merging, and gap analysis."""

import copy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import requests

import worker
from worker import DataWorker


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setattr(requests, "get", Mock(side_effect=AssertionError("Live HTTP")))
    monkeypatch.setattr(worker.time, "time", lambda: 1_000_000)


@pytest.fixture
def data_worker():
    instance = DataWorker.__new__(DataWorker)
    instance.cache = Mock()
    instance.mist = Mock()
    instance.cache_ttl = 1234
    instance.parallel_workers = 2
    return instance


def test_incremental_port_fetch_filters_wan_and_caches_each_successful_site(
    data_worker,
):
    sites = [{"id": "bad", "name": "Bad"}, {}, {"id": "good", "name": "Good"}]
    data_worker.cache.get_sites.return_value = sites
    data_worker.mist.prefetch_site_device_data.side_effect = [
        RuntimeError("site failed"),
        {"configs": {}, "runtime": {}},
    ]
    data_worker.mist._batch_fetch_inventory.return_value = {}
    data_worker.mist.get_site_port_stats.return_value = [
        {"mac": "mac", "port_usage": "wan", "port_id": "wan0"},
        {"mac": "mac", "port_usage": "lan", "port_id": "lan0"},
        {"port_usage": "wan"},
        {"mac": "foreign", "port_usage": "wan"},
    ]
    data_worker.mist.enrich_gateway_ports_optimized.return_value = [{"name": "wan0"}]
    gateways = [{"id": "gw", "mac": "mac", "_basic_only": True}]
    data_worker._fetch_port_stats_incremental(gateways)
    assert gateways == [
        {
            "id": "gw",
            "mac": "mac",
            "_basic_only": False,
            "ports": [{"name": "wan0"}],
            "num_ports": 1,
        }
    ]
    data_worker.cache.set_gateways.assert_called_once_with(gateways, ttl=1234)
    assert (
        data_worker.cache.set_loading_phase.call_args.args[2]["devices_with_ports"] == 1
    )
    raw = data_worker.mist.enrich_gateway_ports_optimized.call_args.args[1]
    assert raw == [{"mac": "mac", "port_usage": "wan", "port_id": "wan0"}]


def test_incremental_fallback_keeps_unknown_gateways_and_does_not_add_num_ports(
    data_worker,
):
    data_worker.cache.get_sites.return_value = None
    data_worker.mist.get_port_stats_paginated.return_value = {
        "mac": [{"name": "wan0"}],
        "foreign": [],
    }
    gateways = [{"mac": "mac", "_basic_only": True}, {"mac": "other"}]
    data_worker._fetch_port_stats_incremental(gateways)
    assert gateways == [
        {"mac": "mac", "_basic_only": False, "ports": [{"name": "wan0"}]},
        {"mac": "other"},
    ]
    data_worker.cache.set_gateways.assert_called_once_with(gateways, ttl=1234)
    data_worker.mist._batch_fetch_inventory.assert_not_called()


def test_incremental_empty_ports_and_outer_failure_do_not_cache(data_worker):
    data_worker.cache.get_sites.return_value = [{"id": "site"}]
    data_worker.mist._batch_fetch_inventory.return_value = {}
    data_worker.mist.get_site_port_stats.return_value = []
    data_worker._fetch_port_stats_incremental([])
    data_worker.cache.set_gateways.assert_not_called()
    data_worker.cache.get_sites.side_effect = RuntimeError("cache unavailable")
    data_worker._fetch_port_stats_incremental([])
    data_worker.cache.set_gateways.assert_not_called()


def test_incremental_partial_enrichment_failure_retains_progress_counts(data_worker):
    data_worker.cache.get_sites.return_value = [{"id": "site"}]
    data_worker.mist._batch_fetch_inventory.return_value = {}
    data_worker.mist.get_site_port_stats.return_value = [
        {"mac": "first", "port_usage": "wan"},
        {"mac": "second", "port_usage": "wan"},
    ]
    data_worker.mist.enrich_gateway_ports_optimized.side_effect = [
        [{"name": "wan0"}],
        RuntimeError("second gateway failed"),
    ]
    gateways = [{"mac": "first"}, {"mac": "second"}]
    data_worker._fetch_port_stats_incremental(gateways)
    assert gateways[0]["num_ports"] == 1
    assert "ports" not in gateways[1]
    data_worker.cache.set_gateways.assert_not_called()
    progress = data_worker.cache.set_loading_phase.call_args.args[2]
    assert progress["ports_loaded"] == 1
    assert progress["devices_with_ports"] == 1


def test_sequential_insight_progress_includes_failed_requests(data_worker, monkeypatch):
    data_worker.mist.api_token = "fake-first"
    data_worker.mist.apisession = SimpleNamespace()
    data_worker.mist.host = "example.invalid"
    monkeypatch.setattr(
        requests, "get", Mock(return_value=SimpleNamespace(status_code=429))
    )
    ports = [{"name": f"wan{i}"} for i in range(100)]
    data_worker._fetch_insights([{"id": "gw", "site_id": "site", "ports": ports}])
    progress = data_worker.cache.set_loading_phase.call_args.args[2]
    assert progress["progress"] == progress["total"] == 100
    data_worker.cache.set_all_insights.assert_called_once_with({"gw": {}}, ttl=1234)


def test_parallel_insight_progress_counts_ports_not_resolutions(data_worker):
    data_worker.mist._get_port_insights.return_value = None
    ports = [{"port_id": f"wan{i}", "usage": "wan"} for i in range(50)]
    data_worker._fetch_insights_parallel(
        [{"id": "gw", "site_id": "site", "ports": ports}]
    )
    assert data_worker.mist._get_port_insights.call_count == 200
    progress = data_worker.cache.set_loading_phase.call_args.args[2]
    assert progress["progress"] == progress["total"] == 50


@pytest.mark.parametrize(
    "timestamps,priority",
    [
        (None, 1),
        ([], 1),
        ([999_000, 999_500], 3),
        ([390_000, 990_000], 2),
        ([390_000, 999_900], 4),
    ],
)
def test_gap_analysis_fetch_windows(data_worker, timestamps, priority):
    data_worker.cache.get_insights.return_value = {"timestamps": timestamps}
    result = data_worker._analyze_port_data_needs("gw", "wan0")
    assert result["priority"] == priority
    if priority == 1:
        assert result["has_data"] is False
        assert (result["fetch_start"], result["fetch_end"]) == (395_200, 1_000_000)
    elif priority == 2:
        assert (result["fetch_start"], result["fetch_end"]) == (990_000, 1_000_000)
    elif priority == 3:
        assert (result["fetch_start"], result["fetch_end"]) == (395_200, 999_000)
    else:
        assert (result["fetch_start"], result["fetch_end"]) == (913_600, 1_000_000)


def test_gap_analysis_retains_all_zero_timestamp_error(data_worker):
    data_worker.cache.get_insights.return_value = {"timestamps": [0, None]}
    with pytest.raises(ValueError):
        data_worker._analyze_port_data_needs("gw", "wan0")


def test_merge_insights_new_duplicates_win_padding_sorting_and_byte_totals(data_worker):
    existing = {
        "timestamps": [30, 10, 0, 20],
        "rx_bps": [8, 16],
        "tx_bps": [None, 8],
        "interval": 60,
    }
    new = {
        "timestamps": [20, 40, 20, None],
        "rx_bps": [1, 8, 24],
        "tx_bps": [8],
        "interval": 120,
    }
    snapshot = copy.deepcopy((existing, new))
    assert data_worker._merge_insights(existing, new) == {
        "timestamps": [10, 20, 30, 40],
        "rx_bps": [16, 24, 8, 8],
        "tx_bps": [8, 0, None, 0],
        "interval": 120,
        "rx_bytes": 840,
        "tx_bytes": 120,
    }
    assert (existing, new) == snapshot


def test_merge_insights_missing_timestamps_returns_original_objects(data_worker):
    data = {"timestamps": [10], "rx_bps": [8]}
    assert data_worker._merge_insights({}, data) is data
    assert data_worker._merge_insights(data, {}) is data


def test_sequential_insights_keeps_nulls_skips_bad_ids_and_continues_failures(
    data_worker, monkeypatch
):
    data_worker.mist.api_token = "fake-first,fake-second"
    data_worker.mist.apisession = SimpleNamespace(
        _apitoken=["fake-first", "fake-active"], _apitoken_index=1
    )
    data_worker.mist.host = "example.invalid"
    request = Mock(
        side_effect=[
            SimpleNamespace(
                status_code=200,
                json=lambda: {
                    "rx_bps": [8, None],
                    "tx_bps": [0, 16],
                    "timestamps": [1, 2],
                },
            ),
            RuntimeError("offline timeout"),
            SimpleNamespace(status_code=429),
        ]
    )
    monkeypatch.setattr(requests, "get", request)
    gateways = [
        {"id": "invalid"},
        {
            "id": "gw",
            "site_id": "site",
            "ports": [{}, {"name": "wan0"}, {"port_id": "wan1"}, {"name": "wan2"}],
        },
    ]
    data_worker._fetch_insights(gateways)
    data_worker.cache.set_all_insights.assert_called_once_with(
        {
            "gw": {
                "wan0": {
                    "rx_bytes": 3600,
                    "tx_bytes": 7200,
                    "rx_bps": [8, None],
                    "tx_bps": [0, 16],
                    "timestamps": [1, 2],
                }
            }
        },
        ttl=1234,
    )
    assert request.call_count == 3
    assert request.call_args.kwargs["headers"]["Authorization"] == "Token fake-active"
    assert request.call_args.kwargs["params"]["start"] == 395_200


def test_parallel_insights_resolution_failure_filtering_and_default_cache(data_worker):
    def insights(_site, _gw, _port, start, end, interval):
        assert end == 1_000_000
        assert start < end
        if interval == 120:
            raise RuntimeError("one resolution fails")
        if interval == 600:
            return {"timestamps": [1], "rx_bps": [0], "tx_bps": [None]}
        return {
            "timestamps": [0, 1, 2, 3],
            "rx_bps": [8, None, 8],
            "tx_bps": [8, 16],
            "interval": interval,
        }

    data_worker.mist._get_port_insights.side_effect = insights
    gateways = [
        {
            "id": "gw",
            "site_id": "site",
            "ports": [
                {"port_id": "wan0", "usage": "wan"},
                {"name": "missing-port-id", "usage": "wan"},
                {"port_id": "lan0", "usage": "lan"},
            ],
        },
        {"id": "missing-site", "ports": [{"port_id": "wan", "usage": "wan"}]},
    ]
    data_worker._fetch_insights_parallel(gateways)
    assert data_worker.mist._get_port_insights.call_count == 4
    calls = data_worker.cache.set_insights_by_resolution.call_args_list
    assert [call.args[2] for call in calls] == ["1h", "7d"]
    assert calls[-1].args[3] == {
        "timestamps": [1, 2],
        "rx_bps": [0, 8],
        "tx_bps": [16, 0],
        "rx_bytes": 3600,
        "tx_bytes": 7200,
        "interval": 3600,
    }
    data_worker.cache.set_insights.assert_called_once_with(
        "gw", "wan0", calls[-1].args[3], ttl=1234
    )


def test_parallel_insights_empty_data_never_replaces_cache(data_worker):
    data_worker.mist._get_port_insights.return_value = None
    data_worker._fetch_insights_parallel(
        [
            {
                "id": "gw",
                "site_id": "site",
                "ports": [{"port_id": "wan0", "port_usage": "wan"}],
            }
        ]
    )
    data_worker.cache.set_insights.assert_not_called()
    data_worker.cache.set_insights_by_resolution.assert_not_called()
