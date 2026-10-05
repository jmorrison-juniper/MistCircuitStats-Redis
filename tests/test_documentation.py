"""Keep the landing README and offline screenshot fixture honest."""

import re
from pathlib import Path

import pytest

import app as web
from docs.sample_cache import SampleCache

ROOT = Path(__file__).resolve().parents[1]


def test_readme_has_only_six_landing_sections_and_real_images() -> None:
    text = (ROOT / "README.md").read_text()
    assert re.findall(r"^## (.+)$", text, re.MULTILINE) == [
        "What",
        "How",
        "Where",
        "When",
        "Why",
        "Who",
    ]
    assert not re.findall(r"^#{3,} ", text, re.MULTILINE)
    images = re.findall(r"!\[[^\]]*\]\(([^)]+)\)", text)
    assert len(images) == 4
    for image in images:
        path = ROOT / image
        assert path.read_bytes().startswith(b"\x89PNG\r\n\x1a\n")
        assert path.stat().st_size > 10000
    for link in re.findall(r"\[[^\]]*\]\(([^)]+)\)", text):
        if not link.startswith("https://"):
            assert (ROOT / link).exists(), link


def test_documentation_preview_serves_real_routes_without_services(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden_connection() -> None:
        pytest.fail("Documentation preview must not initialize Redis")

    monkeypatch.setattr(web, "RedisCache", forbidden_connection)
    monkeypatch.setattr(web, "cache", SampleCache())
    client = web.app.test_client()
    assert b"Mist Circuit Stats (Redis)" in client.get("/").data
    for endpoint in [
        "/api/status",
        "/api/organization",
        "/api/sites",
        "/api/templates",
        "/api/gateways",
        "/api/cache-stats",
        "/api/token-status",
        "/api/insights/all",
        "/api/vpn-peers/all",
    ]:
        response = client.get(endpoint)
        assert response.status_code == 200
        assert response.json["success"] is True
    assert client.get("/api/gateways?site_id=sample-site-1").json["data"][0][
        "name"
    ] == ("Sample Branch 1")
    for duration, count in [("1h", 60), ("6h", 180), ("1d", 144), ("7d", 168)]:
        data = client.get(
            f"/api/gateway/sample-gw-1/port/wan0/traffic?duration={duration}"
        ).json["data"]
        assert (
            len(data["timestamps"])
            == len(data["rx_bps"])
            == len(data["tx_bps"])
            == count
        )
        assert data["timestamps"] == sorted(data["timestamps"])
        assert data["resolution"] == duration
    assert (
        client.get("/api/gateway/unknown/port/wan0/traffic").json["data"]["timestamps"]
        == []
    )
