"""Capture actual dashboard interactions against an offline sample cache."""

from pathlib import Path
from threading import Thread

from playwright.sync_api import sync_playwright
from werkzeug.serving import make_server

from docs.sample_cache import create_preview


def capture() -> None:
    output = Path(__file__).parent / "screenshots"
    output.mkdir(exist_ok=True)
    server = make_server("127.0.0.1", 0, create_preview())
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            page = browser.new_page(viewport={"width": 1366, "height": 900})
            errors: list[str] = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(f"http://127.0.0.1:{server.server_port}")
            page.locator("#gatewayContainer").wait_for(state="visible")
            page.locator("#peer-paths-sample-gw-1-wan0 .clickable-peers").wait_for(
                state="attached"
            )
            page.locator("#gateway-row-0").click()
            page.locator("#details-row-0").wait_for(state="visible")
            page.locator(".toast").wait_for(state="hidden")
            assert page.locator("#totalGateways").inner_text() == "2"
            page.screenshot(path=str(output / "dashboard.png"))
            page.locator("#searchInput").fill("Seattle")
            assert page.locator(".gateway-row").count() == 1
            assert "Sample Branch 1" in page.locator(".gateway-row").inner_text()
            page.screenshot(path=str(output / "search.png"))
            page.locator("#searchInput").fill("")
            page.locator("#gateway-row-0").click()
            page.locator("#rx-sample-gw-1-wan0").click()
            page.wait_for_function(
                "typeof trafficChartRateInstance !== 'undefined' && "
                "trafficChartRateInstance !== null"
            )
            page.locator("#chartModal").screenshot(path=str(output / "traffic.png"))
            page.get_by_role("button", name="1 Hour", exact=True).click()
            page.wait_for_function("cachedTrafficData.timestamps.length === 60")
            page.locator("#chartModal .chart-modal-close").click()
            page.locator("#peer-paths-sample-gw-1-wan0 .clickable-peers").click()
            page.locator("#peerPathsModal").wait_for(state="visible")
            assert "Sample Hub" in page.locator("#peerPathsModalBody").inner_text()
            page.locator("#peerPathsModal .modal-content").screenshot(
                path=str(output / "vpn-peers.png")
            )
            assert not errors, errors
            browser.close()
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


if __name__ == "__main__":
    capture()
