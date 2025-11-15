import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.append(str(Path(__file__).resolve().parents[3]))

if "httpx_impersonate" not in sys.modules:
    import httpx

    stub = types.ModuleType("httpx_impersonate")

    class _StubClient(httpx.Client):  # pragma: no cover - pomocnicze dla testów
        def __init__(self, *args, impersonate: str | None = None, **kwargs):
            super().__init__(*args, **kwargs)

    stub.Client = _StubClient
    sys.modules["httpx_impersonate"] = stub

import backend.app.services.allegro_scraper as scraper


class DummyResponse:
    def __init__(self, text: str, status_code: int = 200):
        self.text = text
        self.status_code = status_code


class DummyClient:
    def __init__(self, response: DummyResponse):
        self._response = response
        self.called_with = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url, headers):  # noqa: D401 - implementacja pomocnicza dla testów
        self.called_with = (url, headers)
        return self._response


class AllegroScraperHttpxTests(unittest.TestCase):
    def _patch_client(self, response: DummyResponse):
        return patch.object(scraper, "Client", return_value=DummyClient(response))

    def test_successful_parse_returns_price_and_sold(self):
        html = '{"price":{"amount":"123,45"}}<span>15 osób kupiło</span>'
        response = DummyResponse(html)

        with self._patch_client(response):
            result = scraper.fetch_allegro_data("5901234123457")

        self.assertEqual(result["source"], "allegro_httpx")
        self.assertEqual(result["lowest_price"], 123.45)
        self.assertEqual(result["sold_count"], 15)
        self.assertFalse(result["not_found"])
        self.assertIsNone(result["error"])

    def test_ban_detection_reports_alert(self):
        html = "Twoje żądanie zostało zablokowane"
        response = DummyResponse(html, status_code=403)

        with self._patch_client(response), patch.object(
            scraper, "send_scraper_alert"
        ) as mock_alert:
            result = scraper.fetch_allegro_data("1234567890123")

        self.assertEqual(result["source"], "ban_detected")
        self.assertTrue(result["alert_sent"])
        self.assertIsNotNone(result["error"])
        mock_alert.assert_called_once()

    def test_captcha_detection(self):
        html = "<html>captcha challenge</html>"
        response = DummyResponse(html)

        with self._patch_client(response), patch.object(
            scraper, "send_scraper_alert"
        ) as mock_alert:
            result = scraper.fetch_allegro_data("1234567890123")

        self.assertEqual(result["source"], "captcha_detected")
        self.assertTrue(result["alert_sent"])
        mock_alert.assert_called_once()

    def test_no_results_detection(self):
        html = "<div>Brak wyników dla tego zapytania</div>"
        response = DummyResponse(html)

        with self._patch_client(response):
            result = scraper.fetch_allegro_data("3213213213213")

        self.assertTrue(result["not_found"])
        self.assertEqual(result["source"], "allegro_httpx")

    def test_missing_data_returns_failure(self):
        html = "<html><body>No price here</body></html>"
        response = DummyResponse(html)

        with self._patch_client(response):
            result = scraper.fetch_allegro_data("1111111111111")

        self.assertEqual(result["source"], "failed")
        self.assertIsNotNone(result["error"])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
