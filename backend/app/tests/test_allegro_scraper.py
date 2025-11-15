import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.append(str(Path(__file__).resolve().parents[3]))

if "httpx_impersonate" not in sys.modules:
    stub = types.ModuleType("httpx_impersonate")

    class _StubClient:  # pragma: no cover - pomocnicze dla testów
        def __init__(self, *args, impersonate: str | None = None, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def close(self):
            return None

    stub.Client = _StubClient
    sys.modules["httpx_impersonate"] = stub

import backend.app.services.allegro_scraper as scraper


class DummyResponse:
    def __init__(self, text: str, status_code: int = 200):
        self.text = text
        self.status_code = status_code


class DummyClient:
    def __init__(self, responses):
        if isinstance(responses, list):
            self._responses = list(responses)
        else:
            self._responses = [responses]
        self.called_with = []
        self.call_count = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url, headers):  # noqa: D401 - implementacja pomocnicza dla testów
        self.call_count += 1
        self.called_with.append((url, headers))
        index = min(self.call_count - 1, len(self._responses) - 1)
        response = self._responses[index]
        if isinstance(response, Exception):
            raise response
        return response


class AllegroScraperHttpxTests(unittest.TestCase):
    def _patch_client(self, responses):
        dummy = DummyClient(responses)
        return patch.object(scraper, "Client", return_value=dummy), dummy

    def test_successful_parse_returns_price_and_sold(self):
        html = '{"price":{"amount":"123,45"}}<span>15 osób kupiło</span>'
        response = DummyResponse(html)

        patcher, _ = self._patch_client(response)
        with patcher:
            result = scraper.fetch_allegro_data("5901234123457", sleep_func=lambda _: None)

        self.assertEqual(result["source"], "allegro_httpx")
        self.assertEqual(result["lowest_price"], 123.45)
        self.assertEqual(result["sold_count"], 15)
        self.assertFalse(result["not_found"])
        self.assertIsNone(result["error"])

    def test_ban_detection_reports_alert(self):
        html = "Twoje żądanie zostało zablokowane"
        response = DummyResponse(html, status_code=403)

        patcher, _ = self._patch_client(response)

        with patcher, patch.object(
            scraper, "send_scraper_alert"
        ) as mock_alert:
            result = scraper.fetch_allegro_data("1234567890123", sleep_func=lambda _: None)

        self.assertEqual(result["source"], "ban_detected")
        self.assertTrue(result["alert_sent"])
        self.assertIsNotNone(result["error"])
        mock_alert.assert_called_once()

    def test_captcha_detection(self):
        html = "<html>captcha challenge</html>"
        response = DummyResponse(html)

        patcher, _ = self._patch_client(response)

        with patcher, patch.object(
            scraper, "send_scraper_alert"
        ) as mock_alert:
            result = scraper.fetch_allegro_data("1234567890123", sleep_func=lambda _: None)

        self.assertEqual(result["source"], "captcha_detected")
        self.assertTrue(result["alert_sent"])
        mock_alert.assert_called_once()

    def test_no_results_detection(self):
        html = "<div>Brak wyników dla tego zapytania</div>"
        response = DummyResponse(html)

        patcher, _ = self._patch_client(response)
        with patcher:
            result = scraper.fetch_allegro_data("3213213213213", sleep_func=lambda _: None)

        self.assertTrue(result["not_found"])
        self.assertEqual(result["source"], "allegro_httpx")

    def test_missing_data_returns_failure(self):
        html = "<html><body>No price here</body></html>"
        response = DummyResponse(html)

        patcher, _ = self._patch_client(response)
        with patcher:
            result = scraper.fetch_allegro_data("1111111111111", sleep_func=lambda _: None)

        self.assertEqual(result["source"], "failed")
        self.assertIsNotNone(result["error"])

    def test_retry_on_429_uses_multiple_attempts(self):
        first = DummyResponse("Twoje żądanie zostało zablokowane", status_code=429)
        second = DummyResponse('{"price":{"amount":"199"}}')

        patcher, dummy_client = self._patch_client([first, second])
        with patcher:
            result = scraper.fetch_allegro_data(
                "5901234123457",
                max_attempts=2,
                sleep_func=lambda _: None,
            )

        self.assertEqual(result["lowest_price"], 199.0)
        self.assertGreaterEqual(dummy_client.call_count, 2)
        self.assertEqual(result["source"], "allegro_httpx")
        self.assertIsNone(result.get("error"))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
