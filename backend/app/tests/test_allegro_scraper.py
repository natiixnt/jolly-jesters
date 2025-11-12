import unittest
from types import SimpleNamespace
from unittest.mock import patch

import socket

from requests.exceptions import ConnectionError as RequestsConnectionError

import backend.app.services.allegro_scraper as scraper


class DummyDriver:
    def __init__(self) -> None:
        self.page_source = "<html></html>"
        self.last_url = None

    def get(self, url: str) -> None:
        self.last_url = url

    def find_elements(self, *args, **kwargs):  # noqa: D401, ANN002, ANN003
        return [object()]

    def quit(self) -> None:
        pass


class EnsureSeleniumReadyTests(unittest.TestCase):
    def setUp(self) -> None:
        scraper._ACTIVE_SELENIUM_URL = None

    def tearDown(self) -> None:
        scraper._ACTIVE_SELENIUM_URL = None

    def test_ensure_selenium_ready_falls_back_to_next_candidate(self):
        candidates = ["http://unreachable", "http://localhost:4444/wd/hub"]

        with patch("backend.app.services.allegro_scraper.SELENIUM_URL_CANDIDATES", candidates):
            with patch(
                "backend.app.services.allegro_scraper._wait_for_candidate_ready",
                side_effect=[scraper.SeleniumEndpointUnavailable("boom"), None],
            ) as mock_wait:
                selected = scraper._ensure_selenium_ready(timeout=1)

        self.assertEqual(selected, candidates[1])
        self.assertEqual(scraper._ACTIVE_SELENIUM_URL, candidates[1])
        self.assertEqual(mock_wait.call_count, 2)

    def test_ensure_selenium_ready_raises_when_all_candidates_fail(self):
        candidates = ["http://bad-grid"]

        with patch("backend.app.services.allegro_scraper.SELENIUM_URL_CANDIDATES", candidates):
            with patch(
                "backend.app.services.allegro_scraper._wait_for_candidate_ready",
                side_effect=scraper.SeleniumEndpointUnavailable("nope"),
            ):
                with self.assertRaises(scraper.SeleniumEndpointUnavailable) as ctx:
                    scraper._ensure_selenium_ready(timeout=1)

        message = str(ctx.exception)
        self.assertIn("http://bad-grid", message)
        self.assertIn("nope", message)
        self.assertIsNone(scraper._ACTIVE_SELENIUM_URL)


class WaitForCandidateReadyTests(unittest.TestCase):
    def test_dns_error_is_not_retried(self):
        err = RequestsConnectionError("dns failure")
        err.__cause__ = socket.gaierror("Name or service not known")

        with patch("backend.app.services.allegro_scraper.requests.get", side_effect=err) as mock_get:
            with self.assertRaises(scraper.SeleniumEndpointUnavailable) as ctx:
                scraper._wait_for_candidate_ready("http://selenium:4444/wd/hub", timeout=5)

        self.assertIn("Nie można rozwiązać nazwy hosta", str(ctx.exception))
        self.assertEqual(mock_get.call_count, 1)

    def test_dns_error_detected_in_reason_chain(self):
        err = RequestsConnectionError("dns failure")
        nested = RequestsConnectionError("nested")
        nested.reason = socket.gaierror("Name or service not known")
        err.__cause__ = nested

        with patch("backend.app.services.allegro_scraper.requests.get", side_effect=err) as mock_get:
            with self.assertRaises(scraper.SeleniumEndpointUnavailable):
                scraper._wait_for_candidate_ready("http://selenium:4444/wd/hub", timeout=5)

        self.assertEqual(mock_get.call_count, 1)


class FetchDataRetryTests(unittest.TestCase):
    def tearDown(self) -> None:
        scraper._ACTIVE_SELENIUM_URL = None

    def test_infrastructure_failure_breaks_retry_loop(self):
        with patch(
            "backend.app.services.allegro_scraper.get_driver",
            side_effect=scraper.SeleniumEndpointUnavailable("dns"),
        ) as mock_driver:
            result = scraper.fetch_allegro_data("1234567890123")

        self.assertEqual(mock_driver.call_count, 1)
        self.assertEqual(result["source"], "failed")
        self.assertIn("dns", result.get("error", ""))


class FetchDataDiagnosticsTests(unittest.TestCase):
    def setUp(self) -> None:
        scraper._ACTIVE_SELENIUM_URL = None

    def tearDown(self) -> None:
        scraper._ACTIVE_SELENIUM_URL = None

    def _dummy_wait(self, driver):
        return SimpleNamespace(until=lambda condition: condition(driver))

    def test_success_response_includes_diagnostics(self):
        diag_payload = {"title": "diag"}
        driver = DummyDriver()

        with patch.object(scraper, "MAX_ATTEMPTS", 1), patch(
            "backend.app.services.allegro_scraper.get_driver", return_value=driver
        ), patch(
            "backend.app.services.allegro_scraper._run_proxy_diagnostics",
            return_value=diag_payload,
        ), patch("backend.app.services.allegro_scraper._accept_cookies"), patch(
            "backend.app.services.allegro_scraper.WebDriverWait",
            side_effect=lambda driver, timeout: self._dummy_wait(driver),
        ), patch(
            "backend.app.services.allegro_scraper._detect_no_results", return_value=False
        ), patch(
            "backend.app.services.allegro_scraper._contains_any", return_value=False
        ), patch(
            "backend.app.services.allegro_scraper._extract_listing_snapshot",
            return_value=[{"price": "123", "sold": "7"}],
        ), patch(
            "backend.app.services.allegro_scraper._parse_price", return_value=123.0
        ), patch(
            "backend.app.services.allegro_scraper._parse_sold_count", return_value=7
        ):
            result = scraper.fetch_allegro_data("1234567890123")

        self.assertEqual(result["diagnostics"], diag_payload)
        self.assertEqual(result["lowest_price"], 123.0)
        self.assertEqual(result["sold_count"], 7)

    def test_failure_response_includes_last_diagnostics(self):
        diag_payload = {"error": "diag_failed"}
        driver = DummyDriver()

        with patch.object(scraper, "MAX_ATTEMPTS", 1), patch(
            "backend.app.services.allegro_scraper.get_driver", return_value=driver
        ), patch(
            "backend.app.services.allegro_scraper._run_proxy_diagnostics",
            return_value=diag_payload,
        ), patch("backend.app.services.allegro_scraper._accept_cookies"), patch(
            "backend.app.services.allegro_scraper.WebDriverWait",
            side_effect=lambda driver, timeout: self._dummy_wait(driver),
        ), patch(
            "backend.app.services.allegro_scraper._detect_no_results", return_value=False
        ), patch(
            "backend.app.services.allegro_scraper._contains_any", return_value=False
        ), patch(
            "backend.app.services.allegro_scraper._extract_listing_snapshot",
            return_value=[],
        ), patch(
            "backend.app.services.allegro_scraper._extract_price", return_value=None
        ), patch(
            "backend.app.services.allegro_scraper._extract_sold_count", return_value=None
        ):
            result = scraper.fetch_allegro_data("9876543210987")

        self.assertEqual(result["source"], "failed")
        self.assertEqual(result["diagnostics"], diag_payload)


if __name__ == "__main__":
    unittest.main()
