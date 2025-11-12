import unittest
from unittest.mock import patch

import socket

from requests.exceptions import ConnectionError as RequestsConnectionError

import backend.app.services.allegro_scraper as scraper


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


if __name__ == "__main__":
    unittest.main()
