import unittest
from unittest.mock import patch

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
                side_effect=[RuntimeError("boom"), None],
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
                side_effect=RuntimeError("nope"),
            ):
                with self.assertRaises(RuntimeError) as ctx:
                    scraper._ensure_selenium_ready(timeout=1)

        message = str(ctx.exception)
        self.assertIn("http://bad-grid", message)
        self.assertIn("nope", message)
        self.assertIsNone(scraper._ACTIVE_SELENIUM_URL)


if __name__ == "__main__":
    unittest.main()