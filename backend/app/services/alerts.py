"""Helpers for notifying administrators about scraper issues."""

from __future__ import annotations

import json
import logging
import os
from typing import Mapping

import requests

SCRAPER_ALERT_WEBHOOK = os.getenv("SCRAPER_ALERT_WEBHOOK")

logger = logging.getLogger(__name__)


def _format_details(details: Mapping[str, object]) -> str:
    """Return a short string representation of alert details for logs."""

    try:
        return json.dumps(details, ensure_ascii=False, sort_keys=True)
    except Exception:
        return str(details)


def send_scraper_alert(event: str, details: Mapping[str, object]) -> None:
    """Report critical scraper events to administrators.

    If the ``SCRAPER_ALERT_WEBHOOK`` environment variable is configured, the
    payload is POSTed as JSON to that URL. Regardless of webhook availability,
    the alert is logged so that we always have an audit trail.
    """

    payload = {"event": event, "details": details}
    details_str = _format_details(details)

    if SCRAPER_ALERT_WEBHOOK:
        try:
            requests.post(SCRAPER_ALERT_WEBHOOK, json=payload, timeout=5)
        except Exception as exc:  # pragma: no cover - best effort alerting
            logger.error("Nie udało się wysłać alertu webhook: %s", exc)

    logger.warning("[SCRAPER ALERT] %s -> %s", event, details_str)

