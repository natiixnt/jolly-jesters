"""Scraper Allegro wykorzystujący httpx_impersonate do maskowania fingerprintu TLS."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Dict, Optional
from urllib.parse import quote, quote_plus, urlparse, urlunparse

from httpx import HTTPError
from httpx_impersonate import Client

from ..config import PROXY_PASSWORD, PROXY_URL, PROXY_USERNAME
from .alerts import send_scraper_alert

logger = logging.getLogger(__name__)

LISTING_URL_TEMPLATE = "https://allegro.pl/listing?string={query}"

DEFAULT_HEADERS: Dict[str, str] = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}

BAN_KEYWORDS = [
    "twoje żądanie zostało zablokowane",
    "przepraszamy, ale",
    "zbyt wiele zapytań",
    "blocked",
]

CAPTCHA_KEYWORDS = [
    "captcha",
    "verify you are human",
    "potwierdź, że nie jesteś robotem",
]

NO_RESULTS_KEYWORDS = [
    "nie znaleźliśmy",
    "brak wyników",
    "spróbuj zmienić frazę",
]

PRICE_REGEXES = [
    re.compile(r"\"price\"\s*:\s*\{\s*\"amount\"\s*:\s*\"([\d., ]+)\"", re.IGNORECASE),
    re.compile(r"\"amount\"\s*:\s*\"([\d., ]+)\"", re.IGNORECASE),
    re.compile(r"data-testid=\"listing-ad-price\"[^>]*>([\d., ]+)\s*zł", re.IGNORECASE),
]

SOLD_REGEXES = [
    re.compile(r"(\d+)\s*(?:osób kupiło|osoby kupiły|sprzedanych|sold)", re.IGNORECASE),
    re.compile(r"sprzedano\s*(\d+)", re.IGNORECASE),
]


def _build_proxy_url() -> Optional[Dict[str, str]]:
    """Zwraca słownik proxy dla httpx z poprawnie osadzonymi poświadczeniami."""

    if not PROXY_URL:
        return None

    parsed = urlparse(PROXY_URL)
    scheme = parsed.scheme or "http"
    hostname = parsed.hostname

    if not hostname:
        logger.warning("Niepoprawna konfiguracja PROXY_URL: brak hosta")
        return None

    port = f":{parsed.port}" if parsed.port else ""
    username = PROXY_USERNAME or parsed.username
    password = PROXY_PASSWORD or parsed.password

    if username and password:
        netloc = f"{quote(username, safe='')}:{quote(password, safe='')}@{hostname}{port}"
    else:
        netloc = f"{hostname}{port}"

    proxy_target = urlunparse((scheme, netloc, parsed.path or "", parsed.params, parsed.query, parsed.fragment))

    return {
        "http://": proxy_target,
        "https://": proxy_target,
    }


def _contains_keyword(text: str, keywords: list[str]) -> bool:
    lowered = text.lower()
    return any(keyword in lowered for keyword in keywords)


def _clean_number(value: str) -> Optional[float]:
    filtered = "".join(ch for ch in value if ch.isdigit() or ch in ".,")
    if not filtered:
        return None
    normalized = filtered.replace(" ", "").replace(",", ".")
    try:
        return float(normalized)
    except ValueError:
        return None


def _extract_price(html: str) -> Optional[float]:
    for pattern in PRICE_REGEXES:
        match = pattern.search(html)
        if match:
            price = _clean_number(match.group(1))
            if price is not None:
                return price
    return None


def _extract_sold_count(html: str) -> Optional[int]:
    for pattern in SOLD_REGEXES:
        match = pattern.search(html)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                continue
    return None


def _base_result(
    *,
    source: str,
    fetched_at: datetime,
    lowest_price: Optional[float] = None,
    sold_count: Optional[int] = None,
    not_found: bool = False,
    error: Optional[str] = None,
    alert_sent: bool = False,
) -> Dict[str, object]:
    return {
        "lowest_price": lowest_price,
        "sold_count": sold_count,
        "source": source,
        "fetched_at": fetched_at,
        "not_found": not_found,
        "error": error,
        "alert_sent": alert_sent,
    }


def fetch_allegro_data(ean: str, **kwargs) -> Dict[str, object]:
    """Pobiera dane ofert z Allegro wykorzystując lekki klient HTTP."""

    fetched_at = datetime.now(timezone.utc)
    proxies = _build_proxy_url()
    url = LISTING_URL_TEMPLATE.format(query=quote_plus(ean))

    try:
        with Client(
            proxies=proxies,
            impersonate="chrome120",
            timeout=30.0,
            follow_redirects=True,
        ) as client:
            response = client.get(url, headers=DEFAULT_HEADERS)
    except HTTPError as exc:
        logger.error("HTTP błąd podczas pobierania Allegro EAN %s: %s", ean, exc)
        return _base_result(
            source="failed",
            fetched_at=fetched_at,
            error=str(exc),
        )
    except Exception as exc:  # pragma: no cover - ochrona przed nieoczekiwanymi wyjątkami
        logger.exception("Nieoczekiwany błąd httpx dla EAN %s", ean)
        return _base_result(
            source="failed",
            fetched_at=fetched_at,
            error=str(exc),
        )

    html = response.text

    if response.status_code in {403, 429} or _contains_keyword(html, BAN_KEYWORDS):
        message = f"Allegro zablokowało zapytanie (status {response.status_code})."
        send_scraper_alert("allegro_ban_detected", {"ean": ean, "status": response.status_code})
        return _base_result(
            source="ban_detected",
            fetched_at=fetched_at,
            error=message,
            alert_sent=True,
        )

    if _contains_keyword(html, CAPTCHA_KEYWORDS):
        message = "Allegro wymaga weryfikacji CAPTCHA."
        send_scraper_alert("allegro_captcha_detected", {"ean": ean})
        return _base_result(
            source="captcha_detected",
            fetched_at=fetched_at,
            error=message,
            alert_sent=True,
        )

    if response.status_code == 404 or _contains_keyword(html, NO_RESULTS_KEYWORDS):
        return _base_result(
            source="allegro_httpx",
            fetched_at=fetched_at,
            not_found=True,
        )

    price = _extract_price(html)
    sold_count = _extract_sold_count(html)

    if price is None and sold_count is None:
        logger.warning("Nie udało się odnaleźć danych w HTML Allegro dla EAN %s", ean)
        return _base_result(
            source="failed",
            fetched_at=fetched_at,
            error="Brak danych w odpowiedzi Allegro",
        )

    return _base_result(
        source="allegro_httpx",
        fetched_at=fetched_at,
        lowest_price=price,
        sold_count=sold_count,
    )

