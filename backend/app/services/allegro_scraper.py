"""Scraper Allegro wykorzystujący httpx_impersonate do maskowania fingerprintu TLS."""

from __future__ import annotations

import logging
import random
import re
import secrets
import time
from datetime import datetime, timezone
from typing import Callable, Dict, Optional
from urllib.parse import quote, quote_plus, urlparse, urlunparse

try:  # pragma: no cover - fallback dla środowisk testowych bez httpx
    from httpx import HTTPError
except ModuleNotFoundError:  # pragma: no cover
    class HTTPError(Exception):
        """Minimalny substytut wyjątku HTTPError na potrzeby testów offline."""


from httpx_impersonate import Client

from ..config import PROXY_PASSWORD, PROXY_URL, PROXY_USERNAME
from .alerts import send_scraper_alert

logger = logging.getLogger(__name__)

LISTING_URL_TEMPLATE = "https://allegro.pl/listing?string={query}"

BASE_HEADERS: Dict[str, str] = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

USER_AGENTS = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) "
        "Gecko/20100101 Firefox/121.0"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1_2) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15"
    ),
]

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


def _augment_proxy_username(username: str) -> str:
    """Dodaje identyfikator sesji Smartproxy aby wymusić rotację IP."""

    lowered = username.lower()
    if "session-" in lowered:
        return username

    suffix = secrets.token_hex(4)
    return f"{username}_session-{suffix}"


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
        username = _augment_proxy_username(username)
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


def _build_headers() -> Dict[str, str]:
    headers = dict(BASE_HEADERS)
    headers["User-Agent"] = random.choice(USER_AGENTS)
    return headers


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


def _request_with_retries(
    client: Client,
    url: str,
    headers_factory: Callable[[], Dict[str, str]],
    *,
    max_attempts: int,
    sleep_func: Callable[[float], None],
) -> object:
    """Wysyła żądanie GET z kontrolą limitów i ponowień."""

    last_response: Optional[object] = None
    last_error: Optional[Exception] = None

    for attempt in range(1, max_attempts + 1):
        delay = random.uniform(1.0, 3.0)
        sleep_func(delay)

        headers = headers_factory()

        try:
            response = client.get(url, headers=headers)
        except HTTPError as exc:
            last_error = exc
            if attempt == max_attempts:
                raise
            backoff = (2 ** attempt) + random.uniform(0.5, 1.5)
            sleep_func(backoff)
            continue
        except Exception:
            raise

        last_response = response

        if getattr(response, "status_code", 200) == 429 and attempt < max_attempts:
            backoff = (2 ** attempt) + random.uniform(0.5, 1.5)
            sleep_func(backoff)
            continue

        return response

    if last_response is not None:
        return last_response

    if last_error is not None:
        raise last_error

    raise RuntimeError("Brak odpowiedzi z Allegro pomimo ponowień")


def fetch_allegro_data(
    ean: str,
    *,
    max_attempts: int = 3,
    sleep_func: Callable[[float], None] = time.sleep,
    **kwargs,
) -> Dict[str, object]:
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
            response = _request_with_retries(
                client,
                url,
                _build_headers,
                max_attempts=max_attempts,
                sleep_func=sleep_func,
            )
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

