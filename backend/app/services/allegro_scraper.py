# Plik: backend/app/services/allegro_scraper.py

import base64
import importlib.util
import io
import logging
import os
import random
import re
import sys
import time
import zipfile
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
import socket

from requests import Response
from requests.exceptions import ConnectionError as RequestsConnectionError, RequestException
from urllib3.exceptions import NameResolutionError

try:
    from selenium.common.exceptions import TimeoutException
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.remote.webdriver import WebDriver as RemoteWebDriver
except ImportError:  # pragma: no cover - executed only when selenium is absent
    class TimeoutException(Exception):  # type: ignore
        """Minimal substytut TimeoutException gdy Selenium nie jest dostępne."""


    class _MissingSeleniumProxy:
        """Fallback callable that raises a helpful error when Selenium is required."""

        _MESSAGE = (
            "Selenium package is required to run the Allegro scraper. "
            "Install selenium or execute this code in the runtime environment "
            "that provides it."
        )

        def __call__(self, *args, **kwargs):  # noqa: D401 - behaviour documented in _MESSAGE
            raise RuntimeError(self._MESSAGE)

        def __getattr__(self, item):
            raise RuntimeError(self._MESSAGE)

    ChromeOptions = _MissingSeleniumProxy()  # type: ignore
    By = _MissingSeleniumProxy()  # type: ignore
    EC = _MissingSeleniumProxy()  # type: ignore
    WebDriverWait = _MissingSeleniumProxy()  # type: ignore
    RemoteWebDriver = _MissingSeleniumProxy()  # type: ignore

from ..config import (
    PROXY_DIAGNOSTIC_BODY_CHARS,
    PROXY_DIAGNOSTIC_EXPECT,
    PROXY_DIAGNOSTIC_URL,
    PROXY_PASSWORD,
    PROXY_URL,
    PROXY_USERNAME,
    SELENIUM_HEADLESS,
)
from .alerts import send_scraper_alert

SELENIUM_URL = os.getenv("SELENIUM_URL", "http://selenium:4444/wd/hub")
_SELENIUM_URLS_ENV = os.getenv("SELENIUM_URLS")
MAX_ATTEMPTS = 3
BASE_BACKOFF_SECONDS = 2
SELENIUM_READY_TIMEOUT = int(os.getenv("SELENIUM_READY_TIMEOUT", "30"))

class SeleniumEndpointUnavailable(RuntimeError):
    """Sygnałizuje brak dostępnego endpointu Selenium."""


_UNPATCHED_REMOTE_CACHE: Optional[type] = None
_ACTIVE_SELENIUM_URL: Optional[str] = None


def _candidate_selenium_urls() -> List[str]:
    """Zwraca uporządkowaną listę kandydatów endpointów Selenium."""

    if _SELENIUM_URLS_ENV:
        raw_candidates = [u.strip() for u in re.split(r"[;\s,]+", _SELENIUM_URLS_ENV) if u.strip()]
    else:
        raw_candidates = [SELENIUM_URL]

        parsed = urlparse(SELENIUM_URL)
        if parsed.hostname == "selenium":
            port = f":{parsed.port}" if parsed.port else ""
            raw_candidates.append(parsed._replace(netloc=f"localhost{port}").geturl())
            raw_candidates.append(parsed._replace(netloc=f"127.0.0.1{port}").geturl())

    seen: set[str] = set()
    ordered: List[str] = []
    for candidate in raw_candidates:
        if candidate not in seen:
            ordered.append(candidate)
            seen.add(candidate)
    return ordered


SELENIUM_URL_CANDIDATES: List[str] = _candidate_selenium_urls()


def _selenium_status_url(base_url: str) -> str:
    explicit = os.getenv("SELENIUM_STATUS_URL")
    if explicit:
        return explicit

    base = base_url
    if base.endswith("/wd/hub"):
        base = base[: -len("/wd/hub")]
    return f"{base}/status"


def _iter_causes(exc: Exception):
    """Yields an exception and its chained causes/contexts without cycling."""

    seen = set()
    queue = [exc]

    while queue:
        current = queue.pop(0)
        if current is None or id(current) in seen:
            continue

        yield current
        seen.add(id(current))
        queue.append(getattr(current, "__cause__", None))
        queue.append(getattr(current, "__context__", None))


def _find_dns_error(exc: Exception) -> Optional[BaseException]:
    """Returns the first DNS-related cause in the exception chain, if any."""

    for cause in _iter_causes(exc):
        if isinstance(cause, (NameResolutionError, socket.gaierror)):
            return cause

        reason = getattr(cause, "reason", None)
        if isinstance(reason, (NameResolutionError, socket.gaierror)):
            return reason
    return None


def _wait_for_candidate_ready(base_url: str, timeout: int) -> None:
    deadline = time.time() + timeout
    last_error: Optional[Exception] = None
    status_url = _selenium_status_url(base_url)

    while time.time() < deadline:
        try:
            response: Response = requests.get(status_url, timeout=3)
            if response.status_code == 200:
                payload = response.json()
                ready = payload.get("value", {}).get("ready")
                if ready is None:
                    ready = payload.get("ready")
                if ready:
                    return
        except RequestException as exc:
            last_error = exc

            if isinstance(exc, RequestsConnectionError):
                dns_error = _find_dns_error(exc)
                if dns_error is not None:
                    raise SeleniumEndpointUnavailable(
                        f"Nie można rozwiązać nazwy hosta dla {status_url}: {dns_error}"
                    ) from exc
        except ValueError as exc:
            last_error = exc

        time.sleep(1)

    raise SeleniumEndpointUnavailable(
        f"Selenium status not ready after {timeout}s (last error: {last_error})"
    )


def _ensure_selenium_ready(timeout: int = SELENIUM_READY_TIMEOUT) -> str:
    global _ACTIVE_SELENIUM_URL

    if _ACTIVE_SELENIUM_URL is not None:
        return _ACTIVE_SELENIUM_URL

    last_error: Optional[Exception] = None

    for candidate in SELENIUM_URL_CANDIDATES:
        try:
            _wait_for_candidate_ready(candidate, timeout)
            _ACTIVE_SELENIUM_URL = candidate
            logger.info("Selenium endpoint selected: %s", candidate)
            return candidate
        except SeleniumEndpointUnavailable as exc:
            last_error = exc
            logger.warning("Selenium endpoint %s failed readiness check: %s", candidate, exc)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            logger.warning("Selenium endpoint %s failed readiness check: %s", candidate, exc)

    raise SeleniumEndpointUnavailable(
        "Brak dostępnego endpointu Selenium. Próbowano: "
        + ", ".join(SELENIUM_URL_CANDIDATES)
        + (f". Ostatni błąd: {last_error}" if last_error else "")
    )


def _load_unpatched_remote_webdriver() -> type:
    """Ładuje oryginalną klasę RemoteWebDriver z Selenium z pominięciem patchy."""

    global _UNPATCHED_REMOTE_CACHE

    if _UNPATCHED_REMOTE_CACHE is not None:
        return _UNPATCHED_REMOTE_CACHE

    spec = importlib.util.find_spec("selenium.webdriver.remote.webdriver")
    if spec is None or not spec.origin:
        raise RuntimeError("Nie można odnaleźć modułu selenium.webdriver.remote.webdriver")

    module_name = "_selenium_unpatched_remote_webdriver"
    module_spec = importlib.util.spec_from_file_location(module_name, spec.origin)
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError("Nie udało się przygotować specyfikacji modułu RemoteWebDriver")

    module = importlib.util.module_from_spec(module_spec)
    sys.modules[module_name] = module
    try:
        module_spec.loader.exec_module(module)
    finally:
        sys.modules.pop(module_name, None)

    try:
        remote_cls = getattr(module, "WebDriver")
    except AttributeError as exc:
        raise RuntimeError("Załadowany moduł nie zawiera klasy WebDriver") from exc

    _UNPATCHED_REMOTE_CACHE = remote_cls
    return remote_cls


logger = logging.getLogger(__name__)


def _run_proxy_diagnostics(
    driver,
    *,
    proxy_url: Optional[str],
    user_agent: Optional[str],
) -> Optional[dict]:
    """Odwiedza stronę diagnostyczną, aby zebrać informacje o ruchu przez proxy."""

    if not PROXY_DIAGNOSTIC_URL:
        return None

    info = {
        "url": PROXY_DIAGNOSTIC_URL,
        "proxy": proxy_url,
        "user_agent": user_agent,
    }

    try:
        driver.get(PROXY_DIAGNOSTIC_URL)
        time.sleep(1)

        title = driver.title
        current_url = driver.current_url
        body_preview = driver.execute_script(
            "return document.body ? document.body.innerText.slice(0, arguments[0]) : '';",
            PROXY_DIAGNOSTIC_BODY_CHARS,
        )

        info.update(
            {
                "title": title,
                "current_url": current_url,
                "body_preview": body_preview,
            }
        )

        if PROXY_DIAGNOSTIC_EXPECT:
            expectation = PROXY_DIAGNOSTIC_EXPECT
            matched = any(
                expectation in (value or "")
                for value in (title, current_url, body_preview)
            )
            info["expectation_matched"] = matched
            if not matched:
                logger.warning(
                    "Proxy diagnostic expectation not met. Expected '%s' in diagnostic output.",
                    expectation,
                )

        logger.info(
            "Proxy diagnostic completed via %s (title=%s, snippet=%s)",
            PROXY_DIAGNOSTIC_URL,
            title,
            (body_preview or "")[:80],
        )
    except Exception as exc:  # noqa: BLE001
        info["error"] = str(exc)
        logger.warning("Proxy diagnostic failed via %s: %s", PROXY_DIAGNOSTIC_URL, exc)

    return info

USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 16_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
]

PRICE_SELECTORS: List[str] = [
    "div[data-role='price'] span",
    "span[data-role='price']",
    "article[data-role='offer'] span.m9qz_fv",
    "article[data-role='offer'] span.m9qz_Fq",
    "article[data-role='offer'] span[data-analytics-interaction-value='price']",
    "article[data-role='offer'] span[data-testid='ad-price']",
    "article[data-role='offer'] span[data-testid='listing-ad-price']",
]

SOLD_SELECTORS: List[str] = [
    "article[data-role='offer'] span.msa3_z4",
    "article[data-role='offer'] span[data-analytics-interaction-value='sold-count']",
    "article[data-role='offer'] span[data-role='sold-counter']",
    "article[data-role='offer'] span[class*='sold']",
    "article[data-role='offer'] span[data-testid='sold-counter']",
]

BAN_KEYWORDS: List[str] = [
    "twoje żądanie zostało zablokowane",
    "przepraszamy, ale",
    "zbyt wiele zapytań",
    "blocked",
]

CAPTCHA_KEYWORDS: List[str] = [
    "captcha",
    "verify you are human",
    "potwierdź, że nie jesteś robotem",
]

CONSENT_BUTTON_SELECTORS: List[str] = [
    "button[data-role='accept-consent']",
    "button[data-testid='accept-consent-button']",
    "button#didomi-notice-agree-button",
]

NO_RESULTS_KEYWORDS: List[str] = [
    "nie znaleźliśmy",
    "brak wyników",
    "spróbuj zmienić frazę",
]


def _parse_price(price_text: str) -> Optional[float]:
    """Helper do parsowania ceny (usuwa 'zł', ' ', ',')"""
    if not price_text:
        return None
    try:
        txt = "".join(ch for ch in price_text if ch.isdigit() or ch in ".,")
        txt = txt.replace(",", ".").replace(" ", "")
        return float(txt) if txt else None
    except Exception:
        return None


def _parse_sold_count(sold_text: str) -> Optional[int]:
    """Helper do parsowania liczby sprzedanych (np. '100 osób kupiło')"""
    if not sold_text:
        return None
    try:
        match = re.search(r"\d+", sold_text.replace(" ", ""))
        return int(match.group(0)) if match else None
    except Exception:
        return None


def _split_proxy_values() -> List[str]:
    """Zwraca listę dostępnych proxy na podstawie PROXY_URL"""
    if not PROXY_URL:
        return []

    candidates = re.split(r"[;,\s]+", PROXY_URL)
    proxies: List[str] = []
    for candidate in candidates:
        candidate = candidate.strip()
        if not candidate:
            continue
        if "://" not in candidate:
            candidate = f"http://{candidate}"
        proxies.append(candidate)
    return proxies


def _build_proxy_extension(
    scheme: str, host: str, port: int, username: str, password: str
) -> str:
    """Tworzy w locie rozszerzenie Chrome z uwierzytelnianiem proxy"""
    manifest_json = """
    {
        "version": "1.0.0",
        "manifest_version": 3,
        "name": "Chrome Proxy",
        "permissions": [
            "proxy",
            "webRequest",
            "webRequestAuthProvider"
        ],
        "host_permissions": [
            "<all_urls>"
        ],
        "background": {{
            "service_worker": "background.js"
        }}
    }}
    """

    # Ta logika jest poprawna - łączy V3 'proxy.settings' z 'webRequest'
    background_js = f"""
    var config = {{
            mode: "fixed_servers",
            rules: {{
            singleProxy: {{
                scheme: "{scheme}",
                host: "{host}",
                port: parseInt({port})
            }},
            bypassList: ["localhost", "127.0.0.1"]
            }}
        }};

    chrome.proxy.settings.set({{value: config, scope: "regular"}}, function() {{}});

    function callbackFn(details) {{
        return {{
            authCredentials: {{
                username: "{username}",
                password: "{password}"
            }}
        }};
    }}

    chrome.webRequest.onAuthRequired.addListener(
                callbackFn,
                {{ "urls": ["<all_urls>"] }},
                ['blocking', 'asyncBlocking']
    );
    """

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr("manifest.json", manifest_json)
        zip_file.writestr("background.js", background_js)

    return base64.b64encode(zip_buffer.getvalue()).decode("utf-8")


def _prepare_proxy(
    proxy_url: str,
) -> Tuple[Optional[str], Optional[str], Optional[Dict[str, str]]]:
    """Zwraca argument proxy, rozszerzenie oraz dodatkowe capability"""
    parsed = urlparse(proxy_url if "://" in proxy_url else f"http://{proxy_url}")
    host = parsed.hostname
    port = parsed.port
    if not host or not port:
        return None, None, None

    scheme = parsed.scheme or "http"
    proxy_argument = f"{scheme}://{host}:{port}"

    username = parsed.username or PROXY_USERNAME
    password = parsed.password or PROXY_PASSWORD

    if username and password:
        extension = _build_proxy_extension(scheme, host, port, username, password)
        capability = {
            "proxyType": "manual",
            "httpProxy": f"{username}:{password}@{host}:{port}",
            "sslProxy": f"{username}:{password}@{host}:{port}",
        }
        return proxy_argument, extension, capability

    return proxy_argument, None, None


def get_driver(user_agent: Optional[str] = None, proxy_url: Optional[str] = None):
    """Tworzy instancję zdalnej przeglądarki Chrome w kontenerze Selenium"""
    options = ChromeOptions()

    if SELENIUM_HEADLESS:
        options.add_argument("--headless=new")
    else:
        logger.info("Selenium running in non-headless mode (VNC debugging enabled)")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("start-maximized")
    options.add_argument("disable-infobars")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    if user_agent:
        options.add_argument(f"user-agent={user_agent}")

    if proxy_url:
        proxy_argument, proxy_extension, proxy_capability = _prepare_proxy(proxy_url)
        if proxy_argument:
            options.add_argument(f"--proxy-server={proxy_argument}")
        if proxy_extension:
            options.add_encoded_extension(proxy_extension)
        if proxy_capability:
            options.set_capability("proxy", proxy_capability)

    selenium_endpoint = _ensure_selenium_ready()

    driver_factory = RemoteWebDriver
    try:
        driver = driver_factory(
            command_executor=selenium_endpoint,
            options=options,
        )
    except TypeError as exc:
        if "desired_capabilities" not in str(exc):
            raise

        logger.warning(
            "RemoteWebDriver zgłosił TypeError dotyczący desired_capabilities, próbuję ponownie z klasą bez patchy: %s",
            exc,
        )
        driver_factory = _load_unpatched_remote_webdriver()
        driver = driver_factory(
            command_executor=selenium_endpoint,
            options=options,
        )

    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.set_page_load_timeout(30)

    return driver


def _contains_any(source: str, keywords: List[str]) -> bool:
    """Sprawdza, czy w źródle znajduje się jakiekolwiek słowo kluczowe"""
    lower_source = source.lower()
    return any(keyword in lower_source for keyword in keywords)


def _find_first_text(driver, selectors: List[str]) -> Optional[str]:
    """Zwraca tekst pierwszego znalezionego elementu dla podanych selektorów"""
    for selector in selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
        except Exception:
            continue
        for element in elements:
            text = (element.text or "").strip()
            if not text:
                text = (element.get_attribute("innerText") or "").strip()
            if text:
                return text
    return None


def _detect_no_results(page_source: str) -> bool:
    """Detekcja strony z brakiem wyników"""
    return _contains_any(page_source, NO_RESULTS_KEYWORDS)


def _accept_cookies(driver) -> bool:
    """Próbuje zaakceptować zgodę na cookies, jeśli baner ją blokuje"""
    for selector in CONSENT_BUTTON_SELECTORS:
        try:
            button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
            )
            button.click()
            time.sleep(0.5)
            return True
        except Exception:
            continue

    # fallback na wyszukiwanie po tekście
    try:
        button = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((
                By.XPATH,
                "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'przejdź do serwisu')]",
            ))
        )
        button.click()
        time.sleep(0.5)
        return True
    except Exception:
        return False


def _extract_price(driver, page_source: str) -> Optional[float]:
    """Pobiera najniższą cenę z listingu"""
    price_text = _find_first_text(driver, PRICE_SELECTORS)
    if price_text:
        parsed = _parse_price(price_text)
        if parsed is not None:
            return parsed

    match = re.search(r'"price"\s*:\s*\{\s*"amount"\s*:\s*"([\d.,]+)"', page_source)
    if not match:
        match = re.search(r'"price"\s*:\s*\{\s*"value"\s*:\s*"([\d.,]+)"', page_source)
    if match:
        return _parse_price(match.group(1))

    match = re.search(r'data-analytics-price\s*=\s*"([\d.,]+)"', page_source)
    if not match:
        match = re.search(r'"lowestPrice"\s*:\s*"([\d.,]+)"', page_source)
    if match:
        return _parse_price(match.group(1))

    return None


def _extract_sold_count(driver, page_source: str) -> Optional[int]:
    """Pobiera liczbę sprzedanych sztuk"""
    sold_text = _find_first_text(driver, SOLD_SELECTORS)
    if sold_text:
        parsed = _parse_sold_count(sold_text)
        if parsed is not None:
            return parsed

    match = re.search(r"(\d+)\s*(osób kupiło|sprzedanych|sold)", page_source, re.IGNORECASE)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None

    match = re.search(r'"soldQuantity"\s*:\s*(\d+)', page_source)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None


def _extract_listing_snapshot(driver) -> List[dict]:
    """Pobiera z DOM kilka pierwszych ofert wraz z ceną i liczbą sprzedaży"""
    try:
        entries = driver.execute_script(
            """
            const offers = Array.from(document.querySelectorAll("article[data-role='offer']"));
            return offers.slice(0, 5).map(offer => ({
                price: (offer.querySelector("[data-role='price'], span[data-testid='ad-price'], span[data-testid='listing-ad-price']")?.textContent || '').trim(),
                sold: (offer.querySelector("[data-role='sold-counter'], span[data-testid='sold-counter'], span[class*='sold']")?.textContent || '').trim()
            }));
            """
        )
        return entries or []
    except Exception:
        return []


def _failure_response(
    ean: str, error: Optional[Exception], diagnostics: Optional[dict] = None
):
    """Buduje odpowiedź dla nieudanej próby wraz z alertem."""

    logger.error("Nie udało się pobrać danych Allegro dla EAN %s: %s", ean, error)

    alert_sent = False
    if error:
        send_scraper_alert(
            "allegro_scrape_failed",
            {
                "ean": ean,
                "error": str(error),
            },
        )
        alert_sent = True

    return {
        "lowest_price": None,
        "sold_count": None,
        "source": "failed",
        "fetched_at": datetime.now(timezone.utc),
        "not_found": False,
        "error": str(error) if error else None,
        "alert_sent": alert_sent,
        "diagnostics": diagnostics,
    }


def fetch_allegro_data(ean: str, use_api: bool = False, api_key: Optional[str] = None):
    """Scraper Allegro z rotacją proxy/UA, retry i detekcją banów"""

    if use_api and api_key:
        # API jest opcjonalne – aktualnie pomijamy, bo nie zwraca wymaganych danych
        pass

    proxies = _split_proxy_values()
    agents_pool = USER_AGENTS[:]
    random.shuffle(agents_pool)
    proxies_cycle = proxies.copy()
    if proxies_cycle:
        random.shuffle(proxies_cycle)

    last_error: Optional[Exception] = None
    last_diagnostics: Optional[dict] = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        user_agent = agents_pool[(attempt - 1) % len(agents_pool)] if agents_pool else None
        proxy_url = None
        if proxies_cycle:
            proxy_url = proxies_cycle[(attempt - 1) % len(proxies_cycle)]

        diagnostics_info = None
        driver = None
        try:
            driver = get_driver(user_agent=user_agent, proxy_url=proxy_url)
            diagnostics_info = _run_proxy_diagnostics(
                driver,
                proxy_url=proxy_url,
                user_agent=user_agent,
            )
            if diagnostics_info:
                last_diagnostics = diagnostics_info.copy()
            listing_url = f"https://allegro.pl/listing?string={ean}"
            driver.get(listing_url)

            _accept_cookies(driver)

            try:
                WebDriverWait(driver, 15).until(
                    lambda d: (
                        _detect_no_results(d.page_source)
                        or bool(d.find_elements(By.CSS_SELECTOR, "article[data-role='offer']"))
                    )
                )
            except TimeoutException as exc:
                last_error = exc
                logger.warning("Timeout oczekiwania na listing (attempt %s, EAN %s)", attempt, ean)
                continue

            # dodatkowy, krótki czas na pełne wczytanie komponentów cenowych
            time.sleep(1 + random.uniform(0.2, 0.8))

            page_source = driver.page_source

            if _contains_any(page_source, BAN_KEYWORDS):
                logger.warning("Wykryto bana przy EAN %s (proxy=%s, ua=%s)", ean, proxy_url, user_agent)
                send_scraper_alert(
                    "allegro_ban_detected",
                    {
                        "ean": ean,
                        "proxy": proxy_url or "<brak>",
                        "user_agent": user_agent or "<brak>",
                    },
                )
                return {
                    "lowest_price": None,
                    "sold_count": None,
                    "source": "ban_detected",
                    "fetched_at": datetime.now(timezone.utc),
                    "not_found": False,
                    "alert_sent": True,
                    "diagnostics": diagnostics_info or last_diagnostics,
                }

            if _contains_any(page_source, CAPTCHA_KEYWORDS):
                logger.warning("Wykryto CAPTCHA przy EAN %s (proxy=%s, ua=%s)", ean, proxy_url, user_agent)
                send_scraper_alert(
                    "allegro_captcha_detected",
                    {
                        "ean": ean,
                        "proxy": proxy_url or "<brak>",
                        "user_agent": user_agent or "<brak>",
                    },
                )
                return {
                    "lowest_price": None,
                    "sold_count": None,
                    "source": "captcha_detected",
                    "fetched_at": datetime.now(timezone.utc),
                    "not_found": False,
                    "alert_sent": True,
                    "diagnostics": diagnostics_info or last_diagnostics,
                }

            if _detect_no_results(page_source):
                return {
                    "lowest_price": None,
                    "sold_count": None,
                    "source": "scrape",
                    "fetched_at": datetime.now(timezone.utc),
                    "not_found": True,
                    "diagnostics": diagnostics_info or last_diagnostics,
                }

            listing_snapshot = _extract_listing_snapshot(driver)
            if listing_snapshot:
                for snapshot_entry in listing_snapshot:
                    if snapshot_entry.get("price"):
                        parsed_price = _parse_price(snapshot_entry["price"])
                        if parsed_price is not None:
                            sold_from_snapshot = _parse_sold_count(snapshot_entry.get("sold"))
                            return {
                                "lowest_price": parsed_price,
                                "sold_count": sold_from_snapshot,
                                "source": "selenium_rotating",
                                "fetched_at": datetime.now(timezone.utc),
                                "not_found": False,
                                "diagnostics": diagnostics_info,
                            }

            lowest_price = _extract_price(driver, page_source)
            sold_count = _extract_sold_count(driver, page_source)

            if lowest_price is not None:
                return {
                    "lowest_price": lowest_price,
                    "sold_count": sold_count,
                    "source": "selenium_rotating",
                    "fetched_at": datetime.now(timezone.utc),
                    "not_found": False,
                    "diagnostics": diagnostics_info,
                }
            else:
                last_error = RuntimeError("Price not found in Allegro listing HTML")

        except TimeoutException as exc:
            last_error = exc
            logger.warning("Timeout Selenium (attempt %s, EAN %s): %s", attempt, ean, exc)
        except SeleniumEndpointUnavailable as exc:
            last_error = exc
            logger.error("Błąd infrastruktury Selenium (attempt %s, EAN %s): %s", attempt, ean, exc)
            return _failure_response(ean, exc, diagnostics_info or last_diagnostics)
        except Exception as exc:
            last_error = exc
            logger.exception("Błąd Selenium (attempt %s, EAN %s)", attempt, ean)
        finally:
            if driver:
                driver.quit()

        # backoff przed kolejną próbą
        if attempt != MAX_ATTEMPTS:
            time.sleep(BASE_BACKOFF_SECONDS * attempt)

    return _failure_response(ean, last_error, last_diagnostics)
