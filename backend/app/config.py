# Plik: backend/app/config.py
import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# ... (wczytanie .env tak jak masz)
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# ... (DATABASE_URL, CELERY_BROKER_URL, etc.)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://pilot:pilot@postgres:5432/pilotdb",
)
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0")


# --- Ustawienia scrapera / analizy ---
# Upewnij się, że te linie istnieją:

ALLEGRO_RATE_LIMIT = int(os.getenv("ALLEGRO_RATE_LIMIT", 5))
CACHE_TTL_DAYS = int(os.getenv("CACHE_TTL_DAYS", 30)) # <-- TA ZMIENNA JEST KLUCZOWA
PROFIT_MULTIPLIER = float(os.getenv("PROFIT_MULTIPLIER", 1.5))

DEFAULT_CURRENCY = os.getenv("DEFAULT_CURRENCY", "PLN")

# -----------------------
# Ustawienia Proxy (Krok 27)
# -----------------------
# Adres URL do bramy proxy (np. http://uzytkownik:haslo@brama.proxy.com:port)
PROXY_URL = os.getenv("PROXY_URL", None)
SCRAPER_ALERT_WEBHOOK = os.getenv("SCRAPER_ALERT_WEBHOOK")


def _parse_bool(value: Optional[str], *, default: bool = True) -> bool:
    """Parse bool-like environment flag values."""

    if value is None:
        return default

    return value.strip().lower() not in {"0", "false", "no", "off"}


SELENIUM_HEADLESS = _parse_bool(os.getenv("SELENIUM_HEADLESS"), default=True)

