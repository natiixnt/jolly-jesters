# Plik: backend/app/config.py
import os
from dotenv import load_dotenv
from pathlib import Path

# ... (wczytanie .env tak jak masz)
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# ... (DATABASE_URL, CELERY_BROKER_URL, etc.)
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://appuser:changeme@db:532/appdb")
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0")


# --- Ustawienia scrapera / analizy ---
# Upewnij się, że te linie istnieją:

ALLEGRO_RATE_LIMIT = int(os.getenv("ALLEGRO_RATE_LIMIT", 5))
CACHE_TTL_DAYS = int(os.getenv("CACHE_TTL_DAYS", 30)) # <-- TA ZMIENNA JEST KLUCZOWA
PROFIT_MULTIPLIER = float(os.getenv("PROFIT_MULTIPLIER", 1.5))

DEFAULT_CURRENCY = os.getenv("DEFAULT_CURRENCY", "PLN")