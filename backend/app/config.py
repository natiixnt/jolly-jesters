import os
from dotenv import load_dotenv
from pathlib import Path

# wczytanie .env
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# -----------------------
# baza danych
# -----------------------
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://appuser:changeme@db:5432/appdb")

# -----------------------
# Redis / Celery
# -----------------------
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0")

# -----------------------
# ustawienia scrapera / analizy
# -----------------------
ALLEGRO_RATE_LIMIT = int(os.getenv("ALLEGRO_RATE_LIMIT", 5))  # liczba zapytań na sekundę
CACHE_TTL_DAYS = int(os.getenv("CACHE_TTL_DAYS", 30))
PROFIT_MULTIPLIER = float(os.getenv("PROFIT_MULTIPLIER", 1.5))

# -----------------------
# inne ustawienia
# -----------------------
DEFAULT_CURRENCY = os.getenv("DEFAULT_CURRENCY", "PLN")
