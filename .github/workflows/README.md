# Allegro Scraper MVP - instrukcja dla klienta

## Wymagania
- Docker
- Docker Compose

## Uruchomienie
1. Sklonuj repo:
git clone <repo-url>
cd <repo-folder>

markdown
Skopiuj kod
2. Skonfiguruj plik `.env` (jeśli trzeba, zmień dane bazy, klucz Allegro, kurs waluty).
3. Uruchom projekt:
docker-compose up

markdown
Skopiuj kod
4. Web API (FastAPI) będzie dostępne pod: `http://localhost:8000`
5. Frontend (Streamlit) będzie dostępny pod: `http://localhost:8501`

## Import plików
- Wejdź w frontend, wybierz plik Excel (.xlsx/.csv), kategorię, walutę i kurs jeśli trzeba.
- Kliknij „Importuj” i poczekaj na zakończenie procesu.

## Eksport wyników
- Po zakończonej analizie kliknij „Eksportuj do Excel/CSV”.
- Plik pojawi się w folderze `exports` lub zostanie wysłany mailem (jeśli skonfigurowano).

## Uwagi
- Wszystkie dane są przechowywane w lokalnej bazie PostgreSQL.
- Możesz uruchomić wielu workerów Celery na różnych maszynach – wszystkie korzystają z tej samej bazy i kolejki Redis.
