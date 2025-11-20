@echo off
REM Uruchamia lokalnie scraper worker z widoczną przeglądarką (headed).
REM Wymaga wcześniej utworzonego wirtualnego środowiska w katalogu backend\.venv
REM i zainstalowanych zależności z requirements.txt.

setlocal
cd /d "%~dp0backend"

if not exist ".venv\Scripts\activate.bat" (
  echo Brak .venv. Uruchom: python -m venv .venv ^&^& .venv\Scripts\activate.bat ^&^& pip install -r requirements.txt
  exit /b 1
)

call .venv\Scripts\activate.bat

rem Ustawienia połączeń do usług z docker-compose (host: localhost, mapowane porty)
set PYTHONPATH=%cd%
set DATABASE_URL=postgresql+psycopg2://pilot:pilot@localhost:5433/pilotdb
set CELERY_BROKER_URL=redis://localhost:6379/0
set CELERY_RESULT_BACKEND=redis://localhost:6379/0
set SELENIUM_HEADED=true

rem Na Windows prefork ma problemy – używamy pool solo
python -m celery -A app.tasks worker --loglevel=info --queues scraper --pool=solo --concurrency=1
