@echo off
setlocal
cd /d "%~dp0"

echo [start_all] Podnoszę usługi docker (postgres/redis/backend/frontend)...
docker compose up -d postgres redis backend frontend

echo [start_all] Startuję lokalny agent do sterowania oknami scrapera...
set "AGENT_PY=%~dp0backend\.venv\Scripts\python.exe"
if not exist "%AGENT_PY%" set "AGENT_PY=python"
set REDIS_URL=redis://localhost:6379/0
start "scraper-agent" cmd /k "%AGENT_PY%" "%~dp0scraper_agent.py"

echo [start_all] Gotowe. Frontend: http://localhost:8501
endlocal
