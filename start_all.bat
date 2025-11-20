@echo off
setlocal
set "BASE=%~dp0"
pushd "%BASE%"

echo [start_all] Starting docker services (postgres/redis/backend/frontend/worker)...
docker compose up -d postgres redis backend frontend worker

echo [start_all] Clearing scraper queue in Redis...
docker compose exec redis redis-cli del scraper

echo [start_all] Starting local scraper agent...
set "AGENT_PY=%BASE%backend\.venv\Scripts\python.exe"
if not exist "%AGENT_PY%" set "AGENT_PY=python"
set "REDIS_URL=redis://localhost:6379/0"
start "scraper-agent" /d "%BASE%" cmd /k ""%AGENT_PY%" "%BASE%scraper_agent.py""

echo [start_all] Ready. Frontend: http://localhost:8501
popd
endlocal
