#!/bin/sh
# entrypoint.sh
# uruchom migracje
alembic upgrade head
# potem start backendu
uvicorn app.main:app --host 0.0.0.0 --port 8000
