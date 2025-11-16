# Uzywamy obrazu slim, poniewaz nie potrzebujemy juz pelnego debiana dla Chrome
FROM python:3.11-slim-bookworm

WORKDIR /app

# Krok 1: Instalacja minimalnych zaleznosci (np. git moze byc kiedys potrzebny do pip install)
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    git \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Krok 2: skopiowanie i instalacja zależności Pythona
# USUNIETO: COPY vendor ./vendor
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir python-dotenv alembic psycopg2-binary

# Krok 3: Kopiowanie i nadanie uprawnien dla antibot.exe
# ZMIANA: kopiujemy do /usr/local/bin/ zeby nie bylo konfliktu z volume
COPY antibot.exe /usr/local/bin/antibot
RUN chmod +x /usr/local/bin/antibot
  
# kopiowanie kodu
COPY ./app ./app

# expose FastAPI port
EXPOSE 8000

# ustawienie PYTHONPATH
ENV PYTHONPATH=/app

# Ta komenda CMD jest dla backendu, worker nadpisze ją w docker-compose
CMD ["sh", "-c", "\
until pg_isready -h postgres -p 5432; do echo 'Waiting for Postgres...'; sleep 2; done; \
alembic upgrade head; \
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload"]