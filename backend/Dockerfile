# Używamy pełnego obrazu debian, aby móc zainstalować Chrome
FROM python:3.11-slim-bookworm

WORKDIR /app

# Krok 1: Instalacja podstawowych zależności
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    curl \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Krok 2: Dodanie repo Google Chrome (poprawna metoda z GPG)
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome-keyring.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list

# Krok 3: Instalacja Google Chrome i jego zależności
RUN apt-get update && apt-get install -y \
    google-chrome-stable \
    # Dodajemy zależności, których może brakować w slim
    libvulkan1 \
    libnss3 \
    fonts-liberation \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Krok 4: skopiowanie i instalacja zależności Pythona
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir python-dotenv alembic psycopg2-binary

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