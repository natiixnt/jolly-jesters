# uzywamy pelnego obrazu debian  aby moc zainstalowac chrome
FROM python:3.11-bookworm

WORKDIR /app

# krok 1: instalacja podstawowych zaleznosci
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    curl \
    git \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# krok 2: dodanie repo google chrome (potrzebne dla seleniumbase)
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome-keyring.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list

# krok 3: instalacja google chrome (BEZ CHROMEDRIVER)
RUN apt-get update && apt-get install -y \
    google-chrome-stable \
    # chromedriver usuniety - seleniumbase zarzadza nim sam
    # dodajemy zaleznosci, ktorych moze brakowac w slim
    libvulkan1 \
    libnss3 \
    fonts-liberation \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# krok 4: skopiowanie i instalacja zaleznosci pythona
COPY requirements.txt .
# usunieto: COPY vendor ./vendor (juz niepotrzebne)
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir python-dotenv alembic psycopg2-binary

# usunieto: linie zwiazane z antibot.exe (juz niepotrzebne)
  
# kopiowanie kodu
COPY ./app ./app

# expose fastapi port
EXPOSE 8000

# ustawienie pythonpath
ENV PYTHONPATH=/app

# ta komenda cmd jest dla backendu, worker nadpisze ja w docker-compose
CMD ["sh", "-c", "\
until pg_isready -h postgres -p 5432; do echo 'Waiting for Postgres...'; sleep 2; done; \
alembic upgrade head; \
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload"]