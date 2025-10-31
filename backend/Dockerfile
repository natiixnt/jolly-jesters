FROM python:3.11-slim

# ustawienie katalogu roboczego
WORKDIR /app

# skopiowanie i instalacja zależności
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

CMD ["sh", "-c", "\
until pg_isready -h postgres -p 5432; do echo 'Waiting for Postgres...'; sleep 2; done; \
alembic upgrade head; \
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload"]
