FROM python:3.11-slim

WORKDIR /app

# kopiujemy tylko wymagania i instalujemy, aby przy zmianie kodu nie instalować wszystkiego od nowa
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

# uruchamiamy streamlit na 0.0.0.0, port 8501
CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
