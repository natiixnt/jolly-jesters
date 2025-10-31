# app/import_handlers.py
from fileinput import filename
import os
from fastapi import UploadFile
from datetime import datetime
from sqlalchemy.orm import Session
from .database import SessionLocal
from . import models
from .tasks import parse_import_file

UPLOAD_DIR = "uploads"

if not os.path.exists(UPLOAD_DIR):
    os.makedirs(UPLOAD_DIR)

def save_upload_file(upload_file: UploadFile) -> str:
    """
    zapisuje plik na dysku w folderze uploads, zwraca ścieżkę
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    filename = f"{timestamp}_{upload_file.filename}"
    filepath = os.path.join(UPLOAD_DIR, filename)
    with open(filepath, "wb") as f:
        f.write(upload_file.file.read())
    return filepath

def start_import(user_id: int, upload_file: UploadFile, category: str = None, currency: str = None):
    """
    Tworzy ImportJob, zapisuje plik, uruchamia parse_import_file task
    """
    db: Session = SessionLocal()
    try:
        # zapis pliku
        filepath = save_upload_file(upload_file)

        # utworzenie rekordu ImportJob
        job = models.ImportJob(
            filename=filename,
            category=category,
            currency=currency,
            multiplier=settings.MULTIPLIER,
            status="pending",
            meta={"category": category, "currency": currency},
        )

        db.add(job)
        db.commit()
        db.refresh(job)

        # uruchomienie taska Celery
        parse_import_file.delay(job.id, filepath)

        return {
            "import_job_id": job.id,
            "status": "started",
            "filepath": filepath
        }
    finally:
        db.close()

def validate_file(upload_file: UploadFile):
    """
    szybka wstępna walidacja kolumn (EAN, name, price)
    """
    import pandas as pd
    try:
        if upload_file.filename.lower().endswith(".csv"):
            df = pd.read_csv(upload_file.file, nrows=5, dtype=str)
        else:
            df = pd.read_excel(upload_file.file, nrows=5, dtype=str)
        cols = [c.lower() for c in df.columns]
        required = ["ean", "name", "price"]
        missing = [c for c in required if not any(rc in cols for rc in [c])]
        return {"ok": len(missing)==0, "missing_columns": missing}
    except Exception as e:
        return {"ok": False, "error": str(e)}
