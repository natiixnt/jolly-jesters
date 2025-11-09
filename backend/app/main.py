# Plik: backend/app/main.py

import os
import uuid
import shutil
from fastapi import FastAPI, UploadFile, File, Form, Depends, HTTPException
from .database import Base, engine, get_db
from sqlalchemy.orm import Session
from . import models, tasks
# Poprawka: Importujemy też config dla mnożnika
from . import config 
from .schemas import ImportStartResponse, JobStatus, ProductAnalysis
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Import Service Pilot")

WORKSPACE = os.getenv("WORKSPACE", "/workspace")
UPLOAD_DIR = Path(WORKSPACE) / "data" / "uploads"
EXPORT_DIR = Path(WORKSPACE) / "data" / "exports"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
EXPORT_DIR.mkdir(parents=True, exist_ok=True)


@app.post("/api/imports/start", response_model=ImportStartResponse)
async def start_import(
    file: UploadFile = File(...),
    category: str = Form(...),
    currency: str = Form(...),
    db: Session = Depends(get_db)
):
    filename = f"{uuid.uuid4().hex}_{file.filename}"
    filepath = UPLOAD_DIR / filename
    try:
        with open(filepath, "wb") as f:
            shutil.copyfileobj(file.file, f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Nie można zapisać pliku: {e}")

    # --- POPRAWKA BŁĘDU TypeError ---
    # Zapisujemy 'category' i 'currency' w polu 'meta' (JSON)
    # Model ImportJob nie ma już pola 'category'
    job = models.ImportJob(
        filename=str(filepath.name),
        status="pending",
        # Usunięto: category=category,
        meta={"currency": currency, "category": category}, # <-- Poprawny zapis
        # Dodajemy multiplier z configu, jak w modelu
        multiplier=config.PROFIT_MULTIPLIER 
    )
    # --- KONIEC POPRAWKI ---
    
    db.add(job)
    db.commit()
    db.refresh(job)

    try:
        # Uruchamiamy zadanie Celery
        tasks.parse_import_file.delay(job.id, str(filepath))

        # Ustawiamy status "queued" tylko jeśli zadanie nie zdążyło jeszcze
        # przejść w stan "processing" (może się to stać bardzo szybko, zanim
        # backend zdąży wykonać commit). Dzięki temu unikamy nadpisywania
        # informacji ustawionej już przez worker.
        rows_updated = (
            db.query(models.ImportJob)
            .filter(
                models.ImportJob.id == job.id,
                models.ImportJob.status.in_(["pending", "queued"]),
            )
            .update({"status": "queued"}, synchronize_session=False)
        )
        if rows_updated:
            db.commit()
        else:
            db.rollback()
    except Exception as e:
        # Błąd przy kolejkowaniu (np. Redis nie działa)
        job.status = "error"
        job.notes = f"Błąd kolejkowania Celery: {e}"
        db.commit()
        raise HTTPException(status_code=500, detail=f"Błąd Celery: {e}")

    return {"job_id": job.id, "message": "Plik zapisany, zadanie parsowania zakolejkowane."}


@app.get("/api/imports/{job_id}/status", response_model=JobStatus)
def job_status(job_id: int, db: Session = Depends(get_db)):
    job = db.query(models.ImportJob).filter(models.ImportJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
        
    # Zwracamy też notatki, jeśli wystąpił błąd
    return {
        "id": job.id, 
        "status": job.status, 
        "meta": job.meta, 
        "notes": getattr(job, 'notes', None) # Dodajemy notes do odpowiedzi
    }


@app.get("/api/imports/{job_id}/products", response_model=list[ProductAnalysis])
def list_products(job_id: int, db: Session = Depends(get_db)):
    job = db.query(models.ImportJob).filter(models.ImportJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    products = (
        db.query(models.ProductInput)
        .filter(models.ProductInput.import_job_id == job_id)
        .all()
    )
    
    results = []
    
    # Pobierz wszystkie cache jednym zapytaniem (optymalizacja)
    eans = [p.ean for p in products]
    cache_data = db.query(models.AllegroCache).filter(models.AllegroCache.ean.in_(eans)).all()
    cache_map = {c.ean: c for c in cache_data}
    
    # Użyj mnożnika zapisanego w jobie (zgodnie z MVP to 1.5)
    multiplier = job.multiplier if job.multiplier else config.PROFIT_MULTIPLIER

    for p in products:
        cache = cache_map.get(p.ean)
        
        lowest_price = cache.lowest_price if cache else None
        sold_count = cache.sold_count if cache else None
        source = cache.source if cache else None
        last_checked = cache.fetched_at if cache else None
        profit_margin = None
        recommendation = "brak danych" # Domyślnie

        if p.status == "done" and lowest_price is not None and p.purchase_price is not None:
            if p.purchase_price > 0:
                profit_margin = round(lowest_price / p.purchase_price, 2)
                recommendation = (
                    "opłacalny"
                    if profit_margin >= multiplier else "nieopłacalny"
                )
            else:
                recommendation = "opłacalny" # Cena zakupu 0
        elif p.status == "not_found":
            recommendation = "brak na Allegro"
        elif p.status == "error":
            recommendation = "błąd pobierania"
        elif p.status in ["pending", "queued", "processing"]:
            recommendation = "w trakcie..."

        results.append(
            ProductAnalysis(
                ean=p.ean,
                name=p.name,
                purchase_price=p.purchase_price,
                lowest_price_allegro=lowest_price,
                sold_count=sold_count,
                source=source,
                last_checked=last_checked,
                profit_margin=profit_margin,
                recommendation=recommendation,
                notes=p.notes,
                # Dodajmy status, aby frontend mógł go widzieć
                status=p.status 
            )
        )
    return results
