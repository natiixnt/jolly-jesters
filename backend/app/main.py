import os
import uuid
import shutil
from fastapi import FastAPI, UploadFile, File, Form, Depends, HTTPException
from .database import Base, engine, get_db
from sqlalchemy.orm import Session
from . import models, tasks
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
    with open(filepath, "wb") as f:
        shutil.copyfileobj(file.file, f)

    # tworzymy job z meta jako JSON
    job = models.ImportJob(
    filename=str(filepath.name),
    status="pending",
    category=category,  # <- dodane
    meta={"currency": currency},  # meta nie trzyma juz category
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    try:
        tasks.parse_import_file.delay(job.id, str(filepath))
        db.query(models.ImportJob).filter(models.ImportJob.id == job.id).update(
            {"status": "processing"}
        )
        db.commit()
    except Exception:
        pass

    return {"job_id": job.id, "message": "plik zapisany i zadanie parse w kolejce"}


@app.get("/api/imports/{job_id}/status", response_model=JobStatus)
def job_status(job_id: int, db: Session = Depends(get_db)):
    job = db.query(models.ImportJob).filter(models.ImportJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    return {"id": job.id, "status": job.status, "meta": job.meta}


@app.get("/api/imports/{job_id}/products", response_model=list[ProductAnalysis])
def list_products(job_id: int, db: Session = Depends(get_db)):
    products = (
        db.query(models.ProductInput)
        .filter(models.ProductInput.import_job_id == job_id)
        .all()
    )
    results = []
    for p in products:
        # join z allegro_cache
        cache = (
            db.query(models.AllegroCache)
            .filter(models.AllegroCache.ean == p.ean)
            .first()
        )
        lowest_price = cache.lowest_price if cache else None
        sold_count = cache.sold_count if cache else None
        source = cache.source if cache else None
        last_checked = cache.fetched_at if cache else None
        profit_margin = None
        recommendation = None
        if lowest_price and p.purchase_price:
            profit_margin = round(lowest_price / p.purchase_price, 2)
            recommendation = (
                "opłacalny"
                if profit_margin >= float(os.getenv("MULTIPLIER", 1.5)) else "nieopłacalny"
            )
        elif p.status in ["pending", "queued", "processing"]:
            recommendation = "brak danych"

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
            )
        )
    return results
