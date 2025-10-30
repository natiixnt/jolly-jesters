import os
import uuid
import shutil
from fastapi import FastAPI, UploadFile, File, Form, Depends, HTTPException
from .database import Base, engine, get_db
from sqlalchemy.orm import Session
from . import models
from .schemas import ImportStartResponse, JobStatus
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

# create db tables if not existing (dev convenience)
Base.metadata.create_all(bind=engine)

app = FastAPI(title='Import Service')

WORKSPACE = os.getenv('WORKSPACE', '/workspace')
UPLOAD_DIR = Path(WORKSPACE) / 'data' / 'uploads'
EXPORT_DIR = Path(WORKSPACE) / 'data' / 'exports'
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

@app.post('/api/imports/start', response_model=ImportStartResponse)
async def start_import(
    file: UploadFile = File(...),
    category: str = Form(...),
    currency: str = Form(...),
    db: Session = Depends(get_db)
):
    # save file
    filename = f"{uuid.uuid4().hex}_{file.filename}"
    filepath = UPLOAD_DIR / filename
    with open(filepath, 'wb') as f:
        shutil.copyfileobj(file.file, f)

    job = models.ImportJob(filename=str(filepath.name), status='pending', meta={'category': category, 'currency': currency})
    db.add(job)
    db.commit()
    db.refresh(job)

    # enqueue parse task - do it async with celery to avoid blocking
    try:
        # import here to avoid circular imports when celery not available
        from .tasks import parse_import_file
        parse_import_file.delay(job.id, str(filepath))
        db.query(models.ImportJob).filter(models.ImportJob.id == job.id).update({'status': 'processing'})
        db.commit()
    except Exception as e:
        # if celery not configured, keep job as pending and return
        pass

    return {'job_id': job.id, 'message': 'plik zapisany i zadanie parse w kolejce'}

@app.get('/api/imports/{job_id}/status', response_model=JobStatus)
def job_status(job_id: int, db: Session = Depends(get_db)):
    job = db.query(models.ImportJob).filter(models.ImportJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail='job not found')
    return {'id': job.id, 'status': job.status, 'meta': job.meta}

# simple endpoint to list first N product_inputs for a job (dev)
@app.get('/api/imports/{job_id}/products')
def list_products(job_id: int, limit: int = 100, db: Session = Depends(get_db)):
    items = db.query(models.ProductInput).filter(models.ProductInput.import_job_id == job_id).limit(limit).all()
    return [ {
        'id': i.id, 'ean': i.ean, 'name': i.name, 'purchase_price': float(i.purchase_price) if i.purchase_price else None,
        'status': i.status, 'not_found': i.not_found, 'notes': i.notes
    } for i in items ]
