from sqlalchemy.orm import Session
from . import models, schemas
from datetime import datetime

# -----------------------
# ImportJob CRUD
# -----------------------
def create_import_job(db: Session, filename: str, user_id: int = None):
    job = models.ImportJob(filename=filename, user_id=user_id)
    db.add(job)
    db.commit()
    db.refresh(job)
    return job

def get_import_job(db: Session, job_id: int):
    return db.query(models.ImportJob).filter(models.ImportJob.id == job_id).first()

def list_import_jobs(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.ImportJob).offset(skip).limit(limit).all()

def update_import_job_status(db: Session, job_id: int, status: str):
    job = db.query(models.ImportJob).filter(models.ImportJob.id == job_id).first()
    if job:
        job.status = status
        db.commit()
        db.refresh(job)
    return job

# -----------------------
# ProductInput CRUD
# -----------------------
def create_product_input(db: Session, product: schemas.ProductInputCreate):
    db_product = models.ProductInput(
        import_job_id=product.import_job_id,
        ean=product.ean,
        name=product.name,
        purchase_price=product.purchase_price,
        currency=product.currency,
        normalized_price=product.purchase_price  # domyślnie
    )
    db.add(db_product)
    db.commit()
    db.refresh(db_product)
    return db_product

def get_product_input(db: Session, product_id: int):
    return db.query(models.ProductInput).filter(models.ProductInput.id == product_id).first()

def list_product_inputs_by_job(db: Session, job_id: int):
    return db.query(models.ProductInput).filter(models.ProductInput.import_job_id == job_id).all()

def update_product_input_status(db: Session, product_id: int, status: str):
    product = db.query(models.ProductInput).filter(models.ProductInput.id == product_id).first()
    if product:
        product.status = status
        db.commit()
        db.refresh(product)
    return product

# -----------------------
# AllegroCache CRUD
# -----------------------
def get_allegro_cache(db: Session, ean: str):
    return db.query(models.AllegroCache).filter(models.AllegroCache.ean == ean).first()

def create_or_update_allegro_cache(db: Session, ean: str, lowest_price: float, sold_count: int, source: str, not_found: bool = False):
    cache = get_allegro_cache(db, ean)
    if cache:
        cache.lowest_price = lowest_price
        cache.sold_count = sold_count
        cache.source = source
        cache.fetched_at = datetime.utcnow()
        cache.not_found = not_found
    else:
        cache = models.AllegroCache(
            ean=ean,
            lowest_price=lowest_price,
            sold_count=sold_count,
            source=source,
            not_found=not_found
        )
        db.add(cache)
    db.commit()
    db.refresh(cache)
    return cache

# -----------------------
# Export CRUD
# -----------------------
def create_export(db: Session, import_job_id: int, filepath: str):
    export = models.Export(import_job_id=import_job_id, filepath=filepath)
    db.add(export)
    db.commit()
    db.refresh(export)
    return export

def list_exports_by_job(db: Session, job_id: int):
    return db.query(models.Export).filter(models.Export.import_job_id == job_id).all()
