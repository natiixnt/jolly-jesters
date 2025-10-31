import os
from celery import Celery
from .database import SessionLocal
from . import models
import pandas as pd
from datetime import datetime, timedelta
from .services.allegro_scraper import fetch_allegro_data as scraper_fetch

CELERY_BROKER = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
celery = Celery("app.tasks", broker=CELERY_BROKER)


@celery.task(bind=True, acks_late=True, max_retries=3)
def parse_import_file(self, import_job_id: int, filepath: str):
    db = SessionLocal()
    try:
        job = db.query(models.ImportJob).filter(models.ImportJob.id == import_job_id).first()
        if not job:
            return

        df = pd.read_excel(filepath) if filepath.lower().endswith(".xlsx") else pd.read_csv(filepath)
        df["ean"] = df["EAN"].astype(str).str.strip()
        df["name"] = df["Name"].astype(str).str.strip()
        df["purchase_price"] = df["Price"].astype(float)

        for _, row in df.iterrows():
            p = models.ProductInput(
                import_job_id=import_job_id,
                ean=row["ean"],
                name=row["name"],
                purchase_price=row["purchase_price"],
                currency=job.meta.get("currency"),
                status="pending",
            )
            db.add(p)
        db.commit()

        # enqueue fetch Allegro
        products = db.query(models.ProductInput).filter(models.ProductInput.import_job_id == import_job_id).all()
        for p in products:
            fetch_allegro_data.delay(p.id, p.ean)

        db.query(models.ImportJob).filter(models.ImportJob.id == import_job_id).update({"status": "processing"})
        db.commit()
    finally:
        db.close()


@celery.task(bind=True, acks_late=True, max_retries=3)
def fetch_allegro_data(self, product_input_id: int, ean: str):
    db = SessionLocal()
    try:
        p = db.query(models.ProductInput).filter(models.ProductInput.id == product_input_id).first()
        if not p:
            return

        result = scraper_fetch(ean)
        cache = db.query(models.AllegroCache).filter(models.AllegroCache.ean == ean).first()
        if not cache:
            cache = models.AllegroCache(
                ean=ean,
                lowest_price=result["lowest_price"],
                sold_count=result["sold_count"],
                source=result["source"],
                fetched_at=result["fetched_at"],
            )
            db.add(cache)
        else:
            cache.lowest_price = result["lowest_price"]
            cache.sold_count = result["sold_count"]
            cache.source = result["source"]
            cache.fetched_at = result["fetched_at"]

        db.query(models.ProductInput).filter(models.ProductInput.id == product_input_id).update(
            {"status": "done", "notes": "fetched"}
        )
        db.commit()
    finally:
        db.close()
