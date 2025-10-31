# Plik: backend/app/tasks.py

import os
import pandas as pd
from celery import Celery
from datetime import datetime, timedelta

from sqlalchemy.orm import Session 
from .database import SessionLocal 

from . import models
from .services.allegro_scraper import fetch_allegro_data as scraper_fetch
from .config import CACHE_TTL_DAYS 

CELERY_BROKER = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
celery = Celery("app.tasks", broker=CELERY_BROKER)


def update_job_error(db: Session, job: models.ImportJob, message: str):
    """Pomocnik do aktualizowania statusu błędu w bazie"""
    job.status = "error"
    job.notes = message
    db.commit()


@celery.task(bind=True, acks_late=True, max_retries=3)
def parse_import_file(self, import_job_id: int, filepath: str):
    """
    Krok 1: Parsuje wgrany plik, tworzy ProductInput i kolejkuje zadania fetch_allegro_data
    """
    db = SessionLocal() 
    job = db.query(models.ImportJob).filter(models.ImportJob.id == import_job_id).first()
    if not job:
        db.close()
        return

    try:
        try:
            df = pd.read_excel(filepath) if filepath.lower().endswith(".xlsx") else pd.read_csv(filepath, dtype=str)
        except Exception as e:
            raise ValueError(f"Nie można otworzyć pliku: {e}")
            
        # Normalizuj nazwy kolumn (małe litery)
        df.columns = [str(c).lower() for c in df.columns]
        
        # --- POPRAWKA: Bardziej elastyczne wyszukiwanie kolumn ---
        ean_col = next((c for c in df.columns if 'ean' in c), None)
        name_col = next((c for c in df.columns if 'name' in c or 'nazwa' in c), None)
        price_col = next((c for c in df.columns if 'price' in c or 'cena' in c), None)
        # --- KONIEC POPRAWKI ---
        
        missing_cols = []
        if not ean_col: missing_cols.append("EAN")
        if not name_col: missing_cols.append("Nazwa/Name")
        if not price_col: missing_cols.append("Cena/Price")
        
        if missing_cols:
             raise ValueError(f"Nie znaleziono wymaganych kolumn zawierających słowa: {', '.join(missing_cols)}")
        
        df["ean_norm"] = df[ean_col].astype(str).str.strip().str.lstrip('0')
        df["name_norm"] = df[name_col].astype(str).str.strip()
        df["price_norm"] = pd.to_numeric(
            df[price_col].astype(str).str.replace(',', '.'), 
            errors='coerce'
        )

        products_to_enqueue = []
        for _, row in df.iterrows():
            if not row["ean_norm"] or pd.isna(row["price_norm"]) or row["price_norm"] <= 0:
                continue # Pomiń wiersze bez EAN lub z niepoprawną ceną

            p = models.ProductInput(
                import_job_id=import_job_id,
                ean=row["ean_norm"],
                name=row["name_norm"],
                purchase_price=row["price_norm"],
                currency=job.meta.get("currency", "PLN"),
                status="pending",
            )
            db.add(p)
            products_to_enqueue.append(p)
            
        if not products_to_enqueue:
            raise ValueError("Nie znaleziono poprawnych wierszy w pliku (sprawdź EAN i Ceny).")

        job.status = "processing" 
        db.commit() 

        for p in products_to_enqueue:
            db.refresh(p) 
            fetch_allegro_data.delay(p.id, p.ean)

    except (ValueError, Exception) as e:
        db.rollback()
        db_error_session = SessionLocal()
        job_to_update = db_error_session.query(models.ImportJob).filter(models.ImportJob.id == import_job_id).first()
        if job_to_update:
            update_job_error(db_error_session, job_to_update, str(e))
        db_error_session.close()
        
        raise e 
    finally:
        db.close()


@celery.task(bind=True, acks_late=True, max_retries=3)
def fetch_allegro_data(self, product_input_id: int, ean: str):
    """
    Krok 2: Pobiera dane z Allegro (z logiką cache)
    """
    db = SessionLocal()
    try:
        p = db.query(models.ProductInput).filter(models.ProductInput.id == product_input_id).first()
        if not p:
            db.close()
            return 

        # 1. Sprawdź cache
        cache = db.query(models.AllegroCache).filter(models.AllegroCache.ean == ean).first()
        ttl_limit = datetime.utcnow() - timedelta(days=CACHE_TTL_DAYS)

        if cache and cache.fetched_at > ttl_limit:
            if cache.not_found:
                p.status = "not_found"
                p.notes = f"Cached not_found @ {cache.fetched_at.date()}"
            else:
                p.status = "done"
                p.notes = f"Cached data @ {cache.fetched_at.date()}"
            db.commit()
            return 
        
        # 2. Brak w cache -> uruchom scraper
        result = scraper_fetch(ean) 
        
        new_status = "done"
        notes = "Fetched via " + result["source"]
        
        if result["source"] == "failed":
            new_status = "error"
            notes = "Scraping failed"
        elif result.get("not_found", False):
            new_status = "not_found"
            notes = "Product not found"
            
        # 3. Zapisz/zaktualizuj cache
        if not cache:
            cache = models.AllegroCache(ean=ean)
            db.add(cache)
        
        cache.lowest_price = result["lowest_price"]
        cache.sold_count = result["sold_count"]
        cache.source = result["source"]
        cache.fetched_at = result["fetched_at"]
        cache.not_found = result.get("not_found", False)

        # 4. Zaktualizuj ProductInput
        p.status = new_status
        p.notes = notes
        
        db.commit()

    except Exception as e:
        db.rollback()
        p = db.query(models.ProductInput).filter(models.ProductInput.id == product_input_id).first()
        if p:
            p.status = "error"
            p.notes = f"Błąd krytyczny workera: {e}"
            db.commit()
    finally:
        db.close()