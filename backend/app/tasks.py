# Plik: backend/app/tasks.py

import os
import pandas as pd
from celery import Celery
from datetime import datetime, timedelta
from .database import SessionLocal
from . import models
from .services.allegro_scraper import fetch_allegro_data as scraper_fetch
# Importujemy ustawienia cache z config
from .config import CACHE_TTL_DAYS 

CELERY_BROKER = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
celery = Celery("app.tasks", broker=CELERY_BROKER)


@celery.task(bind=True, acks_late=True, max_retries=3)
def parse_import_file(self, import_job_id: int, filepath: str):
    """
    Krok 1: Parsuje wgrany plik, tworzy ProductInput i kolejkuje zadania fetch_allegro_data
    """
    db = SessionLocal()
    try:
        job = db.query(models.ImportJob).filter(models.ImportJob.id == import_job_id).first()
        if not job:
            return

        try:
            df = pd.read_excel(filepath) if filepath.lower().endswith(".xlsx") else pd.read_csv(filepath, dtype=str)
        except Exception as e:
            job.status = "error"
            job.notes = f"Nie można otworzyć pliku: {e}"
            db.commit()
            return
            
        # --- MODYFIKACJA: Elastyczne mapowanie kolumn ---
        df.columns = [c.lower() for c in df.columns]
        
        ean_col = next((c for c in df.columns if 'ean' in c), None)
        name_col = next((c for c in df.columns if c in ['name', 'nazwa']), None)
        price_col = next((c for c in df.columns if c in ['price', 'cena zakupu']), None)
        
        if not all([ean_col, name_col, price_col]):
             job.status = "error"
             job.notes = "Nie znaleziono wymaganych kolumn (EAN, nazwa/Name, cena zakupu/Price)"
             db.commit()
             return
        
        df["ean_norm"] = df[ean_col].astype(str).str.strip().str.lstrip('0')
        df["name_norm"] = df[name_col].astype(str).str.strip()
        df["price_norm"] = pd.to_numeric(
            df[price_col].astype(str).str.replace(',', '.'), 
            errors='coerce'
        )
        # --- KONIEC MODYFIKACJI ---

        products_to_enqueue = []
        for _, row in df.iterrows():
            # Pomiń wiersze bez EAN lub ceny
            if not row["ean_norm"] or pd.isna(row["price_norm"]):
                continue 

            p = models.ProductInput(
                import_job_id=import_job_id,
                ean=row["ean_norm"],
                name=row["name_norm"],
                purchase_price=row["price_norm"],
                currency=job.meta.get("currency", "PLN"),
                status="pending", # Poprawny status początkowy
            )
            db.add(p)
            products_to_enqueue.append(p)
            
        if not products_to_enqueue:
            job.status = "error"
            job.notes = "Nie znaleziono poprawnych wierszy w pliku."
            db.commit()
            return

        db.commit()

        # Zakolejkuj zadania scrapingu (robimy to po commicie, aby mieć ID produktów)
        for p in products_to_enqueue:
            db.refresh(p) # Pobierz ID z bazy
            # Kolejkujemy zadanie pobrania danych dla każdego produktu
            fetch_allegro_data.delay(p.id, p.ean)

        job.status = "processing"
        db.commit()

    except Exception as e:
        # Obsługa niespodziewanego błędu
        db.rollback()
        job = db.query(models.ImportJob).filter(models.ImportJob.id == import_job_id).first()
        job.status = "error"
        job.notes = f"Błąd krytyczny parsera: {e}"
        db.commit()
    finally:
        db.close()


@celery.task(bind=True, acks_late=True, max_retries=3)
def fetch_allegro_data(self, product_input_id: int, ean: str):
    """
    Krok 2: Pobiera dane z Allegro dla pojedynczego ProductInput ID, z logiką cache.
    """
    db = SessionLocal()
    try:
        p = db.query(models.ProductInput).filter(models.ProductInput.id == product_input_id).first()
        if not p:
            return # Produkt został usunięty?

        # --- POCZĄTEK MODYFIKACJI (LOGIKA CACHE) ---
        
        # 1. Sprawdź cache
        cache = db.query(models.AllegroCache).filter(models.AllegroCache.ean == ean).first()
        ttl_limit = datetime.utcnow() - timedelta(days=CACHE_TTL_DAYS)

        if cache and cache.fetched_at > ttl_limit:
            # Mamy świeże dane w cache, użyj ich
            if cache.not_found:
                p.status = "not_found"
                p.notes = f"Cached not_found @ {cache.fetched_at.date()}"
            else:
                p.status = "done"
                p.notes = f"Cached data @ {cache.fetched_at.date()}"
            
            db.commit()
            return # Zakończ, nie scrapuj
        
        # 2. Brak w cache lub stare dane -> uruchom scraper
        # result = (lowest_price, sold_count, source, fetched_at, not_found)
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
        # --- KONIEC MODYFIKACJI ---

    except Exception as e:
        db.rollback()
        p = db.query(models.ProductInput).filter(models.ProductInput.id == product_input_id).first()
        if p:
            p.status = "error"
            p.notes = f"Błąd krytyczny workera: {e}"
            db.commit()
    finally:
        db.close()