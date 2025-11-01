# Plik: backend/app/tasks.py

import os
import pandas as pd
from celery import Celery
from datetime import datetime, timedelta
import re 

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

# --- Funkcje pomocnicze do analizy (bez zmian) ---

def find_header_row(df_head):
    """Próbuje znaleźć indeks wiersza, który wygląda jak nagłówek"""
    ean_keys = ['ean', 'barcode', 'kod']
    name_keys = ['name', 'nazwa', 'title', 'tytuł']
    price_keys = ['price', 'cena', 'cost', 'koszt', 'netto']
    
    for i, row in df_head.iterrows():
        cols_str = " ".join([str(c).lower() for c in row if pd.notna(c)])
        has_ean = any(key in cols_str for key in ean_keys)
        has_name = any(key in cols_str for key in name_keys)
        has_price = any(key in cols_str for key in price_keys)
        
        if (has_ean and has_price) or (has_name and has_price) or (has_ean and has_name):
            return i 
            
    return 0 

def is_ean(val):
    """Sprawdza, czy wartość wygląda jak EAN (8, 12, 13 cyfr)"""
    s = str(val).strip()
    return s.isdigit() and len(s) in [8, 12, 13]

def is_price(val):
    """Sprawdza, czy wartość wygląda jak cena (pozwala na przecinki i kropki)"""
    if val is None: return False
    s = str(val).strip().replace(',', '.')
    if re.fullmatch(r"^-?\d+(\.\d+)?$", s):
        try:
            float(s)
            return True
        except ValueError:
            return False
    return False

def find_columns_by_content(df, start_row):
    """Analizuje zawartość DF (bez nagłówków) i zgaduje, które kolumny są które"""
    ean_col, name_col, price_col = None, None, None
    potential_cols = {} 
    
    sample_df = df.iloc[start_row : start_row + 50]
    
    for _, row in sample_df.iterrows():
        for col_idx, cell in enumerate(row.head(10)):
            if col_idx not in potential_cols:
                potential_cols[col_idx] = {'ean_count': 0, 'price_count': 0, 'text_count': 0}
            
            if pd.isna(cell) or str(cell).strip() == "":
                continue
                
            if is_ean(cell):
                potential_cols[col_idx]['ean_count'] += 1
            if is_price(cell):
                potential_cols[col_idx]['price_count'] += 1
            if (isinstance(cell, str) and 
                not is_ean(cell) and 
                not is_price(cell) and 
                len(cell) > 3):
                potential_cols[col_idx]['text_count'] += 1

    best_ean = sorted(potential_cols.items(), key=lambda item: item[1]['ean_count'], reverse=True)
    if best_ean and best_ean[0][1]['ean_count'] > 5:
        ean_col = best_ean[0][0]

    best_price = sorted(potential_cols.items(), key=lambda item: item[1]['price_count'], reverse=True)
    for idx, counts in best_price:
        if idx != ean_col and counts['price_count'] > 5:
            price_col = idx
            break
            
    best_name = sorted(potential_cols.items(), key=lambda item: item[1]['text_count'], reverse=True)
    for idx, counts in best_name:
        if idx != ean_col and idx != price_col and counts['text_count'] > 5:
            name_col = idx
            break
            
    return ean_col, name_col, price_col

# --- Koniec funkcji pomocniczych ---


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
            df = pd.read_excel(filepath, header=None, dtype=str) if filepath.lower().endswith(".xlsx") else pd.read_csv(filepath, header=None, dtype=str)
        except Exception as e:
            raise ValueError(f"Nie można otworzyć pliku: {e}")
            
        header_row_index = find_header_row(df.head(15)) 
        data_start_row = header_row_index + 1

        try:
            df = pd.read_excel(filepath, header=header_row_index, dtype=str) if filepath.lower().endswith(".xlsx") else pd.read_csv(filepath, header=header_row_index, dtype=str)
        except Exception:
            df = pd.read_excel(filepath, header=None, dtype=str) if filepath.lower().endswith(".xlsx") else pd.read_csv(filepath, header=None, dtype=str)
            data_start_row = 0 

        df.columns = [str(c).lower() for c in df.columns]

        ean_keys = ['ean', 'barcode', 'kod', 'symbol']
        name_keys = ['name', 'nazwa', 'title', 'tytuł', 'opis', 'description']
        price_keys = ['price', 'cena', 'cost', 'koszt', 'wartość', 'netto']

        ean_col = next((c for c in df.columns if any(key in str(c) for key in ean_keys)), None)
        name_col = next((c for c in df.columns if any(key in str(c) for key in name_keys)), None)
        price_col = next((c for c in df.columns if any(key in str(c) for key in price_keys)), None)

        if not all([ean_col, name_col, price_col]):
            df_no_header = pd.read_excel(filepath, header=None, dtype=str) if filepath.lower().endswith(".xlsx") else pd.read_csv(filepath, header=None, dtype=str)
            
            c_ean, c_name, c_price = find_columns_by_content(df_no_header, data_start_row)
            
            df = df_no_header 
            ean_col = c_ean
            name_col = c_name
            price_col = c_price
            
            df = df.iloc[data_start_row:].copy() 
        
        missing_cols = []
        if ean_col is None: missing_cols.append("EAN/Barcode/Kod")
        if name_col is None: missing_cols.append("Nazwa/Name/Title")
        if price_col is None: missing_cols.append("Cena/Price/Koszt")
        
        if missing_cols:
             raise ValueError(f"Nie znaleziono wymaganych kolumn zawierających słowa: {', '.join(missing_cols)}")
        
        # --- POPRAWKA (KROK 32): Usunięcie błędnego bloku 'if not df.is_copy:' ---
        # Wcześniejszy .copy() w bloku 'if not all' lub praca na oryginalnym DF jest wystarczająca.
        # Użycie .loc zapobiegnie ostrzeżeniom.
        
        df.loc[:, "ean_norm"] = df[ean_col].astype(str).str.strip().str.lstrip('0')
        df.loc[:, "name_norm"] = df[name_col].astype(str).str.strip()
        df.loc[:, "price_norm"] = pd.to_numeric(
            df[price_col].astype(str).str.replace(',', '.').str.replace(r'[^\d\.]', '', regex=True), 
            errors='coerce'
        )

        products_to_enqueue = []
        for _, row in df.iterrows():
            if not row["ean_norm"] or pd.isna(row["price_norm"]) or row["price_norm"] <= 0:
                continue 

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
            raise ValueError("Nie znaleziono poprawnych wierszy w pliku (sprawdź, czy EAN i Ceny są wypełnione).")

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

        cache = db.query(models.AllegroCache).filter(models.AllegroCache.ean == ean).first()
        ttl_limit = datetime.utcnow() - timedelta(days=CACHE_TTL_DAYS)

        if cache and cache.fetched_at > ttl_limit:
            if cache.source != 'failed' and cache.source != 'error':
                if cache.not_found:
                    p.status = "not_found"
                    p.notes = f"Cached not_found @ {cache.fetched_at.date()}"
                else:
                    p.status = "done"
                    p.notes = f"Cached data @ {cache.fetched_at.date()}"
                db.commit()
                db.close()
                return 
            
        result = scraper_fetch(ean) 
        
        new_status = "done"
        notes = "Fetched via " + result["source"]
        
        if result["source"] == "failed":
            new_status = "error"
            notes = "Scraping failed"
        elif result.get("not_found", False):
            new_status = "not_found"
            notes = "Product not found"
            
        if not cache:
            cache = models.AllegroCache(ean=ean)
            db.add(cache)
        
        cache.lowest_price = result["lowest_price"]
        cache.sold_count = result["sold_count"]
        cache.source = result["source"]
        cache.fetched_at = result["fetched_at"]
        cache.not_found = result.get("not_found", False)

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