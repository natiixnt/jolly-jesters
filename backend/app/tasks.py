# Plik: backend/app/tasks.py

import os
import pandas as pd
from celery import Celery
from datetime import datetime, timedelta
import re
import subprocess  # <-- antibot
import ast         # <-- antibot
import logging     # <-- antibot / logging
from datetime import timezone # <-- antibot
import time        # <-- race condition fix

from kombu import Queue
from sqlalchemy import func
from sqlalchemy.orm import Session
# ZMIANA: Importujemy sygnal bezposrednio
from celery.signals import worker_process_init
# ZMIANA: Importujemy tylko to, co potrzebne, zeby uniknac bledu z forkiem
from .database import SessionLocal, engine 

from . import models
# usunieto: from .services.allegro_scraper import fetch_allegro_data as scraper_fetch
from .services.alerts import send_scraper_alert
from .config import CACHE_TTL_DAYS

CELERY_BROKER = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
celery = Celery("app.tasks", broker=CELERY_BROKER)

# celery config
celery.conf.task_default_queue = "celery"
celery.conf.task_queues = (
    Queue("celery", routing_key="celery"),
    Queue("scraper", routing_key="scraper"),
)
celery.conf.task_routes = {
    "app.tasks.fetch_allegro_data": {
        "queue": "scraper",
        "routing_key": "scraper",
    }
}
celery.conf.worker_prefetch_multiplier = 1

# --- START POPRAWKI: Unikanie bledu Celery/SQLAlchemy fork ---
# https://docs.celeryq.dev/en/stable/userguide/tasks.html#database-sessions
# zamykamy polaczenia engine po forku  zeby workery mialy swieze
# ZMIANA: Uzywamy importowanego sygnalu, a nie obiektu 'celery'
@worker_process_init.connect
def init_db_connection(**kwargs):
    logger.info("Initializing DB connection for worker process...")
    engine.dispose(close=True)

def with_db_session(func):
    """Decorator to provide a db session to a task."""
    def wrapper(*args, **kwargs):
        db = SessionLocal()
        try:
            result = func(db, *args, **kwargs)
            db.commit() # commit na koniec  jesli wszystko ok
            return result
        except Exception as e:
            db.rollback()
            logger.error(f"Task failed, rolling back session. Error: {e}")
            # ponowne wywolanie bledu  zeby celery zarejestrowal jako 'failure'
            raise e
        finally:
            db.close()
    return wrapper
# --- KONIEC POPRAWKI ---


def update_job_error(db: Session, job: models.ImportJob, message: str):
    """Pomocnik do aktualizowania statusu błędu w bazie"""
    job.status = "error"
    job.notes = message
    # usunieto db.commit() - decorator 'with_db_session' sie tym zajmie

ACTIVE_STATUSES = {"pending", "queued", "processing"}

# ZMIANA: Uzywamy decoratora do zarzadzania sesja
@with_db_session
def finalize_job_if_complete(db: Session, import_job_id: int) -> None:
    """Aktualizuje status zadania, jeśli wszystkie produkty zakończyły przetwarzanie."""

    remaining = (
        db.query(models.ProductInput)
        .filter(
            models.ProductInput.import_job_id == import_job_id,
            models.ProductInput.status.in_(ACTIVE_STATUSES),
        )
        .count()
    )

    if remaining:
        return

    job = (
        db.query(models.ImportJob)
        .filter(models.ImportJob.id == import_job_id)
        .first()
    )
    if not job:
        return

    total_products = (
        db.query(func.count(models.ProductInput.id))
        .filter(models.ProductInput.import_job_id == import_job_id)
        .scalar()
        or 0
    )
    error_count = (
        db.query(func.count(models.ProductInput.id))
        .filter(
            models.ProductInput.import_job_id == import_job_id,
            models.ProductInput.status == "error",
        )
        .scalar()
        or 0
    )
    not_found_count = (
        db.query(func.count(models.ProductInput.id))
        .filter(
            models.ProductInput.import_job_id == import_job_id,
            models.ProductInput.status == "not_found",
        )
        .scalar()
        or 0
    )

    if total_products == 0:
        job.status = "error"
        job.notes = "Brak produktów po przetwarzaniu."
    elif error_count and error_count == total_products:
        job.status = "error"
        job.notes = "Wszystkie produkty zakończyły się błędem."
    else:
        job.status = "done"
        if error_count:
            job.notes = f"Zakończono z błędami: {error_count}/{total_products}."
        elif not_found_count and not_found_count == total_products:
            job.notes = "Żaden produkt nie został znaleziony na Allegro."
        elif not_found_count:
            job.notes = f"Zakończono. Nie znaleziono: {not_found_count}/{total_products}."
        else:
            job.notes = None
    # usunieto db.commit()

# --- Funkcje pomocnicze do analizy (bez zmian) ---

def find_header_row(df_head):
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
    s = str(val).strip()
    return s.isdigit() and len(s) in [8, 12, 13]

def is_price(val):
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


# --- POCZATEK: Funkcja pomocnicza Antibot ---
logger = logging.getLogger(__name__) # get logger

def fetch_with_antibot(ean: str) -> dict:
    """
    calls antibot.exe for a single EAN and parses the result
    """
    logger.info(f"[Antibot] running for EAN: {ean}")
    
    try:
        # assume antibot.exe is in /usr/local/bin/
        process = subprocess.Popen(
            ["/usr/local/bin/antibot", ean], # POPRAWKA: sciezka do pliku
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8' # explicit encoding
        )

        product_detail = None
        raw_logs = []

        for line in process.stdout:
            line = line.strip()
            if not line:
                continue
            
            raw_logs.append(line)
            logger.debug(f"[Antibot] LOG: {line}")
            
            if 'Product detail: ' in line:
                product_detail_str = line.split('Product detail: ')[1].strip()
                try:
                    product_detail = ast.literal_eval(product_detail_str)
                    logger.info(f"[Antibot] parse success for EAN: {ean}")
                    break # found data  no need to read more
                except Exception as e:
                    logger.error(f"[Antibot] ast.literal_eval error for EAN {ean}: {e} | Line: {product_detail_str}")
            
        process.wait()

        if product_detail:
            # make sure output data contains all expected fields
            product_detail.setdefault('not_found', False)
            product_detail.setdefault('source', 'antibot_exe')
            product_detail.setdefault('last_checked_at', datetime.now(timezone.utc).isoformat())
            product_detail.setdefault('allegro_lowest_price', None)
            product_detail.setdefault('sold_count', None)
            return product_detail

        # if we got here  process finished but did not return 'Product detail: '
        logger.warning(f"[Antibot] 'Product detail:' not found for EAN {ean}. Return code: {process.returncode}")
        logger.warning(f"[Antibot] Full logs for EAN {ean}: {' '.join(raw_logs)}")

        return {
            "ean": ean,
            "allegro_lowest_price": None,
            "sold_count": None,
            "last_checked_at": datetime.now(timezone.utc).isoformat(),
            "source": "antibot_exe",
            "not_found": True, # treat no data as "not_found"
            "error": "No 'Product detail:' line in output"
        }

    except FileNotFoundError:
        logger.critical(f"[Antibot] File /usr/local/bin/antibot not found!") # POPRAWKA: sciezka w logu
        return {"ean": ean, "source": "antibot_error", "not_found": True, "error": "antibot.exe not found", "last_checked_at": datetime.now(timezone.utc).isoformat()}
    except Exception as e:
        logger.error(f"[Antibot] Critical subprocess error for EAN {ean}: {e}")
        return {"ean": ean, "source": "antibot_error", "not_found": True, "error": str(e), "last_checked_at": datetime.now(timezone.utc).isoformat()}
# --- KONIEC: Funkcja pomocnicza Antibot ---


@celery.task(bind=True, acks_late=True, max_retries=3)
def parse_import_file(self, import_job_id: int, filepath: str):
    """
    Krok 1: Parsuje wgrany plik, tworzy ProductInput i kolejkuje zadania fetch_allegro_data
    """
    # ZMIANA: Uzywamy 'with' dla sesji, zeby miec pewnosc zamkniecia
    db = SessionLocal()
    try:
        job = db.query(models.ImportJob).filter(models.ImportJob.id == import_job_id).first()
        if not job:
            return # job nie istnieje

        job.status = "processing"
        job.notes = None
        db.commit()

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

        # usunieto: bledny blok 'if not df.is_copy:'
        
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

        db.flush()

        task_payloads: list[tuple[int, str]] = []
        for p in products_to_enqueue:
            p.status = "queued"
            task_payloads.append((p.id, p.ean))

        db.commit()

        try:
            for product_id, product_ean in task_payloads:
                fetch_allegro_data.delay(product_id, product_ean)
        except Exception as exc:
            # ZMIANA: Jesli kolejkowanie sie nie uda, uzyj biezacej sesji 'db'
            job.status = "error"
            job.notes = f"Nie udało się zlecić zadań scrapera: {exc}"
            db.commit()
            raise

    except (ValueError, Exception) as e:
        db.rollback()
        # ZMIANA: Uzyj biezacej sesji 'db' do zapisu bledu
        job_to_update = db.query(models.ImportJob).filter(models.ImportJob.id == import_job_id).first()
        if job_to_update:
            update_job_error(db, job_to_update, str(e))
            db.commit() # commit po update_job_error
        
        raise e 
    finally:
        db.close() # Zawsze zamykaj sesje na koniec


@celery.task(bind=True, acks_late=True, max_retries=3)
def fetch_allegro_data(self, product_input_id: int, ean: str):
    """
    Krok 2: Pobiera dane z Allegro (z logiką cache)
    """
    # ZMIANA: Uzywamy 'with' dla sesji
    with SessionLocal() as db:
        try:
            # --- START FIX: Race condition read-after-write ---
            p = None
            for _ in range(3): # try 3 times
                p = db.query(models.ProductInput).filter(models.ProductInput.id == product_input_id).first()
                if p:
                    break # found it
                logger.warning(f"[fetch_allegro_data] ProductInput.id {product_input_id} not found (race condition?), retrying in 0.5s...")
                time.sleep(0.5)

            if not p:
                logger.error(f"[fetch_allegro_data] CRITICAL: ProductInput.id {product_input_id} not found after 3 retries. Task stopping.")
                return # return success (None) to not block the queue
            # --- END FIX ---

            import_job_id = p.import_job_id

            cache = db.query(models.AllegroCache).filter(models.AllegroCache.ean == ean).first()
            ttl_limit = datetime.utcnow() - timedelta(days=CACHE_TTL_DAYS)

            if cache and cache.fetched_at > ttl_limit:
                
                # --- POPRAWKA LOGIKI CACHE ---
                # nie uzywaj cache'u jesli poprzedni wynik byl bledem
                INVALID_CACHE_SOURCES = ['failed', 'error', 'ban_detected', 'captcha_detected', 'antibot_error']
                if cache.source not in INVALID_CACHE_SOURCES:
                # --- KONIEC POPRAWKI LOGIKI CACHE ---
                
                    if cache.not_found:
                        p.status = "not_found"
                        p.notes = f"Cached not_found @ {cache.fetched_at.date()}"
                    else:
                        p.status = "done"
                        p.notes = f"Cached data @ {cache.fetched_at.date()}"
                    db.commit()
                    # musimy wywolac finalize rowniez dla cache'u
                    finalize_job_if_complete(import_job_id) # ZMIANA: Wywolujemy bez sesji
                    return 
                
            p.status = "processing"
            db.commit()

            # --- POCZATEK INTEGRACJI ANTIBOT.EXE ---
        
            # 1. Wywolaj nowa funkcje zamiast scraper_fetch
            raw_result = fetch_with_antibot(ean)
            
            # 2. Zmapuj wynik z antibot.exe na format 'result'  ktorego oczekuje reszta zadania
            try:
                # proba parsowania daty z isoformatu
                fetched_at_dt = datetime.fromisoformat(raw_result["last_checked_at"])
            except (ValueError, TypeError, KeyError):
                fetched_at_dt = datetime.now(timezone.utc) # fallback

            result = {
                "lowest_price": raw_result.get("allegro_lowest_price"),
                "sold_count": raw_result.get("sold_count"),
                "source": raw_result.get("source", "antibot_exe"),
                "fetched_at": fetched_at_dt,
                "not_found": raw_result.get("not_found", False),
                "error": raw_result.get("error"),
                "alert_sent": False # antibot.exe has no alert system
            }
            
            # 3. Logika statusu (taka sama jak wczesniej  ale na podstawie nowych danych)
            new_status = "done"
            notes = "Fetched via " + result["source"]

            if result.get("error"):
                new_status = "error"
                notes = f"antibot.exe error: {result['error']}"
            elif result.get("not_found", False):
                new_status = "not_found"
                notes = "Product not found (antibot.exe)"
            
            # --- KONIEC INTEGRACJI ANTIBOT.EXE ---

            # ten blok jest na wypadek gdybysmy kiedys wrocili do scrapera
            # ktory SAM wysyla alerty (na razie jest wylaczony)
            if (
                new_status == "error"
                and result["source"] == "failed" 
                and result.get("error")
                and not result.get("alert_sent")
            ):
                send_scraper_alert(
                    "allegro_scrape_failed",
                    {"ean": ean, "error": result["error"]},
                )

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
            finalize_job_if_complete(import_job_id) # ZMIANA: Wywolujemy bez sesji

        except Exception as e:
            db.rollback()
            # proba zapisu bledu przy produkcie
            # ZMIANA: musimy uzyc nowej sesji zeby odczytac po rollbacku
            with SessionLocal() as error_db:
                p_error = error_db.query(models.ProductInput).filter(models.ProductInput.id == product_input_id).first()
                if p_error:
                    p_error.status = "error"
                    p_error.notes = f"Błąd krytyczny workera: {e}"
                    import_job_id = p_error.import_job_id
                    error_db.commit()
                    # Wywolanie finalize musi byc POZA sesja error_db
                    finalize_job_if_complete(import_job_id) # ZMIANA: Wywolujemy bez sesji
            
            # ponowne wywolanie bledu zeby celery sprobowal ponowic
            raise e
            
        # ZMIANA: 'finally' jest juz niepotrzebne, bo 'with SessionLocal()' samo zamyka sesje