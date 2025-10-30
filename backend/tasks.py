import os
import io
from celery import Celery
from dotenv import load_dotenv
from .database import SessionLocal
from . import models
from sqlalchemy.orm import Session
import pandas as pd
from datetime import datetime, timedelta
import time
import requests
from bs4 import BeautifulSoup

load_dotenv()

CELERY_BROKER = os.getenv('CELERY_BROKER_URL', 'redis://redis:6379/0')
celery = Celery('app.tasks', broker=CELERY_BROKER)

# helper: normalize EAN
def normalize_ean(raw_ean):
    if raw_ean is None:
        return None
    s = str(raw_ean).strip()
    # remove spaces and non-digit
    s = ''.join(ch for ch in s if ch.isdigit())
    # remove leading zeros
    s = s.lstrip('0')
    return s or None

@celery.task(bind=True, acks_late=True, max_retries=3)
def parse_import_file(self, import_job_id: int, filepath: str):
    db: Session = SessionLocal()
    try:
        job = db.query(models.ImportJob).filter(models.ImportJob.id == import_job_id).first()
        if not job:
            return

        # read file with pandas (support csv and xlsx)
        try:
            if filepath.lower().endswith('.csv'):
                df = pd.read_csv(filepath, dtype=str)
            else:
                df = pd.read_excel(filepath, dtype=str)
        except Exception as e:
            db.query(models.ImportJob).filter(models.ImportJob.id == import_job_id).update({
                'status': 'failed',
                'meta': {**(job.meta or {}), 'error': f'read_failed: {str(e)}'}
            })
            db.commit()
            return

        # expected columns: EAN, name, price (case-insensitive)
        cols = {c.lower(): c for c in df.columns}
        ean_col = cols.get('ean') or cols.get('barcode') or next((c for c in df.columns if 'ean' in c.lower()), None)
        name_col = cols.get('name') or cols.get('product') or next((c for c in df.columns if 'name' in c.lower()), None)
        price_col = cols.get('price') or cols.get('purchase_price') or next((c for c in df.columns if 'price' in c.lower()), None)

        if not ean_col or not name_col or not price_col:
            db.query(models.ImportJob).filter(models.ImportJob.id == import_job_id).update({
                'status': 'failed',
                'meta': {**(job.meta or {}), 'error': 'missing required columns (ean/name/price)'}
            })
            db.commit()
            return

        # normalize and clean
        df['__ean_norm'] = df[ean_col].apply(normalize_ean)
        df['__name'] = df[name_col].astype(str).str.strip()
        # parse price to numeric (replace commas)
        def parse_price(v):
            if pd.isna(v):
                return None
            s = str(v).strip().replace(',', '.')
            try:
                return float(s)
            except:
                # remove non numeric
                s2 = ''.join(ch for ch in s if (ch.isdigit() or ch=='.'))
                try:
                    return float(s2) if s2 else None
                except:
                    return None
        df['__price'] = df[price_col].apply(parse_price)

        # deduplicate within file: keep first occurrence
        before = len(df)
        df['__is_dup'] = df['__ean_norm'].duplicated(keep='first')
        dups = df['__is_dup'].sum()
        df_unique = df[~df['__is_dup']].copy()
        inserted = 0

        # insert product_inputs
        for _, row in df_unique.iterrows():
            ean = row.get('__ean_norm')
            name = row.get('__name')
            price = row.get('__price')
            normalized_price = price  # for now conversion handled later if needed

            pi = models.ProductInput(
                import_job_id = import_job_id,
                ean = ean,
                name = name,
                purchase_price = price,
                currency = job.meta.get('currency') if job.meta else None,
                normalized_price = normalized_price,
                status = 'pending',
                notes = None
            )
            db.add(pi)
            inserted += 1

        db.commit()

        # mark duplicates as product_inputs with notes (optional) or keep for manual fix
        # here we just create notes rows for duplicates so user can see
        dup_rows = df[df['__is_dup']]
        for _, row in dup_rows.iterrows():
            ean = row.get('__ean_norm')
            name = row.get('__name')
            price = row.get('__price')
            pi = models.ProductInput(
                import_job_id = import_job_id,
                ean = ean,
                name = name,
                purchase_price = price,
                currency = job.meta.get('currency') if job.meta else None,
                normalized_price = price,
                status = 'pending',
                notes = 'duplicate_in_file'
            )
            db.add(pi)
        db.commit()

        # update job meta with summary
        db.query(models.ImportJob).filter(models.ImportJob.id == import_job_id).update({
            'status': 'imported',
            'meta': {**(job.meta or {}), 'rows_in_file': before, 'rows_inserted': inserted, 'duplicates': int(dups)}
        })
        db.commit()

        # now check cache and enqueue fetch tasks for those needing it
        product_rows = db.query(models.ProductInput).filter(models.ProductInput.import_job_id == import_job_id).all()
        from .tasks import fetch_allegro_data  # local import to avoid circular issues

        for p in product_rows:
            # skip if no ean
            if not p.ean:
                db.query(models.ProductInput).filter(models.ProductInput.id == p.id).update({'status': 'error', 'notes': 'missing_ean'})
                db.commit()
                continue

            cache = db.query(models.AllegroCache).filter(models.AllegroCache.ean == p.ean).first()
            if cache and cache.fetched_at:
                age = datetime.utcnow() - cache.fetched_at
                if age <= timedelta(days=30):
                    # use cache
                    db.query(models.ProductInput).filter(models.ProductInput.id == p.id).update({'status': 'done', 'notes': 'source=cache'})
                    db.commit()
                    continue

            # otherwise enqueue fetch
            db.query(models.ProductInput).filter(models.ProductInput.id == p.id).update({'status': 'queued'})
            db.commit()
            try:
                fetch_allegro_data.delay(p.id, p.ean)
            except Exception:
                # if celery not running locally, keep queued
                pass

    except Exception as exc:
        # mark job failed
        db.query(models.ImportJob).filter(models.ImportJob.id == import_job_id).update({
            'status': 'failed',
            'meta': {**(job.meta or {}), 'error': str(exc)}
        })
        db.commit()
        raise
    finally:
        db.close()

@celery.task(bind=True, acks_late=True, max_retries=3)
def fetch_allegro_data(self, product_input_id: int, ean: str):
    """
    Task: dla prostoty implementujemy:
    1) probujemy pobrac dane z Allegro API - placeholder
    2) jesli brak - probujemy proste scrape (placeholder)
    3) zapis do allegro_cache i update product_input status
    """
    db: Session = SessionLocal()
    try:
        p = db.query(models.ProductInput).filter(models.ProductInput.id == product_input_id).first()
        if not p:
            return

        # na potrzeby MVP najpierw probujemy prosty http request do strony wynikow allegro (placeholder)
        # NOTE: realny scraping wymaga uwaznego dostosowania i obslugi proxy/headers/captcha
        try:
            # example naive search query (moze nie dzialac przy realnym Allegro)
            search_url = f"https://allegro.pl/listing?string={ean}"
            headers = {'User-Agent': 'Mozilla/5.0'}
            resp = requests.get(search_url, headers=headers, timeout=15)
            if resp.status_code == 200 and 'allegro' in resp.text.lower():
                # prosty parsing - do pelnego rozwiazania trzeba zbadac strukture
                soup = BeautifulSoup(resp.text, 'html.parser')
                # placeholder: znajdz element ceny - tutaj mozna wprowadzic konkretne selektory
                price_el = soup.select_one('span[aria-hidden="true"]')  # dummy selector
                lowest_price = None
                try:
                    if price_el:
                        # extract digits
                        txt = price_el.get_text()
                        txt = ''.join(ch for ch in txt if (ch.isdigit() or ch in '.,'))
                        txt = txt.replace(',', '.')
                        lowest_price = float(txt) if txt else None
                except:
                    lowest_price = None

                # saved cache (even partial)
                cache = db.query(models.AllegroCache).filter(models.AllegroCache.ean == ean).first()
                if not cache:
                    cache = models.AllegroCache(ean=ean, lowest_price=lowest_price, sold_count=None, seller_info=None, source='scrape', fetched_at=datetime.utcnow())
                    db.add(cache)
                else:
                    cache.lowest_price = lowest_price
                    cache.source = 'scrape'
                    cache.fetched_at = datetime.utcnow()
                db.commit()

                # update product_input
                db.query(models.ProductInput).filter(models.ProductInput.id == product_input_id).update({
                    'status': 'done',
                    'notes': 'fetched_via_scrape'
                })
                db.commit()
                return

            # if no useful result, mark not found
            db.query(models.ProductInput).filter(models.ProductInput.id == product_input_id).update({
                'status': 'not_found',
                'not_found': True,
                'notes': 'no_results'
            })
            db.commit()
            return

        except Exception as e:
            # on scraping error -> retry with backoff
            try:
                raise e
            finally:
                self.retry(countdown=min(60, (self.request.retries + 1) * 10), exc=e)

    except Exception as exc:
        db.query(models.ProductInput).filter(models.ProductInput.id == product_input_id).update({
            'status': 'error',
            'notes': f'exception:{str(exc)}'
        })
        db.commit()
        raise
    finally:
        db.close()
