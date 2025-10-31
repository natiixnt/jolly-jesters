# Plik: backend/app/services/allegro_scraper.py

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import random
import time
import re

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.60 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4884.74 Safari/537.36",
]

def _parse_price(price_text: str) -> float | None:
    """Helper do parsowania ceny (usuwa 'zł', ' ', ',')"""
    try:
        txt = "".join(ch for ch in price_text if ch.isdigit() or ch in ".,")
        txt = txt.replace(",", ".").replace(" ", "")
        return float(txt) if txt else None
    except:
        return None

def _parse_sold_count(sold_text: str) -> int | None:
    """Helper do parsowania liczby sprzedanych (np. '100 osób kupiło')"""
    try:
        # Znajdź liczby w tekście
        match = re.search(r'\d+', sold_text.replace(' ', ''))
        return int(match.group(0)) if match else None
    except:
        return None


def fetch_allegro_data(ean: str, use_api: bool = False, api_key: str = None):
    """
    Ulepszony scraper MVP.
    Zwraca słownik z: lowest_price, sold_count, source, fetched_at, not_found (bool)
    """
    
    # Placeholder API
    if use_api and api_key:
        return {"lowest_price": None, "sold_count": None, "source": "api", "fetched_at": datetime.utcnow(), "not_found": False}

    # --- MODYFIKACJA: Ulepszony Scraper ---
    for attempt in range(3):
        try:
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            # Wyszukujemy po EAN w trybie "product" (karty produktu), nie "offer"
            url = f"https://allegro.pl/listing?string={ean}&scope=product"
            resp = requests.get(url, headers=headers, timeout=10)
            
            if resp.status_code != 200:
                time.sleep(1 + attempt)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            
            # 1. Sprawdź, czy produkt został znaleziony
            if soup.find("div", string=lambda t: t and "nie znaleźliśmy" in t.lower()):
                 return {"lowest_price": None, "sold_count": None, "source": "scrape", "fetched_at": datetime.utcnow(), "not_found": True}

            # 2. Znajdź "najtańszą ofertę" (zwykle pierwsza na liście)
            # Używamy bardziej stabilnych selektorów opartych o 'data-analytics-...'
            listing_items = soup.find_all("article", {"data-analytics-role": "offer"})
            
            if not listing_items:
                # Nie ma ofert, choć może być "karta produktu"
                return {"lowest_price": None, "sold_count": None, "source": "scrape", "fetched_at": datetime.utcnow(), "not_found": True}

            # Bierzemy pierwszą ofertę jako najtańszą (domyślne sortowanie Allegro)
            first_offer = listing_items[0]

            # 3. Parsuj cenę
            lowest_price = None
            price_el = first_offer.select_one("span.m9qz_Fq") # Główna cena
            if price_el:
                lowest_price = _parse_price(price_el.get_text())
            
            # 4. Parsuj liczbę sprzedanych
            sold_count = None
            # Szukamy tekstu typu "123 osoby kupiły" lub "kupiono 123 szt."
            sold_el = first_offer.select_one("span.msa3_z4")
            if sold_el:
                sold_count = _parse_sold_count(sold_el.get_text())

            # Zwróć sukces tylko jeśli mamy cenę
            if lowest_price:
                return {
                    "lowest_price": lowest_price, 
                    "sold_count": sold_count, 
                    "source": "scrape", 
                    "fetched_at": datetime.utcnow(), 
                    "not_found": False
                }
            else:
                # Jeśli jest oferta, ale nie ma ceny (np. błąd parsowania)
                time.sleep(1 + attempt)
                continue

        except Exception as e:
            # np. Timeout, błąd parsowania
            print(f"Błąd Scrapera (EAN: {ean}): {e}")
            time.sleep(2 + attempt)
    
    # --- KONIEC MODYFIKACJI ---
    
    # Jeśli wszystkie próby zawiodły
    return {"lowest_price": None, "sold_count": None, "source": "failed", "fetched_at": datetime.utcnow(), "not_found": False}