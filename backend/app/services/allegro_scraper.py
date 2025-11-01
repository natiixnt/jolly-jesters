# Plik: backend/app/services/allegro_scraper.py

# Używamy curl_cffi zamiast requests, aby podszyć się pod przeglądarkę
from curl_cffi.requests import Session
from bs4 import BeautifulSoup
from datetime import datetime
import time
import re

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
    Ulepszony scraper MVP (Krok 22) używający curl_cffi do impersonacji.
    Zwraca słownik z: lowest_price, sold_count, source, fetched_at, not_found (bool)
    """
    
    if use_api and api_key:
        # Placeholder API
        return {"lowest_price": None, "sold_count": None, "source": "api", "fetched_at": datetime.utcnow(), "not_found": False}

    # Używamy sesji, która podszywa się pod Chrome 110
    session = Session(impersonate="chrome110")
    
    # Nagłówki udające prawdziwą przeglądarkę
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7',
        'Upgrade-Insecure-Requests': '1',
    }
    
    url = f"https://allegro.pl/listing?string={ean}&scope=product"

    for attempt in range(3): # 3 próby
        try:
            # Używamy timeoutu 15 sekund (z Kroku 21)
            resp = session.get(url, headers=headers, timeout=15)
            
            if resp.status_code != 200:
                print(f"Błąd Scrapera (EAN: {ean}, Próba: {attempt+1}): Status {resp.status_code}")
                time.sleep(2 + attempt) # Czekaj dłużej po błędzie
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            
            # 1. Sprawdź, czy produkt został znaleziony
            if soup.find("div", string=lambda t: t and "nie znaleźliśmy" in t.lower()):
                 return {"lowest_price": None, "sold_count": None, "source": "scrape", "fetched_at": datetime.utcnow(), "not_found": True}

            # 2. Znajdź "najtańszą ofertę"
            first_offer = soup.find("article", {"data-analytics-role": "offer"})
            
            if not first_offer:
                return {"lowest_price": None, "sold_count": None, "source": "scrape", "fetched_at": datetime.utcnow(), "not_found": True}

            # 3. Parsuj cenę
            lowest_price = None
            # Używamy selektora, który jest bardziej stabilny
            price_el = first_offer.select_one("span.m9qz_Fq")
            if price_el:
                lowest_price = _parse_price(price_el.get_text())
            
            # 4. Parsuj liczbę sprzedanych
            sold_count = None
            sold_el = first_offer.select_one("span.msa3_z4") # Selektor dla "np. 123 osoby kupiły"
            if sold_el:
                sold_count = _parse_sold_count(sold_el.get_text())

            if lowest_price:
                return {
                    "lowest_price": lowest_price, 
                    "sold_count": sold_count, 
                    "source": "scrape", 
                    "fetched_at": datetime.utcnow(), 
                    "not_found": False
                }
            else:
                # Jeśli jest oferta, ale nie ma ceny (błąd selektora)
                print(f"Błąd Scrapera (EAN: {ean}, Próba: {attempt+1}): Znaleziono ofertę, ale nie znaleziono ceny.")
                time.sleep(2 + attempt)
                continue

        except Exception as e:
            # np. Timeout, błąd parsowania
            print(f"Błąd Scrapera (EAN: {ean}, Próba: {attempt+1}): {e}")
            time.sleep(3 + attempt)
    
    # Jeśli wszystkie 3 próby zawiodły
    return {"lowest_price": None, "sold_count": None, "source": "failed", "fetched_at": datetime.utcnow(), "not_found": False}