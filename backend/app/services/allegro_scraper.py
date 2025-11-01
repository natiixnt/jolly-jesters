# Plik: backend/app/services/allegro_scraper.py

from curl_cffi.requests import Session
from bs4 import BeautifulSoup
from datetime import datetime
import time
import re

# (Krok 29) Importujemy PROXY_URL z naszej konfiguracji
from .config import PROXY_URL

def _parse_price(price_text: str) -> float | None:
    try:
        txt = "".join(ch for ch in price_text if ch.isdigit() or ch in ".,")
        txt = txt.replace(",", ".").replace(" ", "")
        return float(txt) if txt else None
    except:
        return None

def _parse_sold_count(sold_text: str) -> int | None:
    try:
        match = re.search(r'\d+', sold_text.replace(' ', ''))
        return int(match.group(0)) if match else None
    except:
        return None


def fetch_allegro_data(ean: str, use_api: bool = False, api_key: str = None):
    """
    Scraper (Krok 29) używający curl_cffi ORAZ rotacji proxy.
    """
    
    if use_api and api_key:
        return {"lowest_price": None, "sold_count": None, "source": "api", "fetched_at": datetime.utcnow(), "not_found": False}

    session = Session(impersonate="chrome110")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7',
        'Upgrade-Insecure-Requests': '1',
    }
    
    url = f"https://allegro.pl/listing?string={ean}&scope=product"

    # --- POPRAWKA (KROK 29): Użyj proxy, jeśli jest zdefiniowane ---
    proxies_dict = None
    if PROXY_URL:
        proxies_dict = {
            "http": PROXY_URL,
            "https": PROXY_URL
        }
    # --- KONIEC POPRAWKI ---

    for attempt in range(3): 
        try:
            # Dodajemy argument 'proxies' do zapytania
            resp = session.get(url, headers=headers, timeout=20, proxies=proxies_dict)
            
            if resp.status_code != 200:
                print(f"Błąd Scrapera (EAN: {ean}, Próba: {attempt+1}): Status {resp.status_code}")
                # Jeśli błąd to 403 (Forbidden), nie ma sensu próbować ponownie z tego samego IP
                if resp.status_code == 403:
                    print("Błąd 403 - Prawdopodobnie zablokowane IP proxy. Przerywam próby dla tego EAN.")
                    break 
                time.sleep(2 + attempt)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            
            if soup.find("div", string=lambda t: t and "nie znaleźliśmy" in t.lower()):
                 return {"lowest_price": None, "sold_count": None, "source": "scrape", "fetched_at": datetime.utcnow(), "not_found": True}

            first_offer = soup.find("article", {"data-analytics-role": "offer"})
            
            if not first_offer:
                return {"lowest_price": None, "sold_count": None, "source": "scrape", "fetched_at": datetime.utcnow(), "not_found": True}

            lowest_price = None
            price_el = first_offer.select_one("span.m9qz_Fq")
            if price_el:
                lowest_price = _parse_price(price_el.get_text())
            
            sold_count = None
            sold_el = first_offer.select_one("span.msa3_z4")
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
                print(f"Błąd Scrapera (EAN: {ean}, Próba: {attempt+1}): Znaleziono ofertę, ale nie znaleziono ceny.")
                time.sleep(2 + attempt)
                continue

        except Exception as e:
            print(f"Błąd Scrapera (EAN: {ean}, Próba: {attempt+1}): {e}")
            time.sleep(3 + attempt)
    
    return {"lowest_price": None, "sold_count": None, "source": "failed", "fetched_at": datetime.utcnow(), "not_found": False}