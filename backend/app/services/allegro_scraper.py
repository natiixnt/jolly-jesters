import requests
from bs4 import BeautifulSoup
from datetime import datetime
import random
import time

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)",
]

def fetch_allegro_data(ean: str, use_api: bool = False, api_key: str = None):
    # placeholder API
    if use_api and api_key:
        return {"lowest_price": None, "sold_count": None, "source": "api", "fetched_at": datetime.utcnow()}

    # scraping MVP
    for attempt in range(3):
        try:
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            url = f"https://allegro.pl/listing?string={ean}"
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                price_el = soup.select_one("span[aria-hidden='true']")
                lowest_price = None
                if price_el:
                    txt = price_el.get_text()
                    txt = "".join(ch for ch in txt if ch.isdigit() or ch in ".,")
                    txt = txt.replace(",", ".")
                    lowest_price = float(txt) if txt else None
                return {"lowest_price": lowest_price, "sold_count": None, "source": "scrape", "fetched_at": datetime.utcnow()}
        except:
            time.sleep(2 + attempt)
    return {"lowest_price": None, "sold_count": None, "source": "failed", "fetched_at": datetime.utcnow()}
