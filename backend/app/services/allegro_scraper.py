# Plik: backend/app/services/allegro_scraper.py

import os
from datetime import datetime
import time
import re
import zipfile # Do tworzenia wtyczki proxy
import io # Do zapisu w pamięci
import base64 # Do przekazania wtyczki do Selenium

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options as ChromeOptions

# Używamy ..config, aby wyjść z folderu 'services' do 'app'
from ..config import PROXY_URL 

# Adres serwera Selenium (z docker-compose.yml)
SELENIUM_URL = os.getenv("SELENIUM_URL", "http://selenium:4444/wd/hub")

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
        match = re.search(r'\d+', sold_text.replace(' ', ''))
        return int(match.group(0)) if match else None
    except:
        return None

def get_proxy_extension():
    """
    Tworzy rozszerzenie Chrome (wtyczkę) w pamięci, 
    które obsługuje uwierzytelnianie proxy.
    """
    if not PROXY_URL:
        return None

    # Rozbierz PROXY_URL na części
    # Oczekiwany format: http://user:pass@host:port
    try:
        creds, location = PROXY_URL.split("://")[1].split("@")
        user, password = creds.split(":")
        host, port = location.split(":")
    except Exception as e:
        print(f"Błąd parsowania PROXY_URL: {e}. Upewnij się, że jest w formacie http://user:pass@host:port")
        return None

    manifest_json = """
    {
        "version": "1.0.0",
        "manifest_version": 2,
        "name": "Chrome Proxy",
        "permissions": [
            "proxy",
            "tabs",
            "unlimitedStorage",
            "storage",
            "<all_urls>",
            "webRequest",
            "webRequestBlocking"
        ],
        "background": {
            "scripts": ["background.js"]
        },
        "minimum_chrome_version":"22.0.0"
    }
    """

    background_js = f"""
    var config = {{
            mode: "fixed_servers",
            rules: {{
            singleProxy: {{
                scheme: "http",
                host: "{host}",
                port: parseInt({port})
            }},
            bypassList: ["localhost"]
            }}
        }};

    chrome.proxy.settings.set({{value: config, scope: "regular"}}, function() {{}});

    function callbackFn(details) {{
        return {{
            authCredentials: {{
                username: "{user}",
                password: "{password}"
            }}
        }};
    }}

    chrome.webRequest.onAuthRequired.addListener(
                callbackFn,
                {{urls: ["<all_urls>"]}},
                ['blocking']
    );
    """
    
    # Tworzymy plik .zip w pamięci
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr("manifest.json", manifest_json)
        zip_file.writestr("background.js", background_js)
    
    # Zwracamy plik .zip zakodowany w Base64
    return base64.b64encode(zip_buffer.getvalue()).decode('utf-8')


def get_driver():
    """Tworzy instancję zdalnej przeglądarki Chrome w kontenerze Selenium"""
    options = ChromeOptions()
    options.add_argument("--headless") # Działaj w tle
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("start-maximized")
    options.add_argument("disable-infobars")
    options.add_argument("--disable-extensions") # Wyłączamy domyślne rozszerzenia
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36")

    # --- POPRAWKA (KROK 39): Poprawna implementacja proxy ---
    proxy_extension = get_proxy_extension()
    if proxy_extension:
        options.add_encoded_extension(proxy_extension)
    # --- KONIEC POPRAWKI ---

    driver = webdriver.Remote(
        command_executor=SELENIUM_URL,
        options=options
    )
    return driver


def fetch_allegro_data(ean: str, use_api: bool = False, api_key: str = None):
    """
    Scraper (Krok 39) używający Selenium z poprawnym uwierzytelnianiem proxy.
    """
    
    if use_api and api_key:
        return {"lowest_price": None, "sold_count": None, "source": "api", "fetched_at": datetime.utcnow(), "not_found": False}

    driver = None
    try:
        driver = get_driver()
        # Używamy linku wyszukiwania, który podałeś
        url = f"https://allegro.pl/listing?string={ean}"
        driver.get(url)

        # Czekamy maksymalnie 10 sekund na pojawienie się pierwszej oferty
        wait = WebDriverWait(driver, 10)
        
        try:
            # Szybkie sprawdzenie, czy jest komunikat o braku wyników
            no_results = driver.find_element(By.XPATH, "//*[contains(text(), 'nie znaleźliśmy')]")
            if no_results:
                driver.quit()
                return {"lowest_price": None, "sold_count": None, "source": "scrape", "fetched_at": datetime.utcnow(), "not_found": True}
        except:
            pass # To dobrze, że nie znaleźliśmy tego komunikatu

        # Czekamy na załadowanie się elementu z ceną
        price_element = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "span.m9qz_Fq"))
        )
        
        lowest_price = _parse_price(price_element.text)
        
        # Spróbuj znaleźć liczbę sprzedanych (to pole jest opcjonalne)
        sold_count = None
        try:
            sold_element = driver.find_element(By.CSS_SELECTOR, "span.msa3_z4")
            sold_count = _parse_sold_count(sold_element.text)
        except:
            pass # Brak informacji o sprzedaży

        driver.quit()
        
        if lowest_price:
            return {
                "lowest_price": lowest_price, 
                "sold_count": sold_count, 
                "source": "selenium_proxy_v2", # Zmieniamy source, żeby wiedzieć, że to ta wersja
                "fetched_at": datetime.utcnow(), 
                "not_found": False
            }
        else:
            return {"lowest_price": None, "sold_count": None, "source": "failed", "fetched_at": datetime.utcnow(), "not_found": False}

    except Exception as e:
        print(f"Błąd Selenium (EAN: {ean}): {e}")
        if driver:
            driver.quit()
        return {"lowest_price": None, "sold_count": None, "source": "failed", "fetched_at": datetime.utcnow(), "not_found": False}