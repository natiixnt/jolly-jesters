# Plik: backend/app/services/allegro_scraper.py

import os
from datetime import datetime
import time
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options as ChromeOptions

from ..config import PROXY_URL # Wciąż używamy proxy ze Smartproxy

# Adres serwera Selenium (z docker-compose.yml)
SELENIUM_URL = os.getenv("SELENIUM_URL", "http://selenium:4444/wd/hub")

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


def get_driver():
    """Tworzy instancję zdalnej przeglądarki Chrome w kontenerze Selenium"""
    options = ChromeOptions()
    options.add_argument("--headless") # Działaj w tle
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("start-maximized")
    options.add_argument("disable-infobars")
    options.add_argument("--disable-extensions")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36")

    # --- KONFIGURACJA PROXY DLA SELENIUM ---
    if PROXY_URL:
        # Selenium wymaga, aby proxy było dodane jako "capability"
        # Usuwamy 'http://' lub 'https://' z początku
        proxy_address = PROXY_URL.split("://")[-1]
        
        # Tworzymy plik .zip z danymi logowania dla proxy (wymagane przez Chrome)
        plugin_file = 'proxy_auth_plugin.zip'
        
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
                    host: "{proxy_address.split('@')[-1].split(':')[0]}",
                    port: parseInt({proxy_address.split(':')[-1]})
                }},
                bypassList: ["localhost"]
                }}
            }};

        chrome.proxy.settings.set({{value: config, scope: "regular"}}, function() {{}});

        function callbackFn(details) {{
            return {{
                authCredentials: {{
                    username: "{proxy_address.split('@')[0].split(':')[0]}",
                    password: "{proxy_address.split('@')[0].split(':')[1]}"
                }}
            }};
        }}

        chrome.webRequest.onAuthRequired.addListener(
                    callbackFn,
                    {{urls: ["<all_urls>"]}},
                    ['blocking']
        );
        """
        
        # Selenium nie potrafi załadować rozszerzenia zdalnie, jeśli nie jest .zip
        # Zapisujemy je w kontenerze workera (będzie widoczne dla Selenium dzięki wolumenowi)
        # UWAGA: To jest bardziej zaawansowane, prostsza metoda może nie działać z proxy z hasłem
        
        # Użyjemy prostszej metody dla MVP:
        options.add_argument(f'--proxy-server={PROXY_URL}')

    # Łączymy się ze zdalnym Chrome w kontenerze 'selenium'
    driver = webdriver.Remote(
        command_executor=SELENIUM_URL,
        options=options
    )
    return driver


def fetch_allegro_data(ean: str, use_api: bool = False, api_key: str = None):
    """
    Scraper (Krok 35) używający Selenium do pełnej mimikry przeglądarki.
    """
    
    if use_api and api_key:
        return {"lowest_price": None, "sold_count": None, "source": "api", "fetched_at": datetime.utcnow(), "not_found": False}

    driver = None
    try:
        driver = get_driver()
        url = f"https://allegro.pl/listing?string={ean}&scope=product"
        driver.get(url)

        # Czekamy maksymalnie 10 sekund na pojawienie się pierwszej oferty
        wait = WebDriverWait(driver, 10)
        
        # 1. Sprawdź, czy nie ma wyników
        try:
            # Szybkie sprawdzenie, czy jest komunikat o braku wyników
            no_results = driver.find_element(By.XPATH, "//*[contains(text(), 'nie znaleźliśmy')]")
            if no_results:
                driver.quit()
                return {"lowest_price": None, "sold_count": None, "source": "scrape", "fetched_at": datetime.utcnow(), "not_found": True}
        except:
            pass # To dobrze, że nie znaleźliśmy tego komunikatu

        # 2. Poczekaj na załadowanie się elementu z ceną
        price_element = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "span.m9qz_Fq"))
        )
        
        lowest_price = _parse_price(price_element.text)
        
        # 3. Spróbuj znaleźć liczbę sprzedanych (to pole jest opcjonalne)
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
                "source": "selenium", 
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