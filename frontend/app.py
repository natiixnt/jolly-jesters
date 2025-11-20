# Plik: frontend/app.py
import streamlit as st
import requests
import pandas as pd
import time
import os
import redis
from io import BytesIO

API_BASE = "http://pilot_backend:8000/api" 
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
CACHE_TTL_DAYS = int(os.getenv("CACHE_TTL_DAYS", "30"))

def _get_redis():
    return redis.Redis.from_url(REDIS_URL)

st.set_page_config(page_title="Import Allegro", layout="wide")
st.title("Pilot: Import i analiza produktów")

# ----------------------
# Sterowanie liczb? okien scrapera
# ----------------------
with st.sidebar:
    st.header("Scraper: liczba okien")
    desired_default = 1
    redis_client = None
    try:
        redis_client = _get_redis()
        current = redis_client.get("scraper:desired_instances")
        if current:
            desired_default = max(1, min(20, int(current)))
    except Exception as e:
        st.warning(f"Redis niedostepny: {e}")

    desired = st.slider("Ile okien scrapera ma sie uruchomic?", 1, 20, desired_default)
    if st.button("Zapisz liczbe okien"):
        if redis_client:
            try:
                redis_client.set("scraper:desired_instances", desired)
                st.success(f"Ustawiono {desired} okien scrapera.")
            except Exception as e:
                st.error(f"Nie udalo sie zapisac do Redis: {e}")
        else:
            st.error("Brak polaczenia z Redis.")

    ttl_default = CACHE_TTL_DAYS
    if redis_client:
        try:
            ttl_val_current = redis_client.get("scraper:cache_ttl_days")
            if ttl_val_current:
                ttl_default = max(1, min(365, int(ttl_val_current)))
        except Exception:
            pass
    st.caption(f"Cache Allegro wazny {ttl_default} dni (starsze dane sa scrapowane ponownie).")

    st.subheader("TTL cache (dni)")
    ttl_val = st.slider("Ustaw waznosc cache (dni)", 1, 365, ttl_default)
    if st.button("Zapisz TTL cache"):
        if redis_client:
            try:
                redis_client.set("scraper:cache_ttl_days", ttl_val)
                st.success(f"Cache TTL ustawiono na {ttl_val} dni.")
            except Exception as e:
                st.error(f"Nie udalo sie zapisac TTL do Redis: {e}")
        else:
            st.error("Brak polaczenia z Redis.")

# ----------------------
# Upload pliku
# ----------------------
st.header("1. Wgraj plik Excel/CSV")
uploaded_file = st.file_uploader("Wybierz plik .xlsx lub .csv", type=["xlsx", "csv"])
category = st.text_input("Kategoria (np. perfumy)", "perfumy")
currency = st.text_input("Waluta (np. PLN)", "PLN")

if uploaded_file and st.button("Rozpocznij import"):
    files = {"file": (uploaded_file.name, uploaded_file, uploaded_file.type)}
    data = {"category": category, "currency": currency}
    
    with st.spinner("Wysyłanie pliku i tworzenie zadania..."):
        try:
            response = requests.post(f"{API_BASE}/imports/start", files=files, data=data)
            if response.status_code == 200:
                job_id = response.json()["job_id"]
                st.success(f"Plik wgrany, job_id={job_id}")
                st.session_state["job_id"] = job_id
                st.session_state["stop_polling"] = False 
                st.rerun() 
            else:
                st.error(f"Błąd przy wysyłaniu pliku: {response.status_code} - {response.text}")
        except requests.exceptions.ConnectionError:
            st.error(f"Błąd połączenia z backendem. Czy backend działa pod adresem {API_BASE}?")
        except Exception as e:
            st.error(f"Błąd krytyczny: {e}")


# ----------------------
# Monitorowanie postępu i wyników
# ----------------------
if "job_id" in st.session_state and not st.session_state.get("stop_polling", False):
    job_id = st.session_state["job_id"]
    st.subheader(f"Monitorowanie zadania: {job_id}")

    progress_bar = st.progress(0)
    status_text = st.empty()
    results_placeholder = st.empty()

    while True:
        try:
            # 1. Zawsze sprawdzaj status joba
            status_resp = requests.get(f"{API_BASE}/imports/{job_id}/status")
            if status_resp.status_code != 200:
                 st.error("Nie można pobrać statusu joba. Przerywam.")
                 st.session_state["stop_polling"] = True
                 break
            
            job_data = status_resp.json()
            status = job_data.get("status")
            notes = job_data.get("notes")
            total_products = job_data.get("total_products", 0)
            completed_products = job_data.get("completed_products", 0)
            processing_products = job_data.get("processing_products", 0)
            queued_products = job_data.get("queued_products", 0)

            # --- OSTATECZNA POPRAWKA LOGIKI PĘTLI ---

            # Stan 1: Błąd (NAJWYŻSZY PRIORYTET)
            if status in ["error", "FAILURE"]:
                st.error(f"Błąd krytyczny zadania! Powód: {notes}")
                st.warning("Proszę, popraw plik (np. nazwy kolumn lub zawartość) i wgraj go ponownie.")
                progress_bar.empty()
                st.session_state["stop_polling"] = True
                break # Zakończ pętlę

            # Stan 2: Sprawdź produkty
            products_resp = requests.get(f"{API_BASE}/imports/{job_id}/products")
            products = products_resp.json() if products_resp.status_code == 200 else []

            # Stan 3: W trakcie (jeśli nie ma jeszcze produktów)
            if not products:
                if total_products:
                    progress = int((completed_products / total_products) * 100)
                    progress_bar.progress(progress)
                    status_text.info(
                        f"Postęp: {progress}% (Zakończono {completed_products} z {total_products}. "
                        f"W kolejce: {queued_products}, w trakcie: {processing_products})"
                    )
                else:
                    if status in ["pending", "queued"]:
                        status_text.info(f"Oczekiwanie na uruchomienie zadania... (Status: {status})")
                    elif status == "processing":
                        status_text.info("Zadanie uruchomione, oczekiwanie na pierwsze wyniki...")

                time.sleep(3)
                continue # Sprawdź status ponownie

            # --- KONIEC POPRAWKI ---

            # Stan 4: Przetwarzanie (są produkty)
            df = pd.DataFrame(products)
            
            total = len(df)
            done = len(df[df["status"].isin(["done", "not_found", "error"])])
            progress = int((done / total) * 100) if total > 0 else 0

            # Użyj danych z backendu, jeśli istnieją (np. gdy tabela jest filtrowana)
            if total_products:
                progress = int((completed_products / total_products) * 100)
                done = completed_products
                total = total_products

            progress_bar.progress(progress)
            status_text.info(
                f"Postęp: {progress}% (Przetworzono {done} z {total}; "
                f"W kolejce: {queued_products}, w trakcie: {processing_products})"
            )

            def color_recommendation(val):
                if val == "opłacalny": return "background-color: #b2f0b2"
                elif val == "nieopłacalny": return "background-color: #f0b2b2"
                elif val == "brak na Allegro": return "background-color: #f0e1b2"
                elif val == "błąd pobierania": return "background-color: #f0b2b2"
                elif val == "w trakcie...": return "background-color: #e0e0e0"
                else: return ""

            # --- POPRAWKA: Zmiana .applymap na .map ---
            results_placeholder.dataframe(
                df.style.map(color_recommendation, subset=["recommendation"])
            )
            # --- KONIEC POPRAWKI ---

            

            # Stan 5: Sukces (Zakończono)
            if done == total and total > 0:
                st.success("Przetwarzanie zakończone!")
                st.session_state["stop_polling"] = True 
                
                csv_buffer = BytesIO()
                df.to_csv(csv_buffer, index=False, encoding='utf-8')
                st.download_button(
                    label="Pobierz gotowy raport CSV",
                    data=csv_buffer.getvalue(),
                    file_name=f"raport_job_{job_id}.csv",
                    mime="text/csv"
                )
                break 

            time.sleep(3) 

        except requests.exceptions.ConnectionError:
            st.error("Utracono połączenie z backendem. Przerywam monitorowanie.")
            st.session_state["stop_polling"] = True
            break
        except Exception as e:
            st.error(f"Wystąpił błąd frontendu: {e}")
            st.session_state["stop_polling"] = True
            break
