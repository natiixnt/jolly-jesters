import streamlit as st
import requests
import pandas as pd
import time
from io import BytesIO
from utils import validate_file

st.set_page_config(page_title="Import Allegro", layout="wide")
st.title("Pilot: Import i analiza produktów")

API_BASE = "http://pilot_backend:8000/api"

# ----------------------
# Upload pliku
# ----------------------
uploaded_file = st.file_uploader("Wybierz plik CSV lub Excel", type=["csv", "xlsx"])
category = st.text_input("Kategoria")
currency = st.text_input("Waluta (np. PLN)")

if uploaded_file and validate_file(uploaded_file):
    st.success(f"Plik {uploaded_file.name} poprawny")
    if st.button("Wyślij do analizy"):
        if not category or not currency:
            st.error("Podaj kategorię i walutę")
        else:
            with st.spinner("Wysyłanie pliku i tworzenie zadania..."):
                files = {"file": (uploaded_file.name, uploaded_file.getvalue())}
                data = {"category": category, "currency": currency}
                try:
                    resp = requests.post(f"{API_BASE}/imports/start", files=files, data=data)
                    if resp.status_code == 200:
                        job_id = resp.json().get("job_id")
                        st.success(f"Job utworzony: {job_id}")
                        st.session_state["job_id"] = job_id
                        st.session_state["job_started"] = True
                        st.rerun()
                    else:
                        st.error(f"Błąd serwera: {resp.text}")
                except Exception as e:
                    st.error(f"Błąd połączenia: {e}")

# ----------------------
# Monitorowanie postępu i wyników
# ----------------------
if "job_id" in st.session_state:
    job_id = st.session_state["job_id"]
    st.subheader(f"Status zadania: {job_id}")

    progress_bar = st.progress(0)
    status_text = st.empty()
    placeholder_table = st.empty()

    def fetch_products():
        try:
            resp = requests.get(f"{API_BASE}/imports/{job_id}/products")
            if resp.status_code == 200:
                return resp.json()
            else:
                st.error(f"Błąd serwera: {resp.text}")
                return []
        except Exception as e:
            st.error(f"Błąd połączenia: {e}")
            return []

    for _ in range(100):  # maksymalnie ok. 8 minut
        products = fetch_products()
        if not products:
            time.sleep(5)
            continue

        total = len(products)
        done = sum(1 for p in products if p['status'] in ['done', 'not_found', 'error'])
        progress = int((done / total) * 100) if total else 0
        progress_bar.progress(progress)
        status_text.text(f"Przetworzono {done} z {total} produktów ({progress}%)")

        df = pd.DataFrame(products)

        if not df.empty:
            if 'recommendation' not in df.columns:
                df['recommendation'] = df.get('notes', 'brak danych')

            if 'allegro_url' in df.columns:
                df['allegro_link'] = df['allegro_url'].apply(
                    lambda x: f'=HYPERLINK("{x}", "Zobacz ofertę")' if pd.notna(x) and str(x).startswith("http") else ''
                )

            def color_row(row):
                if row['status'] in ['pending', 'queued']:
                    return [''] * len(row)
                rec = str(row.get('recommendation', '')).lower()
                if 'opłacalny' in rec:
                    return ['background-color: #b6fcb6'] * len(row)
                elif 'nieopłacalny' in rec:
                    return ['background-color: #fcb6b6'] * len(row)
                else:
                    return ['background-color: #e0e0e0'] * len(row)

            placeholder_table.dataframe(df.style.apply(color_row, axis=1))

        if done == total and total > 0:
            st.success("Import zakończony")

            excel_data = BytesIO()
            df.to_excel(excel_data, index=False)
            excel_data.seek(0)
            st.download_button(
                label="Pobierz wyniki analizy",
                data=excel_data,
                file_name=f"analysis_job_{job_id}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            break

        time.sleep(5)
