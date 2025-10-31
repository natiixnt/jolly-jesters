# frontend.py
import streamlit as st
import requests
import pandas as pd
from io import BytesIO

API_BASE = "http://localhost:8000/api"

st.set_page_config(page_title="Import Allegro", layout="wide")
st.title("Pilot Import Allegro")

# ----------------------
# Upload pliku
# ----------------------
st.header("1. Wgraj plik Excel/CSV")
uploaded_file = st.file_uploader("Wybierz plik .xlsx lub .csv", type=["xlsx", "csv"])
category = st.text_input("Kategoria", "perfumy")
currency = st.text_input("Waluta", "PLN")

if uploaded_file and st.button("Rozpocznij import"):
    files = {"file": (uploaded_file.name, uploaded_file, uploaded_file.type)}
    data = {"category": category, "currency": currency}
    response = requests.post(f"{API_BASE}/imports/start", files=files, data=data)
    if response.status_code == 200:
        job_id = response.json()["job_id"]
        st.success(f"Plik wgrany, job_id={job_id}")
        st.session_state["job_id"] = job_id
    else:
        st.error("Błąd przy wysyłaniu pliku")

# ----------------------
# Status importu
# ----------------------
st.header("2. Status importu")
job_id = st.session_state.get("job_id")
if job_id:
    try:
        response = requests.get(f"{API_BASE}/imports/{job_id}/status")
        if response.status_code == 200:
            status = response.json()["status"]
            st.info(f"Job status: {status}")
        else:
            st.warning("Nie można pobrać statusu")
    except:
        st.warning("Błąd połączenia z backendem")

# ----------------------
# Produkty i analiza
# ----------------------
st.header("3. Produkty i analiza")
if job_id:
    try:
        response = requests.get(f"{API_BASE}/imports/{job_id}/products")
        if response.status_code == 200:
            products = response.json()
            if products:
                df = pd.DataFrame(products)

                # ----------------------
                # Podsumowanie statystyk
                # ----------------------
                st.subheader("Podsumowanie")
                total = len(df)
                done = len(df[df["status"] == "done"])
                pending = len(df[df["status"] == "pending"])
                not_found = len(df[df["status"] == "not_found"])
                error = len(df[df["status"] == "error"])
                opłacalny = len(df[df["recommendation"] == "opłacalny"])
                nieopłacalny = len(df[df["recommendation"] == "nieopłacalny"])
                brak_danych = len(df[df["recommendation"] == "brak danych"])

                st.write(f"Łącznie produktów: {total}")
                st.write(f"Done: {done}, Pending: {pending}, Not Found: {not_found}, Error: {error}")
                st.write(f"Opłacalne: {opłacalny}, Nieopłacalne: {nieopłacalny}, Brak danych: {brak_danych}")

                # ----------------------
                # Filtry wyników
                # ----------------------
                st.subheader("Filtry")
                rekomendacja_options = ["wszystkie", "opłacalny", "nieopłacalny", "brak danych"]
                status_options = ["wszystkie", "pending", "queued", "processing", "done", "not_found", "error"]

                selected_rekom = st.selectbox("Rekomendacja", rekomendacja_options)
                selected_status = st.selectbox("Status", status_options)

                df_filtered = df.copy()
                if selected_rekom != "wszystkie" and "recommendation" in df.columns:
                    df_filtered = df_filtered[df_filtered["recommendation"] == selected_rekom]
                if selected_status != "wszystkie" and "status" in df.columns:
                    df_filtered = df_filtered[df_filtered["status"] == selected_status]

                # kolory wg rekomendacji
                def color_recommendation(val):
                    if val == "opłacalny":
                        return "background-color: #b2f0b2"
                    elif val == "nieopłacalny":
                        return "background-color: #f0b2b2"
                    else:
                        return "background-color: #e0e0e0"

                st.subheader("Tabela wyników")
                st.dataframe(df_filtered.style.applymap(color_recommendation, subset=["recommendation"]))

                # ----------------------
                # Eksport CSV
                # ----------------------
                csv_buffer = BytesIO()
                df_filtered.to_csv(csv_buffer, index=False)
                st.download_button(
                    label="Pobierz raport CSV",
                    data=csv_buffer.getvalue(),
                    file_name=f"raport_import_{job_id}.csv",
                    mime="text/csv"
                )
            else:
                st.info("Brak produktów w tym jobie")
        else:
            st.warning("Nie można pobrać produktów")
    except Exception as e:
        st.error(f"Błąd pobierania produktów: {e}")
