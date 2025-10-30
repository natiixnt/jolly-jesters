import streamlit as st
import pandas as pd

st.title("Frontend test")

st.write("Hello world! Streamlit działa w Dockerze.")

# prosty upload pliku testowy
uploaded_file = st.file_uploader("Wybierz plik Excel", type=["xlsx","csv"])
if uploaded_file:
    df = pd.read_excel(uploaded_file) if uploaded_file.name.endswith(".xlsx") else pd.read_csv(uploaded_file)
    st.write("Podgląd danych:")
    st.dataframe(df.head())
