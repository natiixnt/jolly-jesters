import streamlit as st
import requests

st.set_page_config(page_title='Importer - prototyp', layout='wide')
st.title('Importer - prototyp')

uploaded = st.file_uploader('Wybierz plik xlsx/csv', type=['xlsx','csv'])
category = st.text_input('Kategoria', '')
currency = st.text_input('Waluta zrodla', 'PLN')

if uploaded:
    st.write('Plik gotowy do wyslania:', uploaded.name)

if uploaded and st.button('Wczytaj i rozpocznij import'):
    # send to backend
    files = {'file': (uploaded.name, uploaded.getvalue())}
    data = {'category': category, 'currency': currency}
    try:
        resp = requests.post('http://localhost:8000/api/imports/start', files=files, data=data, timeout=30)
        if resp.ok:
            d = resp.json()
            st.success(f"Job utworzony: {d.get('job_id')}")
            st.write(d)
        else:
            st.error(f'blad: {resp.status_code} - {resp.text}')
    except Exception as e:
        st.error(f'blad polaczenia: {e}')

st.markdown('---')
st.write('Po wgraniu pliku sprawdzaj status: GET /api/imports/{job_id}/status')
