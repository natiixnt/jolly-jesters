import pandas as pd
from io import BytesIO

ALLOWED_EXTENSIONS = ["csv", "xlsx"]

def validate_file(uploaded_file) -> bool:
    """Sprawdza, czy plik ma dozwolone rozszerzenie"""
    if not uploaded_file:
        return False
    ext = uploaded_file.name.split('.')[-1].lower()
    return ext in ALLOWED_EXTENSIONS

def file_to_dataframe(uploaded_file) -> pd.DataFrame:
    """Konwertuje UploadFile do pandas DataFrame"""
    ext = uploaded_file.name.split('.')[-1].lower()
    if ext == "csv":
        df = pd.read_csv(BytesIO(uploaded_file.getvalue()), dtype=str)
    else:  # xlsx
        df = pd.read_excel(BytesIO(uploaded_file.getvalue()), dtype=str)
    return df

def prepare_download_csv(df: pd.DataFrame) -> bytes:
    """Konwertuje DataFrame na bytes CSV do streamlit download"""
    buffer = BytesIO()
    df.to_csv(buffer, index=False)
    return buffer.getvalue()
