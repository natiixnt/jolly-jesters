# app/utils.py
from typing import Optional

def normalize_ean(raw_ean: Optional[str]) -> Optional[str]:
    """
    Normalizacja EAN: usuwa spacje, znaki nie-numeryczne, zera wiodące
    """
    if raw_ean is None:
        return None
    s = str(raw_ean).strip()
    s = ''.join(ch for ch in s if ch.isdigit())
    s = s.lstrip('0')
    return s or None

def parse_price(value: Optional[str]) -> Optional[float]:
    """
    Parsuje cenę z stringa do float, zamienia przecinki na kropki, usuwa nie-numeryczne
    """
    if value is None:
        return None
    s = str(value).strip().replace(',', '.')
    try:
        return float(s)
    except ValueError:
        s2 = ''.join(ch for ch in s if (ch.isdigit() or ch=='.'))
        try:
            return float(s2) if s2 else None
        except:
            return None

def convert_currency(amount: float, rate: float) -> float:
    """
    Konwersja kwoty z waluty źródłowej na bazową
    """
    return amount * rate

def is_profitable(purchase_price: float, lowest_price: float, multiplier: float = 1.5) -> bool:
    """
    Prosty wskaźnik opłacalności: czy cena Allegro >= purchase_price * multiplier
    """
    if purchase_price is None or lowest_price is None:
        return False
    return lowest_price >= purchase_price * multiplier
