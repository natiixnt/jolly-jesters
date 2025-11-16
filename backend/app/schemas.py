from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime

# -----------------------
# Import job
# -----------------------
class ImportJobBase(BaseModel):
    filename: str
    category: str
    currency: str
    multiplier: Optional[float] = 1.5

class ImportJobCreate(ImportJobBase):
    pass

class ImportJobResponse(ImportJobBase):
    id: int
    status: str
    created_at: datetime

    class Config:
        from_attributes = True # POPRAWKA: 'orm_mode' -> 'from_attributes'

# -----------------------
# Endpoint start import
# -----------------------
class ImportStartResponse(BaseModel):
    job_id: int
    message: str

# -----------------------
# Job status
# -----------------------
class JobStatus(BaseModel):
    id: int
    status: str
    meta: Optional[dict] = None
    notes: Optional[str] = None # <-- DODAJ TĘ LINIĘ
    total_products: int = 0
    completed_products: int = 0
    processing_products: int = 0
    queued_products: int = 0

# -----------------------
# Product input
# -----------------------
class ProductInputBase(BaseModel):
    ean: str
    name: str
    purchase_price: float
    currency: str

class ProductInputCreate(ProductInputBase):
    import_job_id: int

class ProductInputResponse(ProductInputBase):
    id: int
    normalized_price: Optional[float]
    status: str
    not_found: bool = False
    notes: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True # POPRAWKA: 'orm_mode' -> 'from_attributes'

# -----------------------
# Allegro cache
# -----------------------
class AllegroCacheBase(BaseModel):
    ean: str

class AllegroCacheCreate(AllegroCacheBase):
    lowest_price: Optional[float]
    sold_count: Optional[int]
    source: Optional[str]
    not_found: Optional[bool] = False

class AllegroCacheResponse(AllegroCacheBase):
    id: int
    lowest_price: Optional[float]
    sold_count: Optional[int]
    source: Optional[str]
    fetched_at: datetime
    not_found: bool

    class Config:
        from_attributes = True # POPRAWKA: 'orm_mode' -> 'from_attributes'

# -----------------------
# Export
# -----------------------
class ExportBase(BaseModel):
    filepath: str
    import_job_id: int

class ExportResponse(ExportBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True # POPRAWKA: 'orm_mode' -> 'from_attributes'

# -----------------------
# Raport zbiorczy dla UI
# -----------------------
class ProductAnalysis(BaseModel):
    ean: str
    name: str
    purchase_price: float
    lowest_price_allegro: Optional[float]
    sold_count: Optional[int]
    source: Optional[str]
    last_checked: Optional[datetime]
    profit_margin: Optional[float]
    recommendation: Optional[str]  # opłacalny / nieopłacalny / brak danych
    notes: Optional[str]
    status: Optional[str] = None