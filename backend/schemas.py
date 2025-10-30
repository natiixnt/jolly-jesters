from pydantic import BaseModel
from typing import Optional, Any
from datetime import datetime

class ImportStartResponse(BaseModel):
    job_id: int
    message: str

class JobStatus(BaseModel):
    id: int
    status: str
    meta: Optional[Any]

class ProductInputOut(BaseModel):
    id: int
    import_job_id: int
    ean: Optional[str]
    name: Optional[str]
    purchase_price: Optional[float]
    currency: Optional[str]
    normalized_price: Optional[float]
    status: str
    not_found: bool
    notes: Optional[str]
    created_at: datetime

    class Config:
        orm_mode = True
