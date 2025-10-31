from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey, JSON
from sqlalchemy.orm import relationship
from datetime import datetime
from .database import Base

# -----------------------
# Import job – info o imporcie pliku
# -----------------------
class ImportJob(Base):
    __tablename__ = "import_jobs"
    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String, nullable=False)
    meta = Column(JSON, nullable=True)  # przechowuje category i currency
    multiplier = Column(Float, default=1.5)
    created_at = Column(DateTime, default=datetime.utcnow)
    status = Column(String, default="pending")
    products = relationship("ProductInput", back_populates="import_job")
    exports = relationship("Export", back_populates="import_job")

# -----------------------
# Product input – rekordy z wczytanego pliku
# -----------------------
class ProductInput(Base):
    __tablename__ = "product_inputs"

    id = Column(Integer, primary_key=True, index=True)
    import_job_id = Column(Integer, ForeignKey("import_jobs.id"), nullable=False)
    ean = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False)
    purchase_price = Column(Float, nullable=False)
    currency = Column(String, nullable=False)
    normalized_price = Column(Float, nullable=True)
    status = Column(String, default="pending")  # pending, queued, processing, done, not_found, error
    created_at = Column(DateTime, default=datetime.utcnow)

    import_job = relationship("ImportJob", back_populates="products")

# -----------------------
# Allegro cache – wyniki scrapingu / API
# -----------------------
class AllegroCache(Base):
    __tablename__ = "allegro_cache"

    id = Column(Integer, primary_key=True, index=True)
    ean = Column(String, nullable=False, unique=True, index=True)
    lowest_price = Column(Float, nullable=True)
    sold_count = Column(Integer, nullable=True)
    source = Column(String, nullable=True)  # api / scrape / cache
    fetched_at = Column(DateTime, default=datetime.utcnow)
    not_found = Column(Boolean, default=False)

# -----------------------
# Eksport wyników
# -----------------------
class Export(Base):
    __tablename__ = "exports"

    id = Column(Integer, primary_key=True, index=True)
    import_job_id = Column(Integer, ForeignKey("import_jobs.id"), nullable=False)
    filepath = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    import_job = relationship("ImportJob", back_populates="exports")
