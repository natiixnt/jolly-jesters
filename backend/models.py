from sqlalchemy import Column, Integer, String, Numeric, Boolean, ForeignKey, DateTime, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from .database import Base

class ImportJob(Base):
    __tablename__ = 'import_jobs'
    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String)
    user_id = Column(Integer, nullable=True)
    status = Column(String, default='pending')  # pending, processing, done, failed
    meta = Column(JSONB, default={})
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class ProductInput(Base):
    __tablename__ = 'product_inputs'
    id = Column(Integer, primary_key=True, index=True)
    import_job_id = Column(Integer, ForeignKey('import_jobs.id'))
    ean = Column(String, index=True)
    name = Column(String)
    purchase_price = Column(Numeric)
    currency = Column(String)
    normalized_price = Column(Numeric)
    status = Column(String, default='pending')  # pending, queued, processing, done, not_found, error
    not_found = Column(Boolean, default=False)
    notes = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class AllegroCache(Base):
    __tablename__ = 'allegro_cache'
    id = Column(Integer, primary_key=True)
    ean = Column(String, unique=True, index=True)
    lowest_price = Column(Numeric)
    sold_count = Column(Integer)
    seller_info = Column(JSONB)
    source = Column(String)  # api / scrape
    fetched_at = Column(DateTime(timezone=True))

class Export(Base):
    __tablename__ = 'exports'
    id = Column(Integer, primary_key=True)
    import_job_id = Column(Integer)
    filepath = Column(String)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
