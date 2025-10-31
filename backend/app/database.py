from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from .config import DATABASE_URL

# engine
engine = create_engine(DATABASE_URL, echo=False)

# session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# baza modeli
Base = declarative_base()

# dependency do FastAPI
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
