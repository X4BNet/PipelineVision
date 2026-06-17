import logging
import os

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from app.core.config import Settings

logger = logging.getLogger(__name__)


def _default_database_url() -> str:
    return (
        f"postgresql://{Settings.POSTGRES_USER}:{Settings.POSTGRES_PASSWORD}"
        f"@{Settings.POSTGRES_HOST}:{Settings.POSTGRES_PORT}/{Settings.POSTGRES_DB}"
        f"?sslmode=require&channel_binding=require"
    )


DATABASE_URL = os.getenv("DATABASE_URL") or _default_database_url()

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


# TODO: Throw an exception if the database connection fails
def get_db():
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        logger.error(e)
        raise
    finally:
        db.close()
