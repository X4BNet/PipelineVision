import os
from contextlib import asynccontextmanager
import logging

from dotenv import load_dotenv

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.utils.logger import setup_logger
from app.db.session import engine, Base, SessionLocal
from app.api.router import api_router
from app.core.config import settings

# from app.core.scheduler import runner_scheduler
from app.middleware.logging import StructuredLoggingMiddleware
from app.middleware.auth import BetterAuthMiddleware
from app.services.github_installation_sync import sync_github_app_installations

load_dotenv()

setup_logger()
logger = logging.getLogger(__name__)

Base.metadata.create_all(bind=engine)

origins = [
    origin for origin in os.getenv("BACKEND_CORS_ORIGINS", "").split(",") if origin
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    db = SessionLocal()
    try:
        synced = await sync_github_app_installations(db)
        logger.info("Synced %s GitHub App installations on startup", synced)
    except Exception as exc:
        db.rollback()
        logger.warning("GitHub App installation startup sync failed: %s", exc)
    finally:
        db.close()

    yield


app = FastAPI(
    title="GitHub Runner Dashboard",
    description="Monitor and manage your GitHub runners",
    version="0.1.0",
    lifespan=lifespan,
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(StructuredLoggingMiddleware)
app.add_middleware(BetterAuthMiddleware)

app.include_router(api_router, prefix=settings.API_V1_STR)


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "Pipeline Vision"}
