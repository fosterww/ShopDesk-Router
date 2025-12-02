from fastapi import FastAPI
from contextlib import asynccontextmanager

from api.app.config import settings
from common.storage.s3 import ensure_bucket
from api.app.routers.ingest import router as ingest_router
from api.app.routers.attachments import router as attachments_router
from api.app.routers.debug_ml import router as debugml_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    await ensure_bucket()
    yield

app = FastAPI(lifespan=lifespan, title="ShopDesk Router API", version="0.0.1")

app.include_router(ingest_router)
app.include_router(attachments_router)
app.include_router(debugml_router)

@app.get("/health")
def health():
    return {
        "status": "OK",
        "version": app.version,
        "env": settings.app_env,
        "services": {
            "db": "unknown",
            "redis": "unknown",
            "s3": "unknown",
        },
    }
