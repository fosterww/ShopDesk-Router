from fastapi import FastAPI
from api.app.config import settings

app = FastAPI(title="ShopDesk Router API", version="0.0.1")

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