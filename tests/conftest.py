import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

load_dotenv(ROOT / ".env")

@pytest.fixture(scope="session")
async def async_db_session():
    try:
        from api.app.db import SessionLocal
    except Exception as exc:
        pytest.skip(f"DB session not available: {exc}")
    async with SessionLocal() as session:
        txn = await session.begin()
        try:
            yield session
        finally:
            await txn.rollback()
