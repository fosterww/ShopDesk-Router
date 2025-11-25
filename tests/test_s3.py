import asyncio
import uuid

import pytest

from common.storage.s3 import (
    put_bytes,
    presign,
    head_object,
    ensure_bucket,
    S3_BUCKET_ATTACHMENTS,
)


@pytest.fixture(scope="session", autouse=True)
def ensure_minio_bucket():
    try:
        asyncio.run(ensure_bucket(S3_BUCKET_ATTACHMENTS))
    except Exception as exc:
        pytest.skip(f"MinIO is not reachable for S3 tests: {exc}")


def test_put_and_head_object(ensure_minio_bucket):
    async def _run():
        data = b"hello shopdesk"
        filename = f"test-{uuid.uuid4().hex}.txt"
        mime = "text/plain"

        key = await put_bytes(data=data, mime=mime, filename=filename)

        assert isinstance(key, str)
        assert len(key) > 0

        meta = await head_object(key)
        assert meta is not None
        assert meta["ContentLength"] == len(data)
        assert meta["ContentType"] == mime

    asyncio.run(_run())


def test_presign_contains_key(ensure_minio_bucket):
    async def _run():
        data = b"another test file"
        filename = f"test-{uuid.uuid4().hex}.bin"
        mime = "application/octet-stream"

        key = await put_bytes(data=data, mime=mime, filename=filename)

        url = await presign(key, ttl_seconds=600)

        assert isinstance(url, str)
        assert len(url) > 0
        assert key in url

    asyncio.run(_run())


def test_same_bytes_same_key(ensure_minio_bucket):
    async def _run():
        data = b"stable-content"
        filename = "stable-file.txt"
        mime = "text/plain"

        key1 = await put_bytes(data=data, mime=mime, filename=filename)
        key2 = await put_bytes(data=data, mime=mime, filename=filename)

        assert key1 == key2

    asyncio.run(_run())
