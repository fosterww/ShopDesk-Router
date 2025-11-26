import asyncio
import uuid

import pytest

from common.storage.s3 import AttachmentStorage


@pytest.fixture(scope="session")
def s3_storage():
    s3 = AttachmentStorage()
    try:
        asyncio.run(s3.ensure_bucket())
    except Exception as exc:
        pytest.skip(f"MinIO is not reachable for S3 tests: {exc}")
    return s3


def test_put_and_head_object(s3_storage: AttachmentStorage):
    async def _run():
        data = b"hello shopdesk"
        filename = f"test-{uuid.uuid4().hex}.txt"
        mime = "text/plain"

        key = await s3_storage.put(data=data, mime=mime, filename=filename)

        assert isinstance(key, str)
        assert len(key) > 0

        meta = await s3_storage.head(key)
        assert meta is not None
        assert meta["ContentLength"] == len(data)
        assert meta["ContentType"] == mime

    asyncio.run(_run())


def test_presign_contains_key(s3_storage: AttachmentStorage):
    async def _run():
        data = b"another test file"
        filename = f"test-{uuid.uuid4().hex}.bin"
        mime = "application/octet-stream"

        key = await s3_storage.put(data=data, mime=mime, filename=filename)

        url = await s3_storage.presign(key, ttl_seconds=600)

        assert isinstance(url, str)
        assert len(url) > 0
        assert key in url

    asyncio.run(_run())


def test_same_bytes_same_key(s3_storage: AttachmentStorage):
    async def _run():
        data = b"stable-content"
        filename = "stable-file.txt"
        mime = "text/plain"

        key1 = await s3_storage.put(data=data, mime=mime, filename=filename)
        key2 = await s3_storage.put(data=data, mime=mime, filename=filename)

        assert key1 == key2

    asyncio.run(_run())
