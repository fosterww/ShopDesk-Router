from __future__ import annotations
import hashlib
from typing import Any, Dict, Optional

import aioboto3
from botocore.exceptions import ClientError
from api.app.config import settings

session = aioboto3.Session()


def _client() -> Any:
    return session.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.s3_region,
    )


S3_BUCKET_ATTACHMENTS = settings.s3_bucket


async def ensure_bucket(bucket: str = S3_BUCKET_ATTACHMENTS) -> None:
    async with _client() as s3:
        try:
            await s3.head_bucket(Bucket=bucket)
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code")
            if error_code not in {"404", "NoSuchBucket"}:
                raise
            await s3.create_bucket(Bucket=bucket)


def hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


async def put_bytes(
    data: bytes, mime: str, filename: str, bucket: str = S3_BUCKET_ATTACHMENTS
) -> str:
    await ensure_bucket(bucket)
    key = f"{hash_bytes(data)[:8]}/{filename}"
    async with _client() as s3:
        await s3.put_object(Body=data, Bucket=bucket, Key=key, ContentType=mime)
    return key


async def presign(key: str, ttl_seconds: int = 600, bucket: str = S3_BUCKET_ATTACHMENTS) -> str:
    async with _client() as s3:
        url = await s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=ttl_seconds,
        )
    return str(url)


async def head_object(key: str, bucket: str = S3_BUCKET_ATTACHMENTS) -> Optional[Dict[str, Any]]:
    async with _client() as s3:
        try:
            resp: Dict[str, Any] = await s3.head_object(Bucket=bucket, Key=key)
            return resp
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "404":
                return None
            raise
