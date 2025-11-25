from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    app_env: str = "dev"
    log_level: str = "INFO"
    port: int = 8000

    database_url: str = "postgresql+asyncpg://shopdesk:shopdesk@postgres:5432/shopdesk"
    redis_url: str = "redis://redis:6379/0"

    aws_access_key_id: str = "minio"
    aws_secret_access_key: str = "minio123"
    s3_endpoint: str = "http://minio:9000"
    s3_region: str = "us-east-1"
    s3_bucket: str = "shopdesk-attachments"

    jwt_secret: str = "devsecret"
    ml_mode: str = "stub"

    gmail_user: str | None = None
    gmail_service_account_file: str | None = None
    gmail_query: str = ""
    gmail_max_results: int = 20
    gmail_batch_size: int = 20
    gmail_label_ids: list[str] | str | None = None
    gmail_history_start: str | None = None

    @field_validator("gmail_label_ids", mode="before")
    @classmethod
    def split_gmail_labels(cls, v):
        if v in (None, "", [], ()):
            return []
        if isinstance(v, str):
            return [p.strip() for p in v.split(",") if p.strip()]
        return v

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="",
        case_sensitive=False,
        extra="ignore",
    )


settings = Settings()
