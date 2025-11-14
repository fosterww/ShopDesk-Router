from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    app_env: str = 'dev'
    log_level: str = "INFO"
    port: int = "8000"

    database_ulr: str = "postgresql+psycopg://shopdesk:shopdesk@postgres:5432/shopdesk"
    redis_url: str = "redis://redis:6379/0"

    aws_access_key: str = "minio"
    aws_secret_access_key: str = "minio123"
    s3_endpoint: str = "http://minio:9000"
    s3_region: str = "us-east-1"
    s3_bucket: str = "shopdesk-attachments"

    jwt_secret: str = "devsecret"
    ml_mode: str = "stub"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        env_prefix = ""
        case_sensitive = False

settings = Settings()