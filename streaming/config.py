import os
from dataclasses import dataclass


def _env_str(name: str, default: str) -> str:
    return os.getenv(name, default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = _env_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic_name: str = _env_str("KAFKA_TOPIC_NAME", "raw_manga")
    group_id: str = _env_str("KAFKA_CONSUMER_GROUP", "mangaflow_consumer_group")
    auto_offset_reset: str = _env_str("KAFKA_AUTO_OFFSET_RESET", "earliest")


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str = _env_str("MINIO_ENDPOINT", "http://localhost:9000")
    access_key: str = _env_str("MINIO_ACCESS_KEY", "your_access_key_here") 
    secret_key: str = _env_str("MINIO_SECRET_KEY", "your_secret_key_here")  
    bucket_name: str = _env_str("MINIO_BUCKET_NAME", "bronze-manga")


@dataclass(frozen=True)
class ProducerConfig:
    api_url: str = _env_str("MANGA_API_URL", "https://api.jikan.moe/v4/manga")
    page_limit: int = _env_int("MANGA_API_PAGE_LIMIT", 25)
    request_timeout: int = _env_int("MANGA_API_TIMEOUT", 10)
    rate_limit_wait: int = _env_int("MANGA_API_RATE_LIMIT_WAIT", 3)
    retry_wait: int = _env_int("MANGA_API_RETRY_WAIT", 5)
    max_retries: int = _env_int("MANGA_API_MAX_RETRIES", 3)


@dataclass(frozen=True)
class ConsumerConfig:
    batch_size: int = _env_int("CONSUMER_BATCH_SIZE", 50)
    max_wait_time: int = _env_int("CONSUMER_MAX_WAIT_TIME", 15)
