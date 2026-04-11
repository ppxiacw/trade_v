import os
from typing import Any, Dict


def _get_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def get_db_runtime_settings() -> Dict[str, Any]:
    """
    统一读取数据库运行时配置，避免多处硬编码导致线上/本地不一致。
    """
    return {
        "host": os.getenv("DB_HOST", "212.64.32.213"),
        "user": os.getenv("DB_USER", "trade"),
        "password": os.getenv("DB_PASSWORD", "trade007576!"),
        "database": os.getenv("DB_NAME", "trade"),
        "pool_name": os.getenv("DB_POOL_NAME", "trade_v_pool"),
        "pool_size": _get_int_env("DB_POOL_SIZE", 10),
        "connect_timeout": _get_int_env("DB_CONNECT_TIMEOUT", 30),
        "pool_recycle_seconds": _get_int_env("DB_POOL_RECYCLE_SECONDS", 300),
        "charset": os.getenv("DB_CHARSET", "utf8mb4"),
        "collation": os.getenv("DB_COLLATION", "utf8mb4_unicode_ci"),
    }


def get_db_connection_uri(db_settings: Dict[str, Any]) -> str:
    return (
        "mysql+mysqlconnector://"
        f"{db_settings['user']}:{db_settings['password']}@"
        f"{db_settings['host']}/{db_settings['database']}"
    )


def get_app_runtime_version() -> str:
    for key in ("APP_VERSION", "BUILD_VERSION", "GIT_COMMIT", "COMMIT_SHA"):
        value = os.getenv(key)
        if value:
            return str(value)
    return "dev-local"


def get_app_runtime_env() -> str:
    return os.getenv("APP_ENV", "development")
