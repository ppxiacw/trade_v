import os
from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - 兼容无 zoneinfo 环境
    ZoneInfo = None


_DEFAULT_MARKET_TZ = "Asia/Shanghai"
_FALLBACK_MARKET_TZ = timezone(timedelta(hours=8))


def get_market_timezone_name() -> str:
    return os.getenv("MARKET_TIMEZONE", _DEFAULT_MARKET_TZ)


def now_in_market_tz() -> datetime:
    """
    获取交易时区当前时间。
    默认使用 Asia/Shanghai，若运行环境无 tz 数据则回退为固定 UTC+8。
    """
    tz_name = get_market_timezone_name()
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo(tz_name))
        except Exception:
            pass
    return datetime.now(_FALLBACK_MARKET_TZ)
