import logging
import threading
import time
from typing import Dict, Optional, Set

import akshare as ak

from monitor.config.db_monitor import exe_query

logger = logging.getLogger(__name__)

_DB_NAME_CACHE_TTL_SECONDS = 5 * 60
_AK_CODE_TABLE_MAX_RETRIES = 3
_AK_CODE_TABLE_RETRY_DELAY_SECONDS = 1
_AK_CODE_TABLE_RETRY_COOLDOWN_SECONDS = 5 * 60
_db_name_cache: Dict[str, str] = {}
_db_name_cache_expire_at: float = 0.0
_db_name_cache_lock = threading.Lock()

result: Dict[str, Dict[str, str]] = {}
result_dict = result  # 兼容旧引用
_result_loaded = False
_result_next_retry_at: float = 0.0
_result_load_lock = threading.Lock()


def convert_code_format(code):
    """
    将股票代码转换为带交易所后缀的格式（000001 -> 000001.SZ）。
    """
    if code is None:
        return ""
    raw = str(code).strip()
    if not raw:
        return ""
    if "." in raw:
        num, exch = raw.split(".", 1)
        num = num.zfill(6)
        exch = exch.upper()
        if exch in ("SH", "SZ"):
            return f"{num}.{exch}"
        return f"{num}.SZ"
    if raw[:2].lower() in ("sh", "sz") and raw[2:].isdigit():
        num = raw[2:].zfill(6)
        exch = raw[:2].upper()
        return f"{num}.{exch}"
    if raw.isdigit():
        num = raw.zfill(6)
        return f"{num}.SH" if num.startswith("6") else f"{num}.SZ"
    return raw


def _code_candidates(stock_code: str) -> Set[str]:
    candidates: Set[str] = set()
    if stock_code is None:
        return candidates
    raw = str(stock_code).strip()
    if not raw:
        return candidates

    candidates.add(raw)
    candidates.add(raw.lower())
    candidates.add(raw.upper())

    ts_code = convert_code_format(raw)
    if ts_code:
        candidates.add(ts_code)
        candidates.add(ts_code.lower())
        candidates.add(ts_code.upper())

    if "." in ts_code:
        num, exch = ts_code.split(".", 1)
        candidates.add(num)
        candidates.add(f"{exch.lower()}{num}")

    return {item for item in candidates if item}


def _load_result_dict_once() -> None:
    global _result_loaded, _result_next_retry_at
    if _result_loaded:
        return
    if time.time() < _result_next_retry_at:
        return

    with _result_load_lock:
        if _result_loaded:
            return
        if _result_loaded:
            return
        if time.time() < _result_next_retry_at:
            return

        for attempt in range(1, _AK_CODE_TABLE_MAX_RETRIES + 1):
            try:
                result_df = ak.stock_info_a_code_name()
                records = result_df.to_dict(orient="records")
                loaded: Dict[str, Dict[str, str]] = {}
                for record in records:
                    original_code = str(record.get("code") or "").strip()
                    name = str(record.get("name") or "").strip()
                    if not original_code:
                        continue
                    ts_code = convert_code_format(original_code)
                    if not ts_code:
                        continue
                    loaded[ts_code] = {
                        "ts_code": ts_code,
                        "symbol": original_code,
                        "name": name,
                    }
                result.clear()
                result.update(loaded)
                _result_loaded = True
                _result_next_retry_at = 0.0
                logger.info("akshare 股票代码表加载成功，共 %d 条", len(loaded))
                return
            except Exception as exc:
                if attempt < _AK_CODE_TABLE_MAX_RETRIES:
                    logger.warning(
                        "加载 akshare 股票代码表失败（第 %d/%d 次）：%s；%d 秒后重试",
                        attempt,
                        _AK_CODE_TABLE_MAX_RETRIES,
                        exc,
                        _AK_CODE_TABLE_RETRY_DELAY_SECONDS,
                    )
                    time.sleep(_AK_CODE_TABLE_RETRY_DELAY_SECONDS)
                    continue

                _result_next_retry_at = time.time() + _AK_CODE_TABLE_RETRY_COOLDOWN_SECONDS
                logger.warning(
                    "加载 akshare 股票代码表失败，已重试 %d 次；将在 %d 秒后再试。错误：%s",
                    _AK_CODE_TABLE_MAX_RETRIES,
                    _AK_CODE_TABLE_RETRY_COOLDOWN_SECONDS,
                    exc,
                )
                logger.debug("akshare 股票代码表加载失败堆栈", exc_info=True)


def _refresh_db_name_cache(force: bool = False) -> Dict[str, str]:
    global _db_name_cache, _db_name_cache_expire_at
    now = time.time()
    if not force and now < _db_name_cache_expire_at and _db_name_cache:
        return _db_name_cache

    with _db_name_cache_lock:
        now = time.time()
        if not force and now < _db_name_cache_expire_at and _db_name_cache:
            return _db_name_cache

        loaded: Dict[str, str] = {}
        try:
            rows = exe_query("select stock_code, stock_name from stocks") or []
            for item in rows:
                code = str(item.get("stock_code") or "").strip()
                name = str(item.get("stock_name") or "").strip()
                if not code or not name:
                    continue
                for candidate in _code_candidates(code):
                    loaded[candidate] = name
        except Exception:
            logger.exception("刷新股票名称缓存失败")

        _db_name_cache = loaded
        _db_name_cache_expire_at = time.time() + _DB_NAME_CACHE_TTL_SECONDS
        return _db_name_cache


def _name_from_result_dict(stock_code: str) -> Optional[str]:
    _load_result_dict_once()
    for candidate in _code_candidates(stock_code):
        ts_code = convert_code_format(candidate)
        if not ts_code:
            continue
        meta = result.get(ts_code)
        if not meta:
            continue
        name = str(meta.get("name") or "").strip()
        if name:
            return name
    return None


def get_stock_name(stock_code):
    raw = str(stock_code).strip() if stock_code is not None else ""
    if not raw:
        return stock_code

    cache = _refresh_db_name_cache(force=False)
    for candidate in _code_candidates(raw):
        name = cache.get(candidate)
        if name:
            return name

    # 未命中时强制刷新一次，避免新增股票短时间查不到名称
    cache = _refresh_db_name_cache(force=True)
    for candidate in _code_candidates(raw):
        name = cache.get(candidate)
        if name:
            return name

    name = _name_from_result_dict(raw)
    return name or stock_code


# 延迟加载：仅在数据库未命中股票名称时再尝试拉取 akshare 代码表，
# 避免启动阶段因外部网络波动产生无关告警。