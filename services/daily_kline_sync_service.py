"""
全市场日K数据入库与每日增量补齐服务。

能力：
1) 手动触发全量历史同步（首次建库）。
2) 手动/定时触发增量同步（收盘后补齐最新交易日）。
"""
from __future__ import annotations

import logging
import threading
import time
import os
import json
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import akshare as ak
import pandas as pd
import requests

from monitor.config.db_monitor import db_manager
from config.dbconfig import exeQuery
from utils.common import format_stock_code

logger = logging.getLogger(__name__)

_TABLE_NAME = "stock_daily_kline"
_BATCH_SIZE = 500
_MAX_RETRY_PER_STOCK = 3
_RETRY_BACKOFF_BASE_SECONDS = 1.2
_REQUEST_GAP_SECONDS = 0.12
_SECOND_PASS_RETRY_ENABLED = True
_PROXY_ENV_KEYS = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")
_TENCENT_KLINE_COUNT = 1500
_RETENTION_START_YYYYMMDD = "20240501"
_RETENTION_START_DATE = date(2024, 5, 1)

_status_lock = threading.Lock()
_sync_lock = threading.Lock()
_scheduler_lock = threading.Lock()
_scheduler = None
_scheduler_started = False

_sync_status: Dict[str, object] = {
    "running": False,
    "mode": "",
    "trigger": "",
    "total_stocks": 0,
    "processed_stocks": 0,
    "success_stocks": 0,
    "failed_stocks": 0,
    "current_stock": "",
    "last_run_started_at": None,
    "last_run_finished_at": None,
    "last_error": "",
    "retry_round": 0,
}


def _disable_proxy_env() -> None:
    os.environ["NO_PROXY"] = "*"
    os.environ["no_proxy"] = "*"
    for key in _PROXY_ENV_KEYS:
        if key in os.environ:
            os.environ.pop(key, None)


def _default_full_start_date() -> str:
    # 默认仅导入近3年历史数据
    candidate = (datetime.now() - timedelta(days=365 * 3)).date()
    return max(candidate, _RETENTION_START_DATE).strftime("%Y%m%d")


def _clamp_start_date(start_date: Optional[str]) -> str:
    """
    将开始日期限制在保留窗口内（不早于 2024-05-01）。
    """
    if not start_date:
        return _RETENTION_START_YYYYMMDD
    raw = str(start_date).strip()
    if not raw:
        return _RETENTION_START_YYYYMMDD
    dt = datetime.strptime(raw, "%Y%m%d").date()
    return max(dt, _RETENTION_START_DATE).strftime("%Y%m%d")


def _normalize_ts_code(raw_code: str) -> Optional[str]:
    if not raw_code:
        return None
    code = str(raw_code).strip()
    if not code:
        return None

    try:
        suffix = format_stock_code(code, "suffix")  # 000001.sz
    except Exception:
        suffix = None

    if suffix and "." in suffix:
        num, ex = suffix.split(".", 1)
        ex = ex.upper()
        if ex in ("SH", "SZ", "BJ"):
            return f"{num.zfill(6)}.{ex}"

    # 兜底：按数字前缀判断市场
    digits = "".join(ch for ch in code if ch.isdigit())
    if not digits:
        return None
    digits = digits.zfill(6)[-6:]
    ex = "SH" if digits.startswith("6") else "SZ"
    return f"{digits}.{ex}"


def _ts_code_to_tencent_symbol(ts_code: str) -> Optional[str]:
    code = str(ts_code or "").strip().upper()
    if "." not in code:
        return None
    pure, exch = code.split(".", 1)
    pure = pure.zfill(6)
    if exch == "SH":
        return f"sh{pure}"
    if exch == "SZ":
        return f"sz{pure}"
    return None


def _get_stock_universe() -> List[Tuple[str, str]]:
    """
    返回 [(ts_code, stock_name), ...]。
    优先复用 stocks 表（与前端股票列表一致）；空时回退 akshare 股票基础信息。
    """
    rows: List[Tuple[str, str]] = []
    seen = set()

    records = exeQuery("SELECT stock_code, stock_name FROM stocks") or []
    for item in records:
        ts_code = _normalize_ts_code(item.get("stock_code"))
        name = (item.get("stock_name") or "").strip()
        if not ts_code or ts_code in seen:
            continue
        # 过滤明显指数代码，避免混入监控里手工加的指数
        if ts_code.startswith("399") or ("指数" in name):
            continue
        seen.add(ts_code)
        rows.append((ts_code, name))

    if rows:
        return rows

    logger.warning("stocks 表为空，回退使用 akshare 股票基础列表")
    try:
        df = ak.stock_info_a_code_name()
        for _, r in df.iterrows():
            pure_code = str(r.get("code", "")).strip()
            name = str(r.get("name", "")).strip()
            if not pure_code:
                continue
            ts_code = _normalize_ts_code(pure_code)
            if not ts_code or ts_code in seen:
                continue
            seen.add(ts_code)
            rows.append((ts_code, name))
    except Exception as e:
        logger.exception("加载股票基础列表失败: %s", e)

    return rows


def _ensure_table_once() -> None:
    conn = None
    cursor = None
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{_TABLE_NAME}` (
                  `id` BIGINT NOT NULL AUTO_INCREMENT,
                  `ts_code` VARCHAR(16) NOT NULL COMMENT '股票代码(000001.SZ)',
                  `trade_date` DATE NOT NULL COMMENT '交易日期',
                  `open` DECIMAL(16,4) NULL,
                  `high` DECIMAL(16,4) NULL,
                  `low` DECIMAL(16,4) NULL,
                  `close` DECIMAL(16,4) NULL,
                  `pre_close` DECIMAL(16,4) NULL,
                  `change_amount` DECIMAL(16,4) NULL,
                  `pct_chg` DECIMAL(10,4) NULL,
                  `vol` BIGINT NULL,
                  `amount` DECIMAL(20,2) NULL,
                  `turnover_rate` DECIMAL(10,4) NULL,
                  `amplitude` DECIMAL(10,4) NULL,
                  `source` VARCHAR(32) NOT NULL DEFAULT 'akshare_qfq',
                  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (`id`),
                  UNIQUE KEY `uk_ts_trade_date` (`ts_code`, `trade_date`),
                  KEY `idx_trade_date` (`trade_date`),
                  KEY `idx_ts_code` (`ts_code`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """
            )
    finally:
        if cursor:
            cursor.close()


def _set_status(**kwargs) -> None:
    with _status_lock:
        _sync_status.update(kwargs)


def get_daily_kline_sync_status() -> Dict[str, object]:
    with _status_lock:
        snapshot = dict(_sync_status)
    snapshot["table_name"] = _TABLE_NAME
    snapshot["scheduler_started"] = _scheduler_started
    return snapshot


def _fetch_single_stock_daily(ts_code: str, start_date: Optional[str]) -> pd.DataFrame:
    _disable_proxy_env()
    try:
        df = _fetch_single_stock_daily_from_tx_akshare(ts_code=ts_code, start_date=start_date)
        if df is not None and not df.empty:
            return df
    except Exception as e:
        logger.warning("akshare腾讯日K失败，改用腾讯直连回退 %s: %s", ts_code, e)

    try:
        df = _fetch_single_stock_daily_from_tencent(ts_code=ts_code, start_date=start_date)
        if df is not None and not df.empty:
            return df
    except Exception as e:
        logger.warning("腾讯直连日K失败，改用原akshare回退 %s: %s", ts_code, e)

    return _fetch_single_stock_daily_from_akshare_hist(ts_code=ts_code, start_date=start_date)


def _fetch_single_stock_daily_from_tx_akshare(ts_code: str, start_date: Optional[str]) -> pd.DataFrame:
    symbol = _ts_code_to_tencent_symbol(ts_code)
    if not symbol:
        return pd.DataFrame()

    query_start = start_date or "19000101"
    df = ak.stock_zh_a_hist_tx(
        symbol=symbol,
        start_date=query_start,
        end_date="20500101",
        adjust="qfq",
    )
    if df is None or df.empty:
        return pd.DataFrame()

    # stock_zh_a_hist_tx: date/open/close/high/low/amount
    rename_map = {
        "date": "trade_date",
        "open": "open",
        "close": "close",
        "high": "high",
        "low": "low",
        "amount": "vol",
    }
    df = df.rename(columns=rename_map).copy()
    for col in ("trade_date", "open", "high", "low", "close"):
        if col not in df.columns:
            return pd.DataFrame()

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df = df.dropna(subset=["trade_date"]).copy()
    if df.empty:
        return df

    for col in ("open", "close", "high", "low", "vol"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open", "close", "high", "low"]).copy()
    if df.empty:
        return df

    df = df.sort_values("trade_date").reset_index(drop=True)
    df["pre_close"] = df["close"].shift(1)
    first_idx = df.index.min()
    if pd.isna(df.loc[first_idx, "pre_close"]):
        df.loc[first_idx, "pre_close"] = df.loc[first_idx, "open"]
    df["change_amount"] = df["close"] - df["pre_close"]
    df["pct_chg"] = (df["change_amount"] / df["pre_close"]) * 100
    df["amplitude"] = ((df["high"] - df["low"]) / df["pre_close"]) * 100
    df["amount"] = None
    df["turnover_rate"] = None
    df["trade_date"] = df["trade_date"].dt.date
    df["ts_code"] = ts_code
    df["source"] = "akshare_tx_qfq"
    return df[
        [
            "ts_code",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "change_amount",
            "pct_chg",
            "vol",
            "amount",
            "turnover_rate",
            "amplitude",
            "source",
        ]
    ]


def _fetch_single_stock_daily_from_akshare_hist(ts_code: str, start_date: Optional[str]) -> pd.DataFrame:
    pure_code = ts_code.split(".", 1)[0]
    kwargs = {
        "symbol": pure_code,
        "period": "daily",
        "adjust": "qfq",
    }
    if start_date:
        kwargs["start_date"] = start_date

    df = ak.stock_zh_a_hist(**kwargs)
    if df is None or df.empty:
        return pd.DataFrame()

    rename_map = {
        "日期": "trade_date",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "收盘": "close",
        "成交量": "vol",
        "成交额": "amount",
        "振幅": "amplitude",
        "涨跌幅": "pct_chg",
        "涨跌额": "change_amount",
        "换手率": "turnover_rate",
    }
    df = df.rename(columns=rename_map)
    for col in ("trade_date", "open", "high", "low", "close"):
        if col not in df.columns:
            return pd.DataFrame()

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df = df.dropna(subset=["trade_date"]).copy()
    if df.empty:
        return df

    df["trade_date"] = df["trade_date"].dt.date
    df["ts_code"] = ts_code
    df["pre_close"] = df["close"] - df.get("change_amount", 0)
    df["source"] = "akshare_qfq"
    return df[
        [
            "ts_code",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "change_amount",
            "pct_chg",
            "vol",
            "amount",
            "turnover_rate",
            "amplitude",
            "source",
        ]
    ]


def _fetch_single_stock_daily_from_tencent(ts_code: str, start_date: Optional[str]) -> pd.DataFrame:
    symbol = _ts_code_to_tencent_symbol(ts_code)
    if not symbol:
        return pd.DataFrame()

    _disable_proxy_env()
    session = requests.Session()
    session.trust_env = False
    url = (
        "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
        f"?param={symbol},day,,,{_TENCENT_KLINE_COUNT},qfq&_var=kline_day&r={time.time()}"
    )
    resp = session.get(url, timeout=20)
    resp.raise_for_status()
    text = resp.text
    payload = text.split("=", 1)[-1].strip().rstrip(";")
    data = json.loads(payload)
    if int(data.get("code", -1)) != 0:
        raise RuntimeError(f"腾讯日K返回异常: {data.get('msg')}")

    kline_root = (data.get("data") or {}).get(symbol) or {}
    rows = kline_root.get("qfqday") or kline_root.get("day") or []
    if not rows:
        return pd.DataFrame()

    records = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 6:
            continue
        # 腾讯 qfqday: [日期, 开, 收, 高, 低, 量, ...]
        records.append(
            {
                "trade_date": row[0],
                "open": row[1],
                "close": row[2],
                "high": row[3],
                "low": row[4],
                "vol": row[5],
            }
        )
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df = df.dropna(subset=["trade_date"]).copy()
    if df.empty:
        return df

    if start_date:
        try:
            start_dt = datetime.strptime(start_date, "%Y%m%d")
            df = df[df["trade_date"] >= start_dt]
        except Exception:
            pass

    for col in ("open", "close", "high", "low", "vol"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open", "close", "high", "low"]).copy()
    if df.empty:
        return df

    df = df.sort_values("trade_date").reset_index(drop=True)
    df["pre_close"] = df["close"].shift(1)
    first_idx = df.index.min()
    if pd.isna(df.loc[first_idx, "pre_close"]):
        df.loc[first_idx, "pre_close"] = df.loc[first_idx, "open"]
    df["change_amount"] = df["close"] - df["pre_close"]
    df["pct_chg"] = (df["change_amount"] / df["pre_close"]) * 100
    df["amount"] = None
    df["turnover_rate"] = None
    df["amplitude"] = ((df["high"] - df["low"]) / df["pre_close"]) * 100
    df["trade_date"] = df["trade_date"].dt.date
    df["ts_code"] = ts_code
    df["source"] = "tencent_qfq"
    return df[
        [
            "ts_code",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "change_amount",
            "pct_chg",
            "vol",
            "amount",
            "turnover_rate",
            "amplitude",
            "source",
        ]
    ]


def _sync_single_stock_once(ts_code: str, start_date: str) -> int:
    df = _fetch_single_stock_daily(ts_code=ts_code, start_date=start_date)
    return _upsert_daily_rows(df)


def _sync_single_stock_with_retry(
    ts_code: str,
    start_date: str,
    max_retries: int = _MAX_RETRY_PER_STOCK,
) -> Tuple[bool, Optional[Exception]]:
    """
    单只股票同步带重试。
    返回: (是否成功, 最后一次异常)
    """
    last_error: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            _sync_single_stock_once(ts_code=ts_code, start_date=start_date)
            return True, None
        except Exception as e:
            last_error = e
            if attempt >= max_retries:
                break
            sleep_seconds = _RETRY_BACKOFF_BASE_SECONDS * attempt
            time.sleep(sleep_seconds)
    return False, last_error


def _upsert_daily_rows(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0

    rows = []
    for _, row in df.iterrows():
        rows.append(
            (
                row.get("ts_code"),
                row.get("trade_date"),
                row.get("open"),
                row.get("high"),
                row.get("low"),
                row.get("close"),
                row.get("pre_close"),
                row.get("change_amount"),
                row.get("pct_chg"),
                row.get("vol"),
                row.get("amount"),
                row.get("turnover_rate"),
                row.get("amplitude"),
                row.get("source"),
            )
        )

    sql = f"""
    INSERT INTO `{_TABLE_NAME}` (
      ts_code, trade_date, open, high, low, close,
      pre_close, change_amount, pct_chg, vol, amount,
      turnover_rate, amplitude, source
    ) VALUES (
      %s, %s, %s, %s, %s, %s,
      %s, %s, %s, %s, %s,
      %s, %s, %s
    )
    ON DUPLICATE KEY UPDATE
      open = VALUES(open),
      high = VALUES(high),
      low = VALUES(low),
      close = VALUES(close),
      pre_close = VALUES(pre_close),
      change_amount = VALUES(change_amount),
      pct_chg = VALUES(pct_chg),
      vol = VALUES(vol),
      amount = VALUES(amount),
      turnover_rate = VALUES(turnover_rate),
      amplitude = VALUES(amplitude),
      source = VALUES(source),
      updated_at = CURRENT_TIMESTAMP
    """

    affected = 0
    for i in range(0, len(rows), _BATCH_SIZE):
        chunk = rows[i : i + _BATCH_SIZE]
        affected += db_manager.execute_many(sql, chunk)
    return affected


def _load_latest_trade_date_map() -> Dict[str, datetime.date]:
    m: Dict[str, datetime.date] = {}
    rows = db_manager.execute_query(
        f"SELECT ts_code, MAX(trade_date) AS max_trade_date FROM `{_TABLE_NAME}` GROUP BY ts_code"
    )
    for r in rows or []:
        ts_code = r.get("ts_code")
        d = r.get("max_trade_date")
        if ts_code and d:
            m[ts_code] = d
    return m


def _next_start_date(latest_date) -> str:
    if not latest_date:
        return _RETENTION_START_YYYYMMDD
    return max(latest_date + timedelta(days=1), _RETENTION_START_DATE).strftime("%Y%m%d")


def _cleanup_outdated_rows() -> int:
    return db_manager.execute_delete(_TABLE_NAME, f"trade_date < '{_RETENTION_START_DATE.isoformat()}'")


def _run_sync(mode: str, trigger: str, full_start_date: Optional[str] = None) -> None:
    with _sync_lock:
        _set_status(
            running=True,
            mode=mode,
            trigger=trigger,
            total_stocks=0,
            processed_stocks=0,
            success_stocks=0,
            failed_stocks=0,
            current_stock="",
            last_run_started_at=datetime.now().isoformat(timespec="seconds"),
            last_run_finished_at=None,
            last_error="",
            retry_round=1,
        )
        try:
            _ensure_table_once()
            deleted_rows = _cleanup_outdated_rows()
            if deleted_rows > 0:
                logger.info("已清理 %s 条过期日K（早于 %s）", deleted_rows, _RETENTION_START_DATE.isoformat())
            universe = _get_stock_universe()
            _set_status(total_stocks=len(universe))

            latest_map = {} if mode == "full" else _load_latest_trade_date_map()
            failed_items: List[Tuple[str, str, str]] = []
            for idx, (ts_code, stock_name) in enumerate(universe, start=1):
                _set_status(processed_stocks=idx, current_stock=f"{ts_code} {stock_name}")
                if mode == "full":
                    start_date = _clamp_start_date(full_start_date)
                else:
                    start_date = _next_start_date(latest_map.get(ts_code))

                ok, err = _sync_single_stock_with_retry(ts_code=ts_code, start_date=start_date)
                if ok:
                    with _status_lock:
                        _sync_status["success_stocks"] = int(_sync_status["success_stocks"]) + 1
                else:
                    logger.warning("首轮同步失败 %s: %s", ts_code, err)
                    failed_items.append((ts_code, stock_name, start_date))
                    with _status_lock:
                        _sync_status["failed_stocks"] = int(_sync_status["failed_stocks"]) + 1
                        _sync_status["last_error"] = f"{ts_code}: {err}"

                time.sleep(_REQUEST_GAP_SECONDS)

            # 失败回补：再扫一轮，处理网络抖动/源端限流导致的临时失败
            if _SECOND_PASS_RETRY_ENABLED and failed_items:
                _set_status(retry_round=2, failed_stocks=0)
                for idx, (ts_code, stock_name, start_date) in enumerate(failed_items, start=1):
                    _set_status(current_stock=f"[回补 {idx}/{len(failed_items)}] {ts_code} {stock_name}")
                    ok, err = _sync_single_stock_with_retry(
                        ts_code=ts_code,
                        start_date=start_date,
                        max_retries=2,
                    )
                    if ok:
                        with _status_lock:
                            _sync_status["success_stocks"] = int(_sync_status["success_stocks"]) + 1
                    else:
                        logger.warning("回补仍失败 %s: %s", ts_code, err)
                        with _status_lock:
                            _sync_status["failed_stocks"] = int(_sync_status["failed_stocks"]) + 1
                            _sync_status["last_error"] = f"{ts_code}: {err}"
                    time.sleep(_REQUEST_GAP_SECONDS)
        except Exception as e:
            logger.exception("日K同步任务失败: %s", e)
            _set_status(last_error=str(e))
        finally:
            _set_status(
                running=False,
                current_stock="",
                retry_round=0,
                last_run_finished_at=datetime.now().isoformat(timespec="seconds"),
            )


def _start_background_sync(mode: str, trigger: str, full_start_date: Optional[str] = None) -> Tuple[bool, str]:
    with _status_lock:
        if _sync_status.get("running"):
            return False, "已有日K同步任务正在执行"

    worker = threading.Thread(
        target=_run_sync,
        kwargs={"mode": mode, "trigger": trigger, "full_start_date": full_start_date},
        daemon=True,
    )
    worker.start()
    return True, "任务已启动"


def start_daily_kline_full_sync(trigger: str = "manual", start_date: Optional[str] = None) -> Tuple[bool, str]:
    return _start_background_sync(
        mode="full",
        trigger=trigger,
        full_start_date=_clamp_start_date(start_date or _default_full_start_date()),
    )


def start_daily_kline_incremental_sync(trigger: str = "manual") -> Tuple[bool, str]:
    return _start_background_sync(mode="incremental", trigger=trigger)


def start_daily_kline_scheduler() -> None:
    global _scheduler, _scheduler_started
    with _scheduler_lock:
        if _scheduler_started:
            return
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
        except Exception as e:
            logger.warning("未安装 APScheduler，跳过日K定时任务: %s", e)
            return

        _scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
        _scheduler.add_job(
            lambda: start_daily_kline_incremental_sync(trigger="scheduler"),
            trigger="cron",
            id="daily_kline_incremental_after_close",
            day_of_week="mon-fri",
            hour=15,
            minute=35,
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        _scheduler.start()
        _scheduler_started = True
        logger.info("日K增量补齐定时任务已启动：每个交易日 15:35")
