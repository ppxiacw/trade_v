"""
市值 + 涨幅筛选：使用腾讯行情 qt.gtimg.cn（与前端日 K 的 fqkline 同属腾讯源），
批量拉取实时字段，避免东方财富分页接口易断连的问题。
"""
from __future__ import annotations

import logging
import math
import os
import re
import time
from datetime import datetime, timezone, timedelta
from typing import Any, List, Optional, Tuple

import requests

from config.dbconfig import exeQuery
from utils.common import format_stock_code

logger = logging.getLogger(__name__)

_GTIMG_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://finance.qq.com/",
}

# 经样本校验：腾讯 v_sh600519 波浪线分段，下标从 0 起
# 31: 涨跌额, 32: 涨跌幅(%), 44: 总市值(亿元)
_IDX_NAME = 1
_IDX_PRICE = 3
_IDX_CHG_AMT = 31
_IDX_PCT = 32
_IDX_TOTAL_MV_YI = 44
_IDX_FLOAT_MV_YI = 45


def _disable_proxy_for_requests() -> None:
    os.environ["NO_PROXY"] = "*"
    os.environ["no_proxy"] = "*"
    for key in list(os.environ.keys()):
        if key.lower() in ("http_proxy", "https_proxy", "all_proxy"):
            os.environ.pop(key, None)


def _safe_float(x: Any) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except (TypeError, ValueError):
        return None


def ts_to_gtimg_symbol(ts_code: str) -> Optional[str]:
    """600519.SH -> sh600519；920xxx.BJ -> bj920xxx"""
    ts_code = ts_code.strip().upper()
    if "." not in ts_code:
        return None
    num, suf = ts_code.split(".", 1)
    num = num.zfill(6)
    if suf == "SH":
        return f"sh{num}"
    if suf == "SZ":
        return f"sz{num}"
    if suf == "BJ":
        return f"bj{num}"
    return None


def _is_star_market_board(ts_code: str) -> bool:
    """科创板：证券代码 688 开头（沪市）。"""
    ts_code = ts_code.strip().upper()
    if "." not in ts_code:
        return False
    num, _ = ts_code.split(".", 1)
    return num.zfill(6).startswith("688")


def _prefix_to_ts_code(stock_code: str) -> Optional[str]:
    """
    sh600519/sz000001/600519 -> 600519.SH / 000001.SZ
    """
    if not stock_code:
        return None
    code = str(stock_code).strip().lower()
    if len(code) >= 8 and code[:2] in ("sh", "sz", "bj") and code[2:].isdigit():
        num = code[2:].zfill(6)
        return f"{num}.{code[:2].upper()}"
    if code.isdigit():
        num = code.zfill(6)
        if num.startswith("6"):
            return f"{num}.SH"
        return f"{num}.SZ"
    return None


def _load_universe_from_db() -> List[Tuple[str, str, str]]:
    rows: List[Tuple[str, str, str]] = []
    try:
        records = exeQuery("SELECT stock_code, stock_name FROM stocks")
        if not records:
            return rows
        for item in records:
            ts_code = _prefix_to_ts_code(item.get("stock_code"))
            if not ts_code or _is_star_market_board(ts_code):
                continue
            sym = ts_to_gtimg_symbol(ts_code)
            if not sym:
                continue
            rows.append((sym, ts_code, (item.get("stock_name") or "").strip()))
    except Exception as e:
        logger.warning("从数据库加载股票池失败: %s", e)
    return rows


def _load_universe() -> List[Tuple[str, str, str]]:
    """
    (gtimg_symbol, ts_code, name_from_list)
    """
    rows: List[Tuple[str, str, str]] = []
    try:
        from utils import GetStockData

        result_map = getattr(GetStockData, "result", {}) or {}
        for ts_code, info in result_map.items():
            if _is_star_market_board(ts_code):
                continue
            sym = ts_to_gtimg_symbol(ts_code)
            if sym:
                rows.append((sym, ts_code, (info or {}).get("name") or ""))
        if rows:
            return rows
        logger.warning("GetStockData.result 为空，尝试回退到数据库 stocks 表")
    except Exception as e:
        logger.warning("加载全市场代码表失败: %s", e)
    return _load_universe_from_db()


def _parse_gtimg_response(text: str) -> List[dict]:
    """解析 qt.gtimg.cn 返回的 v_sh600519=\"...\" 片段。"""
    out: List[dict] = []
    for m in re.finditer(r"v_(sh|sz|bj)(\d{6})=\"([^\"]*)\"", text, re.I):
        gsym = f"{m.group(1).lower()}{m.group(2)}"
        payload = m.group(3)
        parts = payload.split("~")
        if len(parts) <= _IDX_TOTAL_MV_YI:
            continue
        name = parts[_IDX_NAME] if len(parts) > _IDX_NAME else ""
        price = _safe_float(parts[_IDX_PRICE]) if len(parts) > _IDX_PRICE else None
        chg_amt = _safe_float(parts[_IDX_CHG_AMT]) if len(parts) > _IDX_CHG_AMT else None
        pct = _safe_float(parts[_IDX_PCT]) if len(parts) > _IDX_PCT else None
        mv_yi = _safe_float(parts[_IDX_TOTAL_MV_YI])
        float_mv_yi = (
            _safe_float(parts[_IDX_FLOAT_MV_YI])
            if len(parts) > _IDX_FLOAT_MV_YI
            else None
        )
        out.append(
            {
                "gtimg_symbol": gsym,
                "name_raw": name,
                "price": price,
                "pct_chg": pct,
                "change_amount": chg_amt,
                "total_mv_yi": mv_yi,
                "float_mv_yi": float_mv_yi,
            }
        )
    return out


def _fetch_gtimg_batches(symbols: List[str], batch_size: int = 80) -> List[dict]:
    _disable_proxy_for_requests()
    all_rows: List[dict] = []
    session = requests.Session()
    session.trust_env = False

    for i in range(0, len(symbols), batch_size):
        batch = symbols[i : i + batch_size]
        url = "https://qt.gtimg.cn/q=" + ",".join(batch)
        last_err: Optional[Exception] = None
        for attempt in range(4):
            try:
                r = session.get(url, headers=_GTIMG_HEADERS, timeout=25)
                r.raise_for_status()
                text = r.content.decode("gbk", errors="replace")
                all_rows.extend(_parse_gtimg_response(text))
                break
            except Exception as e:
                last_err = e
                time.sleep(0.6 * (2**attempt) + 0.1)
        else:
            logger.warning("腾讯行情批次失败 symbols=%s.. err=%s", batch[:3], last_err)
        time.sleep(0.06)
    return all_rows


def screen_stocks_by_mv_and_pct(
    min_mv_yi: float,
    min_pct_chg: float,
    limit: int = 3000,
) -> Tuple[List[dict], dict]:
    universe = _load_universe()
    if not universe:
        raise RuntimeError(
            "本地股票代码表为空（utils/GetStockData 初始化失败）。"
            "请确认本机能访问 akshare 的 stock_info_a_code_name 或检查启动日志。"
        )

    sym_to_meta = {u[0]: (u[1], u[2]) for u in universe}
    symbols = list(sym_to_meta.keys())

    tz_cn = timezone(timedelta(hours=8))
    fetched_at = datetime.now(tz_cn).isoformat(timespec="seconds")

    quotes = _fetch_gtimg_batches(symbols)
    if not quotes:
        raise RuntimeError("腾讯行情接口未返回有效数据，请检查网络或稍后重试。")

    rows: List[dict] = []
    for q in quotes:
        gsym = q.get("gtimg_symbol")
        if not gsym or gsym not in sym_to_meta:
            continue
        ts_code, name_hint = sym_to_meta[gsym]
        if _is_star_market_board(ts_code):
            continue
        pure = ts_code.split(".")[0].zfill(6)
        try:
            stock_code = format_stock_code(pure, "prefix")
        except ValueError:
            stock_code = gsym

        mv_yi = q.get("total_mv_yi")
        pct = q.get("pct_chg")
        if mv_yi is None or pct is None:
            continue
        if mv_yi < float(min_mv_yi) or pct < float(min_pct_chg):
            continue

        nm = (q.get("name_raw") or "").strip() or name_hint
        fmv = q.get("float_mv_yi")
        float_mv_yi = round(fmv, 4) if fmv is not None else None

        rows.append(
            {
                "stock_code": stock_code,
                "stock_name": nm,
                "price": q.get("price"),
                "pct_chg": pct,
                "change_amount": q.get("change_amount"),
                "total_mv_yi": round(mv_yi, 4),
                "float_mv_yi": float_mv_yi,
                "turnover_rate": None,
                "volume": None,
                "amount": None,
            }
        )

    rows.sort(key=lambda x: (x["pct_chg"] or 0), reverse=True)
    if limit and limit > 0:
        rows = rows[: int(limit)]

    meta = {
        "fetched_at": fetched_at,
        "source": "qt.gtimg.cn (Tencent, same vendor family as K-line fqkline)",
        "universe_size": len(symbols),
        "quotes_parsed": len(quotes),
        "total_after_filter": len(rows),
        "exclude_star_board": True,
        "exclude_note": "已排除科创板（代码 688 开头）",
    }
    return rows, meta
