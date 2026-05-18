"""
腾讯 qt.gtimg.cn 实时行情（无 DB 依赖，供监控列表等模块使用）。
"""
from __future__ import annotations

import logging
import math
import os
import re
import time
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

_GTIMG_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://finance.qq.com/",
}

_IDX_PRICE = 3
_IDX_PRE_CLOSE = 4
_IDX_CHG_AMT = 31
_IDX_PCT = 32


def _disable_proxy_for_requests() -> None:
    os.environ["NO_PROXY"] = "*"
    os.environ["no_proxy"] = "*"
    for key in list(os.environ.keys()):
        if key.lower() in ("http_proxy", "https_proxy", "all_proxy"):
            os.environ.pop(key, None)


def _safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return None
        return number
    except (TypeError, ValueError):
        return None


def monitor_code_to_gtimg_symbol(stock_code: str, stock_name: str = "") -> Optional[str]:
    """监控股票代码 -> 腾讯行情符号（sh600519 / sz000001）。"""
    from monitor.config.stock_code import normalize_monitor_stock_code
    from utils.common import format_stock_code

    code = str(stock_code or "").strip()
    if not code:
        return None

    normalized = normalize_monitor_stock_code(code, stock_name)
    candidates: List[str] = []
    for item in (normalized, code):
        text = str(item or "").strip()
        if text and text not in candidates:
            candidates.append(text)

    for item in candidates:
        if item[:2].lower() in ("sh", "sz", "bj") and len(item) >= 8 and "." not in item:
            return item[:2].lower() + item[2:8]
        try:
            return format_stock_code(item, "prefix")
        except ValueError:
            continue
    return None


def _parse_gtimg_response(text: str) -> List[dict]:
    rows: List[dict] = []
    for match in re.finditer(r'v_(sh|sz|bj)(\d{6})="([^"]*)"', text, re.I):
        gtimg_symbol = f"{match.group(1).lower()}{match.group(2)}"
        parts = match.group(3).split("~")
        if len(parts) <= _IDX_PCT:
            continue

        price = _safe_float(parts[_IDX_PRICE]) if len(parts) > _IDX_PRICE else None
        pre_close = _safe_float(parts[_IDX_PRE_CLOSE]) if len(parts) > _IDX_PRE_CLOSE else None
        pct_chg = _safe_float(parts[_IDX_PCT]) if len(parts) > _IDX_PCT else None
        change_amount = _safe_float(parts[_IDX_CHG_AMT]) if len(parts) > _IDX_CHG_AMT else None

        if pct_chg is None and price is not None and pre_close not in (None, 0):
            pct_chg = ((price - pre_close) / pre_close) * 100
        if change_amount is None and price is not None and pre_close is not None:
            change_amount = price - pre_close

        rows.append(
            {
                "gtimg_symbol": gtimg_symbol,
                "price": price,
                "pre_close": pre_close,
                "pct_chg": pct_chg,
                "change_amount": change_amount,
            }
        )
    return rows


def _fetch_gtimg_symbols_once(
    session: requests.Session,
    symbols: List[str],
    timeout: int = 25,
) -> List[dict]:
    if not symbols:
        return []
    url = "https://qt.gtimg.cn/q=" + ",".join(symbols)
    response = session.get(url, headers=_GTIMG_HEADERS, timeout=timeout)
    response.raise_for_status()
    text = response.content.decode("gbk", errors="replace")
    return _parse_gtimg_response(text)


def fetch_gtimg_quotes(symbols: List[str], batch_size: int = 80) -> List[dict]:
    if not symbols:
        return []

    _disable_proxy_for_requests()
    dedup_symbols: List[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        key = str(symbol or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        dedup_symbols.append(key)

    all_rows: List[dict] = []
    session = requests.Session()
    session.trust_env = False

    for index in range(0, len(dedup_symbols), batch_size):
        batch = dedup_symbols[index : index + batch_size]
        last_error: Optional[Exception] = None
        for attempt in range(4):
            try:
                all_rows.extend(_fetch_gtimg_symbols_once(session, batch, timeout=25))
                last_error = None
                break
            except Exception as error:
                last_error = error
                time.sleep(0.6 * (2**attempt) + 0.1)
        if last_error is not None:
            logger.warning("腾讯行情批次失败 symbols=%s err=%s", batch[:3], last_error)
            for symbol in batch:
                try:
                    all_rows.extend(_fetch_gtimg_symbols_once(session, [symbol], timeout=12))
                except Exception as error:
                    logger.warning("腾讯行情单票失败 symbol=%s err=%s", symbol, error)
        time.sleep(0.05)

    quote_map: Dict[str, dict] = {}
    for item in all_rows:
        key = str(item.get("gtimg_symbol") or "").strip().lower()
        if key:
            quote_map[key] = item
    return list(quote_map.values())


def attach_intraday_quotes_to_stocks(stocks: List[dict]) -> List[dict]:
    if not stocks:
        return stocks

    symbol_by_stock: List[tuple[dict, str]] = []
    symbols: List[str] = []
    seen_symbols: set[str] = set()
    for stock in stocks:
        if not isinstance(stock, dict):
            continue
        gtimg_symbol = monitor_code_to_gtimg_symbol(
            stock.get("stock_code"),
            stock.get("stock_name"),
        )
        if not gtimg_symbol or gtimg_symbol in seen_symbols:
            symbol_by_stock.append((stock, gtimg_symbol or ""))
            continue
        seen_symbols.add(gtimg_symbol)
        symbols.append(gtimg_symbol)
        symbol_by_stock.append((stock, gtimg_symbol))

    quote_map: Dict[str, dict] = {}
    if symbols:
        try:
            quotes = fetch_gtimg_quotes(symbols)
            quote_map = {
                str(item.get("gtimg_symbol") or "").strip().lower(): item
                for item in quotes
                if item.get("gtimg_symbol")
            }
        except Exception as error:
            logger.exception("拉取监控股票当日行情失败: %s", error)

    matched = 0
    for stock, gtimg_symbol in symbol_by_stock:
        quote = quote_map.get(gtimg_symbol.lower()) if gtimg_symbol else None
        if quote:
            matched += 1
        stock["pct_chg"] = quote.get("pct_chg") if quote else None
        stock["price"] = quote.get("price") if quote else None

    if symbols and matched == 0:
        logger.warning(
            "监控股票行情未匹配任何标的 symbols=%s sample_codes=%s",
            len(symbols),
            [stock.get("stock_code") for stock, _ in symbol_by_stock[:3]],
        )
    return stocks
