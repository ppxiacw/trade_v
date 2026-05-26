"""
分钟/分时前复权：腾讯 mkline 无 qfq，用日线 bfq vs qfq 收盘比值按交易日缩放 OHLC。
"""
from __future__ import annotations

import json
import re
import time
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence

import requests

_JSONP_TAIL_RE = re.compile(r'=\s*(\{.*\})\s*;?\s*$', re.DOTALL)
_FACTOR_CACHE: Dict[str, Dict[str, Any]] = {}
_FACTOR_CACHE_TTL_SECONDS = 300


def _parse_jsonp_payload(text: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(text)
    except Exception:
        pass
    match = _JSONP_TAIL_RE.search(str(text or ''))
    if not match:
        return None
    try:
        return json.loads(match.group(1))
    except Exception:
        return None


def kline_time_to_date_key(time_str: Any) -> str:
    text = str(time_str or '').strip()
    if not text:
        return ''
    if '-' in text:
        return text[:10]
    if len(text) >= 8:
        return f'{text[:4]}-{text[4:6]}-{text[6:8]}'
    return ''


def _ingest_daily_closes(rows: Any, target: MutableMapping[str, float]) -> None:
    if not isinstance(rows, list):
        return
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 3:
            continue
        date_key = kline_time_to_date_key(row[0])
        try:
            close_value = float(row[2])
        except (TypeError, ValueError):
            continue
        if not date_key or close_value <= 0:
            continue
        target[date_key] = close_value


def build_forward_factor_map(raw_rows: Any, qfq_rows: Any) -> Dict[str, float]:
    raw_map: Dict[str, float] = {}
    qfq_map: Dict[str, float] = {}
    _ingest_daily_closes(raw_rows, raw_map)
    _ingest_daily_closes(qfq_rows, qfq_map)

    factors: Dict[str, float] = {}
    for date_key, qfq_close in qfq_map.items():
        raw_close = raw_map.get(date_key)
        if not raw_close or raw_close <= 0:
            continue
        factor = qfq_close / raw_close
        if factor > 0:
            factors[date_key] = factor
    return factors


def fetch_daily_forward_factors(
    formatted_code: str,
    bar_count: int = 400,
    *,
    session: Optional[requests.Session] = None,
) -> Dict[str, float]:
    cache_key = f'{formatted_code}|{bar_count}'
    cached = _FACTOR_CACHE.get(cache_key)
    now = time.time()
    if cached and now - float(cached.get('ts', 0)) < _FACTOR_CACHE_TTL_SECONDS:
        return dict(cached.get('factors') or {})

    count = max(60, min(2000, int(bar_count or 400)))
    client = session or requests
    raw_url = (
        'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?'
        f'param={formatted_code},day,,,{count},bfq&_var=kline_day_raw&r={now}'
    )
    qfq_url = (
        'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?'
        f'param={formatted_code},day,,,{count},qfq&_var=kline_day_qfq&r={now}'
    )

    raw_payload = _parse_jsonp_payload(client.get(raw_url, timeout=8).text)
    qfq_payload = _parse_jsonp_payload(client.get(qfq_url, timeout=8).text)
    if not raw_payload or raw_payload.get('code') != 0:
        return {}
    if not qfq_payload or qfq_payload.get('code') != 0:
        return {}

    stock_raw = (raw_payload.get('data') or {}).get(formatted_code) or {}
    stock_qfq = (qfq_payload.get('data') or {}).get(formatted_code) or {}
    factors = build_forward_factor_map(stock_raw.get('day'), stock_qfq.get('qfqday'))
    _FACTOR_CACHE[cache_key] = {'ts': now, 'factors': factors}
    return factors


def _scale_price(value: Any, factor: float) -> Any:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return value
    if not number:
        return value
    return round(number * factor, 4)


def apply_forward_adjust_to_kline_rows(
    rows: Sequence[Any],
    factors: Mapping[str, float],
) -> List[Any]:
    if not rows or not factors:
        return list(rows)

    adjusted: List[Any] = []
    for item in rows:
        if not isinstance(item, (list, tuple)) or len(item) < 5:
            adjusted.append(item)
            continue
        date_key = kline_time_to_date_key(item[0])
        factor = factors.get(date_key)
        if not factor or abs(factor - 1.0) < 1e-6:
            adjusted.append(item)
            continue
        next_row = list(item)
        next_row[1] = _scale_price(item[1], factor)
        next_row[2] = _scale_price(item[2], factor)
        next_row[3] = _scale_price(item[3], factor)
        next_row[4] = _scale_price(item[4], factor)
        if len(next_row) > 7:
            try:
                float(next_row[7])
                next_row[7] = _scale_price(item[7], factor)
            except (TypeError, ValueError):
                pass
        adjusted.append(next_row)
    return adjusted


def apply_forward_adjust_to_quote_rows(
    rows: Sequence[Mapping[str, Any]],
    factors: Mapping[str, float],
) -> List[Dict[str, Any]]:
    if not rows or not factors:
        return [dict(item) for item in rows]

    adjusted: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        date_key = kline_time_to_date_key(item.get('time'))
        factor = factors.get(date_key or '')
        if not factor or abs(factor - 1.0) < 1e-6:
            adjusted.append(item)
            continue
        for key in ('open', 'close', 'high', 'low'):
            if key not in item:
                continue
            item[key] = _scale_price(item[key], factor)
        adjusted.append(item)
    return adjusted


def adjust_reference_price(
    price: Any,
    date_key: str,
    factors: Mapping[str, float],
    *,
    use_previous_day: bool = True,
) -> Any:
    try:
        number = float(price)
    except (TypeError, ValueError):
        return price
    keys = sorted(factors.keys())
    if not keys:
        return number
    if use_previous_day:
        if date_key in keys:
            index = keys.index(date_key)
            lookup = keys[index - 1] if index > 0 else date_key
        else:
            lookup = keys[-1]
    else:
        lookup = date_key
    factor = factors.get(lookup)
    if not factor:
        return number
    return round(number * factor, 4)
