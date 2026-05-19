"""
背离检测服务：与监控告警共用同一套 K 线拉取与 detect_divergence 逻辑。
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

import requests

from utils.divergence_detect import calculate_macd_hist_series, detect_divergence

_JSONP_TAIL_RE = re.compile(r'=\s*(\{.*\})\s*;?\s*$', re.DOTALL)


def normalize_stock_code_for_kline(stock_code: str) -> str:
    text = str(stock_code or '').strip()
    if not text:
        return text

    suffix_match = re.fullmatch(r'([0-9]{6})\.(sh|sz)', text, flags=re.IGNORECASE)
    if suffix_match:
        return f"{suffix_match.group(2).lower()}{suffix_match.group(1)}"

    prefix_match = re.fullmatch(r'(sh|sz)([0-9]{6})', text, flags=re.IGNORECASE)
    if prefix_match:
        return f"{prefix_match.group(1).lower()}{prefix_match.group(2)}"

    pure_match = re.fullmatch(r'([0-9]{1,6})', text)
    if pure_match:
        pure = pure_match.group(1).zfill(6)
        exchange = 'sh' if pure.startswith('6') else 'sz'
        return f"{exchange}{pure}"

    return text.lower()


def parse_jsonp_payload(text: str) -> Optional[Dict[str, Any]]:
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


def fetch_kline_rows(stock_code: str, period: str, count: int) -> List[Dict[str, Any]]:
    formatted_code = normalize_stock_code_for_kline(stock_code)
    if not formatted_code:
        return []

    period_key = str(period or '').strip().lower()
    bar_count = max(60, int(count or 240))

    if period_key in {'day', 'week', 'month'}:
        url = (
            f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?"
            f"param={formatted_code},{period_key},,,{bar_count},qfq&_var=kline_{period_key}"
        )
        kline_key = f"qfq{period_key}"
    else:
        url = (
            f"https://ifzq.gtimg.cn/appstock/app/kline/mkline?"
            f"param={formatted_code},{period_key},,{bar_count}&_var=kline_{period_key}"
        )
        kline_key = period_key

    try:
        response = requests.get(url, timeout=8)
        payload = parse_jsonp_payload(response.text)
        if not payload or payload.get('code') != 0:
            return []

        stock_data = payload.get('data', {}).get(formatted_code) or {}
        raw_rows = stock_data.get(kline_key) or stock_data.get(period_key) or []
        rows: List[Dict[str, Any]] = []
        for item in raw_rows:
            if not isinstance(item, (list, tuple)) or len(item) < 5:
                continue
            try:
                close_value = float(item[2])
                high_value = float(item[3])
                low_value = float(item[4])
            except (TypeError, ValueError):
                continue
            rows.append({
                'time': str(item[0]),
                'close': close_value,
                'high': high_value,
                'low': low_value,
            })
        return rows
    except Exception:
        return []


def _attach_time_to_points(
    kline_rows: List[Dict[str, Any]],
    divergence: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, List[Dict[str, Any]]]:
    out_top: List[Dict[str, Any]] = []
    out_bottom: List[Dict[str, Any]] = []

    for side, target in (('top', out_top), ('bottom', out_bottom)):
        for point in divergence.get(side) or []:
            idx = int(point.get('index', -1))
            if idx < 0 or idx >= len(kline_rows):
                continue
            row = kline_rows[idx]
            target.append({
                'index': idx,
                'time': row.get('time'),
                'priceValue': point.get('priceValue'),
                'indicatorValue': point.get('indicatorValue'),
            })

    return {'top': out_top, 'bottom': out_bottom}


def compute_divergence_points(
    stock_code: str,
    period: str,
    *,
    kline_count: int = 240,
    lookback: int = 3,
    indicator: str = 'macd',
) -> Dict[str, Any]:
    period_key = str(period or 'm30').strip().lower()
    indicator_key = str(indicator or 'macd').strip().lower()
    kline_count_value = max(120, int(kline_count or 240))
    lookback_value = max(2, int(lookback or 3))

    kline_rows = fetch_kline_rows(stock_code, period_key, kline_count_value)
    if len(kline_rows) < max(60, lookback_value * 12):
        return {
            'stock_code': stock_code,
            'period': period_key,
            'indicator': indicator_key,
            'kline_count': kline_count_value,
            'lookback': lookback_value,
            'bars': len(kline_rows),
            'top': [],
            'bottom': [],
        }

    if indicator_key != 'macd':
        return {
            'stock_code': stock_code,
            'period': period_key,
            'indicator': indicator_key,
            'kline_count': kline_count_value,
            'lookback': lookback_value,
            'bars': len(kline_rows),
            'top': [],
            'bottom': [],
        }

    close_values = [item['close'] for item in kline_rows]
    macd_hist = calculate_macd_hist_series(close_values)
    divergence = detect_divergence(kline_rows, macd_hist, lookback_value)
    points = _attach_time_to_points(kline_rows, divergence)

    return {
        'stock_code': stock_code,
        'period': period_key,
        'indicator': indicator_key,
        'kline_count': kline_count_value,
        'lookback': lookback_value,
        'bars': len(kline_rows),
        'top': points['top'],
        'bottom': points['bottom'],
    }
