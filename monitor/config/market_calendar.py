"""
A 股交易日历：节假日休市判断与下一交易时段计算。
"""
from __future__ import annotations

import json
import logging
import threading
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from monitor.config.market_time import now_in_market_tz

_logger = logging.getLogger(__name__)
_calendar_lock = threading.Lock()
_trading_day_cache: Dict[str, bool] = {}
_year_rows_cache: Dict[int, List[Dict[str, Any]]] = {}

_CONFIG_DIR = Path(__file__).resolve().parent
_STATIC_CALENDAR_PATH = _CONFIG_DIR / 'trade_calendar_static.json'
_PERSIST_DIR = _CONFIG_DIR / 'data' / 'trade_calendar'
_WEEKDAY_LABELS = ('周一', '周二', '周三', '周四', '周五', '周六', '周日')
_static_closed_dates: Optional[set[str]] = None


def _normalize_date(value: Union[date, datetime, str]) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or '').strip()
    if not text:
        return now_in_market_tz().date()
    if ' ' in text:
        text = text.split(' ', 1)[0]
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, '%Y%m%d').date()
    return datetime.strptime(text[:10], '%Y-%m-%d').date()


def _date_key(value: Union[date, datetime, str]) -> str:
    return _normalize_date(value).strftime('%Y-%m-%d')


def _load_static_closed_dates() -> set[str]:
    global _static_closed_dates
    if _static_closed_dates is not None:
        return _static_closed_dates
    dates: set[str] = set()
    try:
        if _STATIC_CALENDAR_PATH.exists():
            payload = json.loads(_STATIC_CALENDAR_PATH.read_text(encoding='utf-8'))
            for item in payload.get('closed_dates') or []:
                text = str(item or '').strip()
                if text:
                    dates.add(text[:10])
    except Exception as exc:
        _logger.warning('读取静态交易日历失败: %s', exc)
    _static_closed_dates = dates
    return dates


def _persist_path(year: int) -> Path:
    _PERSIST_DIR.mkdir(parents=True, exist_ok=True)
    return _PERSIST_DIR / f'{year}.json'


def _load_persisted_year_rows(year: int) -> Optional[List[Dict[str, Any]]]:
    path = _persist_path(year)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
        rows = payload.get('rows')
        return rows if isinstance(rows, list) else None
    except Exception as exc:
        _logger.warning('读取本地交易日历缓存失败(%s): %s', path, exc)
        return None


def _save_persisted_year_rows(year: int, rows: List[Dict[str, Any]]):
    try:
        path = _persist_path(year)
        path.write_text(
            json.dumps({'year': year, 'rows': rows}, ensure_ascii=False),
            encoding='utf-8',
        )
    except Exception as exc:
        _logger.warning('写入本地交易日历缓存失败: %s', exc)


def _rows_from_calendar_df(calendar_df, year: int) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for _, row in calendar_df.iterrows():
        day_text = str(row.get('trade_date') or '').strip()
        if not day_text:
            continue
        day = _normalize_date(day_text)
        trading = str(row.get('trade_status')) == '1'
        items.append({
            'date': day_text,
            'is_trading_day': trading,
            'weekday': day.weekday(),
            'weekday_label': _WEEKDAY_LABELS[day.weekday()],
            'status_label': '交易日' if trading else '休市',
        })
    return items


def _build_static_year_rows(year: int) -> List[Dict[str, Any]]:
    closed = _load_static_closed_dates()
    start = date(int(year), 1, 1)
    end = date(int(year), 12, 31)
    items: List[Dict[str, Any]] = []
    cursor = start
    while cursor <= end:
        key = cursor.strftime('%Y-%m-%d')
        if cursor.weekday() >= 5:
            trading = False
        elif key in closed:
            trading = False
        else:
            trading = True
        items.append({
            'date': key,
            'is_trading_day': trading,
            'weekday': cursor.weekday(),
            'weekday_label': _WEEKDAY_LABELS[cursor.weekday()],
            'status_label': '交易日' if trading else '休市',
        })
        cursor += timedelta(days=1)
    return items


def _load_calendar_df(year: int):
    from utils.date_utils import Date_utils

    return Date_utils.get_trade_calendar(year)


def _resolve_year_rows(year: int) -> List[Dict[str, Any]]:
    year = int(year)
    with _calendar_lock:
        cached = _year_rows_cache.get(year)
        if cached is not None:
            return cached

    rows: Optional[List[Dict[str, Any]]] = None
    source = 'network'
    try:
        calendar_df = _load_calendar_df(year)
        rows = _rows_from_calendar_df(calendar_df, year)
        _save_persisted_year_rows(year, rows)
    except Exception as exc:
        _logger.warning('在线交易日历加载失败(%s)，尝试本地缓存: %s', year, exc)
        rows = _load_persisted_year_rows(year)
        source = 'persisted' if rows else source

    if not rows:
        rows = _build_static_year_rows(year)
        source = 'static'
        _logger.warning('使用静态交易日历兜底(%s)', year)

    with _calendar_lock:
        _year_rows_cache[year] = rows
        for item in rows:
            _trading_day_cache[str(item['date'])] = bool(item['is_trading_day'])

    _logger.info('交易日历已加载: year=%s source=%s days=%s', year, source, len(rows))
    return rows


def is_trading_day(value: Union[date, datetime, str], *, default_weekday: bool = False) -> bool:
    """判断是否为 A 股交易日。"""
    day = _normalize_date(value)
    key = day.strftime('%Y-%m-%d')
    with _calendar_lock:
        if key in _trading_day_cache:
            return _trading_day_cache[key]

    rows = _resolve_year_rows(day.year)
    for item in rows:
        if item.get('date') == key:
            result = bool(item.get('is_trading_day'))
            with _calendar_lock:
                _trading_day_cache[key] = result
            return result

    if day.weekday() >= 5:
        return False
    if key in _load_static_closed_dates():
        return False
    return default_weekday


def get_trade_calendar_year(year: int) -> List[Dict[str, Any]]:
    return _resolve_year_rows(int(year))


def get_latest_trading_day(reference: Optional[Union[date, datetime, str]] = None) -> str:
    """返回不晚于 reference 的最近一个 A 股交易日（YYYY-MM-DD）。"""
    ref = _normalize_date(reference or now_in_market_tz())
    for offset in range(0, 366):
        candidate = ref - timedelta(days=offset)
        if is_trading_day(candidate):
            return candidate.strftime('%Y-%m-%d')
    return ref.strftime('%Y-%m-%d')


def invalidate_calendar_cache():
    global _static_closed_dates
    with _calendar_lock:
        _trading_day_cache.clear()
        _year_rows_cache.clear()
    _static_closed_dates = None
    try:
        from utils.date_utils import Date_utils

        Date_utils.calendar_cache.clear()
    except Exception:
        pass


def _session_starts_for_day(day: date) -> List[datetime]:
    return [
        datetime.combine(day, dt_time(9, 30)),
        datetime.combine(day, dt_time(13, 0)),
    ]


def get_next_trading_session_start(now_dt: Optional[datetime] = None) -> datetime:
    """返回下一次可能进入连续竞价监控的时间点。"""
    now_dt = (now_dt or now_in_market_tz()).replace(tzinfo=None)

    for day_offset in range(0, 366):
        day = now_dt.date() + timedelta(days=day_offset)
        if not is_trading_day(day):
            continue
        for session_start in _session_starts_for_day(day):
            if session_start > now_dt:
                return session_start

    return now_dt + timedelta(hours=12)


def get_market_state(now_dt: Optional[datetime] = None) -> str:
    """
    返回市场状态：open / lunch_break / closed
    含节假日休市判断。
    """
    now_dt = (now_dt or now_in_market_tz()).replace(tzinfo=None)
    now = dt_time(now_dt.hour, now_dt.minute, now_dt.second)

    if not is_trading_day(now_dt):
        return 'closed'

    morning_session = dt_time(9, 30) <= now < dt_time(11, 30)
    afternoon_session = dt_time(13, 0) <= now < dt_time(15, 0)
    if morning_session or afternoon_session:
        return 'open'
    if dt_time(11, 30) <= now < dt_time(13, 0):
        return 'lunch_break'
    return 'closed'


def is_market_open(now_dt: Optional[datetime] = None) -> bool:
    return get_market_state(now_dt) == 'open'


def is_alert_time_allowed(now_dt: Optional[datetime] = None) -> bool:
    """告警是否允许触发：交易日 + 连续竞价时段。"""
    return is_market_open(now_dt)


def seconds_until_next_trading_check(
    now_dt: Optional[datetime] = None,
    *,
    max_sleep_seconds: int = 300,
) -> int:
    now_dt = (now_dt or now_in_market_tz()).replace(tzinfo=None)
    state = get_market_state(now_dt)

    if state == 'open':
        return 1

    if state == 'lunch_break':
        next_dt = datetime.combine(now_dt.date(), dt_time(13, 0))
    else:
        next_dt = get_next_trading_session_start(now_dt)

    seconds = max(5, int((next_dt - now_dt).total_seconds()))
    return min(seconds, max(1, int(max_sleep_seconds)))
