"""
监控告警配置：RSI 周期阈值、开关默认值及解析工具。
"""
from __future__ import annotations

import copy
import json
import re
from typing import Any, Dict, List, Optional, Tuple

RSI_WINDOWS: Tuple[int, ...] = (1, 5, 30)

_RSI_PERIOD_FIELD_DEFAULTS: Dict[str, Any] = {
    'enabled': False,
    'low': 20,
    'high': 80,
    'boundary_low': False,
    'boundary_high': False,
    'reversal_low': False,
    'reversal_high': False,
    'engulfing': False,
}

DEFAULT_RSI_PERIOD_PRESETS: Dict[int, Dict[str, Any]] = {
    1: {
        'enabled': False,
        'low': 20,
        'high': 80,
        'boundary_low': False,
        'boundary_high': False,
        'reversal_low': False,
        'reversal_high': False,
        'engulfing': False,
    },
    5: {
        'enabled': True,
        'low': 20,
        'high': 80,
        'boundary_low': False,
        'boundary_high': True,
        'reversal_low': True,
        'reversal_high': False,
        'engulfing': True,
    },
    30: {
        'enabled': True,
        'low': 30,
        'high': 70,
        'boundary_low': True,
        'boundary_high': True,
        'reversal_low': True,
        'reversal_high': True,
        'engulfing': True,
    },
}

DEFAULT_RSI_ALERT_CONFIG: Dict[str, Any] = {
    'periods': {
        str(window): copy.deepcopy(DEFAULT_RSI_PERIOD_PRESETS[window])
        for window in RSI_WINDOWS
    },
}

def normalize_point_monitor_mode(raw_value: Any, default: str = 'both') -> str:
    text = str(raw_value or '').strip().lower()
    if text in {'off', 'none', 'stop', 'disable', '停止监控', '关闭监控'}:
        return 'off'
    if text in {'buy', 'buy_only', 'only_buy', '仅买点'}:
        return 'buy'
    if text in {'sell', 'sell_only', 'only_sell', '仅卖点'}:
        return 'sell'
    if text in {'both', 'all', '买卖点', '都监视'}:
        return 'both'
    return default


DEFAULT_STOCK_ALERT_TEMPLATE: Dict[str, Any] = {
    'point_monitor_mode': 'both',
    'common': True,
    'divergence_enabled': True,
    'divergence_macd_enabled': True,
    'divergence_top_enabled': True,
    'divergence_bottom_enabled': True,
    'divergence_periods': ['m30'],
    'divergence_scan_interval_seconds': 60,
    'divergence_kline_count': 240,
    'divergence_lookback': 3,
    'rsi_alert_config': DEFAULT_RSI_ALERT_CONFIG,
}


def _window_key(window: Any) -> str:
    try:
        return str(int(window))
    except (TypeError, ValueError):
        return str(window or '').strip()


def _to_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    if text in {'1', 'true', 'yes', 'on'}:
        return True
    if text in {'0', 'false', 'no', 'off'}:
        return False
    return default


def _clamp_threshold(value: Any, default: float) -> float:
    try:
        num = float(value)
    except (TypeError, ValueError):
        num = default
    return max(1.0, min(99.0, num))


def _normalize_period_item(raw_item: Any, window: int) -> Dict[str, Any]:
    preset = copy.deepcopy(DEFAULT_RSI_PERIOD_PRESETS.get(window, _RSI_PERIOD_FIELD_DEFAULTS))
    if not isinstance(raw_item, dict):
        return preset

    preset['enabled'] = _to_bool(raw_item.get('enabled'), preset['enabled'])
    preset['low'] = _clamp_threshold(raw_item.get('low'), preset['low'])
    preset['high'] = _clamp_threshold(raw_item.get('high'), preset['high'])
    if preset['low'] >= preset['high']:
        fallback = DEFAULT_RSI_PERIOD_PRESETS.get(window, {'low': 20, 'high': 80})
        preset['low'] = float(fallback['low'])
        preset['high'] = float(fallback['high'])

    for key in ('boundary_low', 'boundary_high', 'reversal_low', 'reversal_high', 'engulfing'):
        if key in raw_item:
            preset[key] = _to_bool(raw_item.get(key), preset[key])
    return preset


def normalize_rsi_alert_config(raw_value: Any, base: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """解析并规范化 RSI 告警配置。"""
    merged_base = copy.deepcopy(base or DEFAULT_RSI_ALERT_CONFIG)
    base_periods = merged_base.get('periods') if isinstance(merged_base.get('periods'), dict) else {}

    parsed: Dict[str, Any] = {}
    if isinstance(raw_value, str) and raw_value.strip():
        try:
            parsed = json.loads(raw_value)
        except Exception:
            parsed = {}
    elif isinstance(raw_value, dict):
        parsed = raw_value

    raw_periods = parsed.get('periods') if isinstance(parsed.get('periods'), dict) else parsed
    periods: Dict[str, Dict[str, Any]] = {}
    for window in RSI_WINDOWS:
        key = str(window)
        source = {}
        if isinstance(raw_periods, dict):
            source = raw_periods.get(key) or raw_periods.get(window) or {}
        elif isinstance(base_periods, dict):
            source = base_periods.get(key) or {}
        periods[key] = _normalize_period_item(source, window)

    return {'periods': periods}


def rsi_alert_config_to_storage_text(config: Any) -> str:
    normalized = normalize_rsi_alert_config(config)
    return json.dumps(normalized, ensure_ascii=False)


def parse_rsi_alert_config_from_storage(raw_value: Any) -> Dict[str, Any]:
    if raw_value in (None, ''):
        return copy.deepcopy(DEFAULT_RSI_ALERT_CONFIG)
    return normalize_rsi_alert_config(raw_value)


def resolve_rsi_alert_config(stock_cfg: Optional[Dict[str, Any]], global_cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    base = normalize_rsi_alert_config(global_cfg or DEFAULT_RSI_ALERT_CONFIG)
    stock_raw = (stock_cfg or {}).get('rsi_alert_config')
    if stock_raw in (None, ''):
        return base
    return normalize_rsi_alert_config(stock_raw, base=base)


def get_rsi_period_config(stock_cfg: Optional[Dict[str, Any]], window: int, global_cfg: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    resolved = resolve_rsi_alert_config(stock_cfg, global_cfg)
    period = (resolved.get('periods') or {}).get(_window_key(window))
    if not period or not period.get('enabled'):
        return None
    return period


def get_enabled_rsi_windows(stock_cfg: Optional[Dict[str, Any]], global_cfg: Optional[Dict[str, Any]] = None) -> List[int]:
    resolved = resolve_rsi_alert_config(stock_cfg, global_cfg)
    periods = resolved.get('periods') or {}
    enabled: List[int] = []
    for window in RSI_WINDOWS:
        period = periods.get(str(window)) or {}
        if period.get('enabled'):
            enabled.append(window)
    return enabled


def extract_rsi_window_from_message(message: str) -> Optional[int]:
    match = re.search(r'\((\d+)min\)', str(message or ''))
    if not match:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return None


def classify_rsi_message_side(message: str, stock_cfg: Optional[Dict[str, Any]], global_cfg: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """根据消息与配置判断 RSI 告警属于买侧还是卖侧。"""
    text = str(message or '')
    lowered = text.lower()
    window = extract_rsi_window_from_message(text)
    period_cfg = get_rsi_period_config(stock_cfg, window, global_cfg) if window else None
    low = period_cfg['low'] if period_cfg else 20
    high = period_cfg['high'] if period_cfg else 80

    if 'rsi_6_up' in lowered or 'engulfing_up' in lowered:
        return 'buy'
    if 'rsi_6_down' in lowered or 'engulfing_down' in lowered:
        return 'sell'
    if 'rsi_6:' in lowered:
        match = re.search(r'rsi_6\s*:\s*([0-9]+(?:\.[0-9]+)?)', lowered)
        if match:
            try:
                value = float(match.group(1))
                if value <= low:
                    return 'buy'
                if value >= high:
                    return 'sell'
            except (TypeError, ValueError):
                return None
    return None
