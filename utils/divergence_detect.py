"""
MACD/RSI 背离检测（图表与监控告警共用，口径一致）。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

# 局部极值窗口
DEFAULT_LOOKBACK = 5
# 两次价格极值至少间隔 K 线根数（5 分钟周期约 50 分钟）
MIN_EXTREME_BAR_GAP = 10
# 价格创新低/创新高的最小幅度
MIN_PRICE_MOVE_PCT = 0.002
# 指标反向运动的最小相对幅度（相对前一次极值绝对值）
MIN_INDICATOR_MOVE_RATIO = 0.08
INDICATOR_ALIGN_TOLERANCE = 2


def calculate_macd_hist_series(
    close_values: Sequence[float],
    short_period: int = 12,
    long_period: int = 26,
    signal_period: int = 9,
) -> List[Optional[float]]:
    if not close_values:
        return []

    ema_short: List[float] = []
    ema_long: List[float] = []
    dif_values: List[float] = []
    dea_values: List[float] = []
    hist_values: List[Optional[float]] = []

    for i, close_price in enumerate(close_values):
        price = float(close_price)
        if i == 0:
            ema_short.append(price)
            ema_long.append(price)
            dif_values.append(0.0)
            dea_values.append(0.0)
            hist_values.append(0.0)
            continue

        prev_ema_short = ema_short[-1]
        prev_ema_long = ema_long[-1]
        current_ema_short = (price * 2 / (short_period + 1)) + prev_ema_short * (1 - 2 / (short_period + 1))
        current_ema_long = (price * 2 / (long_period + 1)) + prev_ema_long * (1 - 2 / (long_period + 1))
        ema_short.append(current_ema_short)
        ema_long.append(current_ema_long)

        dif = current_ema_short - current_ema_long
        dif_values.append(dif)
        dea = (dif * 2 / (signal_period + 1)) + dea_values[-1] * (1 - 2 / (signal_period + 1))
        dea_values.append(dea)
        hist_values.append(round((dif - dea) * 2, 4))

    return hist_values


def _find_local_extremes(values: Sequence[Any], lookback: int, is_max: bool = True) -> List[Dict[str, Any]]:
    extremes: List[Dict[str, Any]] = []
    length = len(values)
    for i in range(lookback, length - lookback):
        value = values[i]
        if value is None:
            continue

        is_extreme = True
        for j in range(1, lookback + 1):
            left = values[i - j]
            right = values[i + j]
            if left is None or right is None:
                is_extreme = False
                break
            if is_max:
                if value < left or value < right:
                    is_extreme = False
                    break
            elif value > left or value > right:
                is_extreme = False
                break

        if is_extreme:
            extremes.append({'index': i, 'value': float(value)})

    return extremes


def _find_nearest_indicator_extreme(extremes: List[Dict[str, Any]], target_index: int) -> Optional[Dict[str, Any]]:
    candidates = [
        item
        for item in extremes
        if abs(int(item['index']) - int(target_index)) <= INDICATOR_ALIGN_TOLERANCE
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda item: (abs(int(item['index']) - int(target_index)), int(item['index'])))
    return candidates[0]


def _indicator_move_is_significant(prev_value: float, current_value: float, is_bottom: bool) -> bool:
    base = max(abs(prev_value), 1e-6)
    delta = current_value - prev_value
    if is_bottom:
        return delta > 0 and (delta / base) >= MIN_INDICATOR_MOVE_RATIO
    return delta < 0 and (abs(delta) / base) >= MIN_INDICATOR_MOVE_RATIO


def _price_move_is_significant(prev_value: float, current_value: float, is_bottom: bool) -> bool:
    if prev_value <= 0:
        return False
    if is_bottom:
        return (prev_value - current_value) / prev_value >= MIN_PRICE_MOVE_PCT
    return (current_value - prev_value) / prev_value >= MIN_PRICE_MOVE_PCT


def _upsert_divergence_point(points: List[Dict[str, Any]], point: Dict[str, Any]) -> None:
    if not points:
        points.append(point)
        return
    last = points[-1]
    if int(point['index']) - int(last['index']) < MIN_EXTREME_BAR_GAP:
        points[-1] = point
        return
    points.append(point)


def detect_divergence(
    price_rows: Sequence[Dict[str, Any]],
    indicator_values: Sequence[Any],
    lookback_period: int = DEFAULT_LOOKBACK,
) -> Dict[str, List[Dict[str, Any]]]:
    top_divergence: List[Dict[str, Any]] = []
    bottom_divergence: List[Dict[str, Any]] = []

    if not price_rows or not indicator_values:
        return {'top': top_divergence, 'bottom': bottom_divergence}

    lookback = max(2, int(lookback_period or DEFAULT_LOOKBACK))
    price_highs_raw = [item.get('high', item.get('close')) for item in price_rows]
    price_lows_raw = [item.get('low', item.get('close')) for item in price_rows]

    price_highs = _find_local_extremes(price_highs_raw, lookback, is_max=True)
    price_lows = _find_local_extremes(price_lows_raw, lookback, is_max=False)
    indicator_highs = _find_local_extremes(indicator_values, lookback, is_max=True)
    indicator_lows = _find_local_extremes(indicator_values, lookback, is_max=False)

    for i in range(1, len(price_highs)):
        current_price_high = price_highs[i]
        prev_price_high = price_highs[i - 1]
        if current_price_high['index'] - prev_price_high['index'] < MIN_EXTREME_BAR_GAP:
            continue
        if not _price_move_is_significant(prev_price_high['value'], current_price_high['value'], is_bottom=False):
            continue

        prev_indicator_high = _find_nearest_indicator_extreme(indicator_highs, prev_price_high['index'])
        current_indicator_high = _find_nearest_indicator_extreme(indicator_highs, current_price_high['index'])
        if not prev_indicator_high or not current_indicator_high:
            continue
        if current_indicator_high['index'] <= prev_indicator_high['index']:
            continue
        if not _indicator_move_is_significant(
            float(prev_indicator_high['value']),
            float(current_indicator_high['value']),
            is_bottom=False,
        ):
            continue

        _upsert_divergence_point(
            top_divergence,
            {
                'index': current_price_high['index'],
                'priceValue': current_price_high['value'],
                'indicatorValue': current_indicator_high['value'],
            },
        )

    for i in range(1, len(price_lows)):
        current_price_low = price_lows[i]
        prev_price_low = price_lows[i - 1]
        if current_price_low['index'] - prev_price_low['index'] < MIN_EXTREME_BAR_GAP:
            continue
        if not _price_move_is_significant(prev_price_low['value'], current_price_low['value'], is_bottom=True):
            continue

        prev_indicator_low = _find_nearest_indicator_extreme(indicator_lows, prev_price_low['index'])
        current_indicator_low = _find_nearest_indicator_extreme(indicator_lows, current_price_low['index'])
        if not prev_indicator_low or not current_indicator_low:
            continue
        if current_indicator_low['index'] <= prev_indicator_low['index']:
            continue
        if not _indicator_move_is_significant(
            float(prev_indicator_low['value']),
            float(current_indicator_low['value']),
            is_bottom=True,
        ):
            continue

        _upsert_divergence_point(
            bottom_divergence,
            {
                'index': current_price_low['index'],
                'priceValue': current_price_low['value'],
                'indicatorValue': current_indicator_low['value'],
            },
        )

    return {'top': top_divergence, 'bottom': bottom_divergence}
