"""
MACD 背离（DIF）— 摆动高低点算法，贴近肉眼所见的波峰/波谷。

MACD DIF / 图表标记筛选与 trader_front KLineMerger.vue 保持一致。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, TypedDict

class PivotConfig(TypedDict):
    pivot_left: int
    pivot_right: int
    range_lower: int
    range_upper: int


TRADING_VIEW_RSI_DIVERGENCE_CONFIG: PivotConfig = {
    'pivot_left': 5,
    'pivot_right': 5,
    'range_lower': 5,
    'range_upper': 60,
}


def resolve_swing_config(period: str) -> PivotConfig:
    _ = period
    return dict(TRADING_VIEW_RSI_DIVERGENCE_CONFIG)


def resolve_divergence_lookback(period: str) -> int:
    return int(resolve_swing_config(period)['pivot_right'])


def calculate_macd_dif_series(
    close_values: Sequence[float],
    short_period: int = 12,
    long_period: int = 26,
    signal_period: int = 9,
) -> List[Optional[float]]:
    """
    与 trader_front KLineMerger.vue calculateMACD 的 DIF 口径一致：
    EMA12/26 以前 N 根收盘价 SMA 为种子，满 long_period 后才输出 DIF。
    """
    _ = signal_period
    if not close_values:
        return []

    closes = [float(value) for value in close_values]
    length = len(closes)
    multiplier12 = 2 / (short_period + 1)
    multiplier26 = 2 / (long_period + 1)

    sum12 = sum(closes[: min(short_period, length)])
    sum26 = sum(closes[: min(long_period, length)])
    ema_val12 = sum12 / min(short_period, length)
    ema_val26 = sum26 / min(long_period, length)

    dif_values: List[Optional[float]] = []
    for i in range(length):
        if i < short_period - 1:
            ema12: Optional[float] = None
        elif i == short_period - 1:
            ema12 = ema_val12
        else:
            ema_val12 = (closes[i] - ema_val12) * multiplier12 + ema_val12
            ema12 = ema_val12

        if i < long_period - 1:
            ema26: Optional[float] = None
        elif i == long_period - 1:
            ema26 = ema_val26
        else:
            ema_val26 = (closes[i] - ema_val26) * multiplier26 + ema_val26
            ema26 = ema_val26

        if ema12 is not None and ema26 is not None:
            dif_values.append(round(ema12 - ema26, 4))
        else:
            dif_values.append(None)

    return dif_values


def calculate_macd_hist_series(
    close_values: Sequence[float],
    short_period: int = 12,
    long_period: int = 26,
    signal_period: int = 9,
) -> List[Optional[float]]:
    """与 KLineMerger.vue calculateMACD 的 MACD 柱口径一致。"""
    dif_values = calculate_macd_dif_series(close_values, short_period, long_period, signal_period)
    if not dif_values:
        return []

    multiplier_signal = 2 / (signal_period + 1)
    dea_val: Optional[float] = None
    valid_dif_count = 0
    dif_sum = 0.0
    hist_values: List[Optional[float]] = []

    for dif in dif_values:
        if dif is None:
            hist_values.append(None)
            continue

        valid_dif_count += 1
        if valid_dif_count < signal_period:
            dif_sum += dif
            hist_values.append(None)
        elif valid_dif_count == signal_period:
            dif_sum += dif
            dea_val = dif_sum / signal_period
            hist_values.append(round((dif - dea_val) * 2, 4))
        else:
            assert dea_val is not None
            dea_val = (dif - dea_val) * multiplier_signal + dea_val
            hist_values.append(round((dif - dea_val) * 2, 4))

    return hist_values


CHART_DIVERGENCE_MIN_MARKER_GAP = 3
CHART_DIVERGENCE_MAX_MARKERS_EACH = 999


def select_chart_divergence_points(
    points: Sequence[Dict[str, Any]],
    pin_indexes: Optional[Sequence[int]] = None,
    min_marker_gap: int = CHART_DIVERGENCE_MIN_MARKER_GAP,
    max_markers: int = CHART_DIVERGENCE_MAX_MARKERS_EACH,
) -> List[Dict[str, Any]]:
    """与 KLineMerger.vue selectChartDivergencePoints 一致，用于监控取「图表可见」信号。"""
    safe_points = list(points or [])
    selected: List[Dict[str, Any]] = []
    pin_set = {
        int(index)
        for index in (pin_indexes or [])
        if isinstance(index, (int, float)) and int(index) >= 0
    }

    def non_pin_count() -> int:
        return sum(1 for item in selected if int(item['index']) not in pin_set)

    def try_add(point: Optional[Dict[str, Any]], force: bool = False) -> None:
        if not point:
            return
        try:
            index = int(point['index'])
        except (TypeError, ValueError):
            return
        if any(int(item['index']) == index for item in selected):
            return
        if (
            not force
            and any(abs(int(item['index']) - index) < min_marker_gap for item in selected)
        ):
            return
        if not force and non_pin_count() >= max_markers:
            return
        selected.append(point)

    for index in pin_set:
        matched = next((item for item in safe_points if int(item['index']) == index), None)
        try_add(matched, force=True)

    candidates = sorted(safe_points, key=lambda item: int(item['index']), reverse=True)
    for point in candidates:
        try_add(point, force=False)

    return sorted(selected, key=lambda item: int(item['index']))


def _find_swing_points(
    values: Sequence[Any],
    radius: int,
    is_high: bool,
) -> List[Dict[str, Any]]:
    swings: List[Dict[str, Any]] = []
    length = len(values)
    for i in range(radius, length - radius):
        center = values[i]
        if center is None:
            continue
        try:
            center_value = float(center)
        except (TypeError, ValueError):
            continue

        is_swing = True
        for j in range(i - radius, i + radius + 1):
            if j == i:
                continue
            other = values[j]
            if other is None:
                is_swing = False
                break
            try:
                other_value = float(other)
            except (TypeError, ValueError):
                is_swing = False
                break
            if is_high and center_value < other_value:
                is_swing = False
                break
            if not is_high and center_value > other_value:
                is_swing = False
                break

        if is_swing:
            swings.append({'index': i, 'value': center_value})

    return swings


def _merge_nearby_swings(
    swings: List[Dict[str, Any]],
    merge_within_bars: int,
    is_high: bool,
) -> List[Dict[str, Any]]:
    if not swings:
        return swings

    merged: List[Dict[str, Any]] = [swings[0]]
    for current in swings[1:]:
        last = merged[-1]
        if int(current['index']) - int(last['index']) < merge_within_bars:
            if is_high and current['value'] > last['value']:
                merged[-1] = current
            elif not is_high and current['value'] < last['value']:
                merged[-1] = current
        else:
            merged.append(current)
    return merged


def _dif_at_swing(
    indicator_values: Sequence[Any],
    index: int,
    window: int,
    is_high: bool,
) -> Optional[float]:
    result: Optional[float] = None
    for i in range(index - window, index + window + 1):
        if i < 0 or i >= len(indicator_values):
            continue
        raw = indicator_values[i]
        if raw is None:
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if result is None:
            result = value
        elif is_high:
            result = max(result, value)
        else:
            result = min(result, value)
    return result


def _dif_lift_enough(prev_dif: float, current_dif: float, min_ratio: float) -> bool:
    if current_dif > prev_dif:
        return True
    base = max(abs(prev_dif), 1e-6)
    return current_dif >= prev_dif + base * min_ratio


def _dif_drop_enough(prev_dif: float, current_dif: float, min_ratio: float) -> bool:
    if current_dif < prev_dif:
        return True
    base = max(abs(prev_dif), 1e-6)
    return current_dif <= prev_dif - base * min_ratio


def _dedupe_signal_clusters(
    points: List[Dict[str, Any]],
    gap: int,
    is_bottom: bool,
) -> List[Dict[str, Any]]:
    if not points or gap <= 0:
        return points

    sorted_points = sorted(points, key=lambda item: int(item['index']))
    result: List[Dict[str, Any]] = []
    cluster: List[Dict[str, Any]] = []

    def flush_cluster() -> None:
        nonlocal cluster
        if not cluster:
            return
        best = cluster[0]
        for point in cluster:
            best_price = float(best.get('priceValue', float('inf') if is_bottom else float('-inf')))
            point_price = float(point.get('priceValue', float('inf') if is_bottom else float('-inf')))
            if is_bottom and point_price < best_price:
                best = point
            if not is_bottom and point_price > best_price:
                best = point
        result.append(best)
        cluster = []

    for point in sorted_points:
        if not cluster or int(point['index']) - int(cluster[-1]['index']) < gap:
            cluster.append(point)
        else:
            flush_cluster()
            cluster = [point]
    flush_cluster()
    return result


def _to_float_or_none(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric


def _find_indicator_pivots(
    values: Sequence[Any],
    left: int,
    right: int,
    is_high: bool,
) -> List[Dict[str, Any]]:
    pivots: List[Dict[str, Any]] = []
    length = len(values)
    for i in range(left, length - right):
        center = _to_float_or_none(values[i])
        if center is None:
            continue

        is_pivot = True
        for j in range(i - left, i + right + 1):
            if j == i:
                continue
            other = _to_float_or_none(values[j])
            if other is None:
                is_pivot = False
                break
            if is_high and center <= other:
                is_pivot = False
                break
            if not is_high and center >= other:
                is_pivot = False
                break

        if is_pivot:
            pivots.append({'index': i, 'value': center})
    return pivots


def _in_pivot_range(prev: Dict[str, Any], current: Dict[str, Any], config: PivotConfig) -> bool:
    bars = int(current['index']) - int(prev['index'])
    return int(config['range_lower']) <= bars <= int(config['range_upper'])


def detect_divergence(
    price_rows: Sequence[Dict[str, Any]],
    indicator_values: Sequence[Any],
    period_or_legacy_lookback: Any = 'm30',
) -> Dict[str, List[Dict[str, Any]]]:
    top_divergence: List[Dict[str, Any]] = []
    bottom_divergence: List[Dict[str, Any]] = []

    bar_count = len(price_rows)
    if bar_count == 0 or len(indicator_values) != bar_count:
        return {'top': top_divergence, 'bottom': bottom_divergence}

    config = resolve_swing_config(str(period_or_legacy_lookback or 'm30'))
    if not isinstance(period_or_legacy_lookback, str):
        legacy_lookback = max(1, int(period_or_legacy_lookback or config['pivot_right']))
        config['pivot_left'] = legacy_lookback
        config['pivot_right'] = legacy_lookback

    pivot_lows = _find_indicator_pivots(
        indicator_values,
        int(config['pivot_left']),
        int(config['pivot_right']),
        is_high=False,
    )
    pivot_highs = _find_indicator_pivots(
        indicator_values,
        int(config['pivot_left']),
        int(config['pivot_right']),
        is_high=True,
    )

    for i in range(1, len(pivot_lows)):
        current = pivot_lows[i]
        prev = pivot_lows[i - 1]
        if not _in_pivot_range(prev, current, config):
            continue

        current_row = price_rows[int(current['index'])]
        prev_row = price_rows[int(prev['index'])]
        current_low = _to_float_or_none(current_row.get('low', current_row.get('close')))
        prev_low = _to_float_or_none(prev_row.get('low', prev_row.get('close')))
        if current_low is None or prev_low is None:
            continue

        price_lower_low = current_low < prev_low
        indicator_higher_low = float(current['value']) > float(prev['value'])
        if not price_lower_low or not indicator_higher_low:
            continue

        bottom_divergence.append({
            'index': current['index'],
            'priceValue': current_low,
            'indicatorValue': current['value'],
        })

    for i in range(1, len(pivot_highs)):
        current = pivot_highs[i]
        prev = pivot_highs[i - 1]
        if not _in_pivot_range(prev, current, config):
            continue

        current_row = price_rows[int(current['index'])]
        prev_row = price_rows[int(prev['index'])]
        current_high = _to_float_or_none(current_row.get('high', current_row.get('close')))
        prev_high = _to_float_or_none(prev_row.get('high', prev_row.get('close')))
        if current_high is None or prev_high is None:
            continue

        price_higher_high = current_high > prev_high
        indicator_lower_high = float(current['value']) < float(prev['value'])
        if not price_higher_high or not indicator_lower_high:
            continue

        top_divergence.append({
            'index': current['index'],
            'priceValue': current_high,
            'indicatorValue': current['value'],
        })

    return {'top': top_divergence, 'bottom': bottom_divergence}
