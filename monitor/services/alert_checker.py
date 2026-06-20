import json
import logging
import os
import re
import threading
import time
from datetime import datetime
import requests
from utils.tushare_utils import IndexAnalysis
from utils.IndicatorCalculation import IndicatorCalculation
from utils.GetStockData import get_stock_name
from utils.divergence_detect import (
    calculate_macd_dif_series,
    detect_divergence,
    select_chart_divergence_points,
)
from utils.kline_forward_adjust import (
    apply_forward_adjust_to_quote_rows,
    fetch_daily_forward_factors,
)
from utils.divergence_service import fetch_kline_rows
from monitor.config.db_monitor import db_manager
from monitor.config.alert_monitor_config import (
    normalize_point_monitor_mode,
    DEFAULT_RSI_ALERT_CONFIG,
    classify_rsi_message_side,
    get_enabled_rsi_windows,
    get_rsi_period_config,
    normalize_rsi_alert_config,
)

_logger = logging.getLogger(__name__)
_DIVERGENCE_PERIOD_WINDOW_SECONDS = {
    'm1': 60,
    'm5': 300,
    'm15': 900,
    'm30': 1800,
    'day': 86400,
    'week': 604800,
    'month': 2592000,
}
_DIVERGENCE_PERIOD_COOLDOWN_SECONDS = {
    'm1': 180,
    'm5': 300,
    'm15': 900,
    'm30': 1800,
    'day': 21600,
    'week': 86400,
    'month': 172800,
}
_DIVERGENCE_RECENT_BAR_LIMIT = {
    'm1': 60,
    'm5': 48,
    'm15': 32,
    'm30': 24,
    'day': 12,
    'week': 8,
    'month': 6,
}
_RUNTIME_SETTING_TABLE = 'monitor_runtime_settings'
_DIVERGENCE_SETTING_KEY = 'divergence_monitor_config'
_RSI_SETTING_KEY = 'rsi_alert_config'
_STOCK_TEMPLATE_SETTING_KEY = 'stock_alert_template'


class AlertChecker:
    def __init__(self, config, stock_data):
        self.config = config
        self.stock_data = stock_data
        self._last_candle_times = {}
        self._rsi_trigger_states = {}
        self._divergence_lock = threading.Lock()
        self._divergence_periods = self._load_divergence_periods()
        self._divergence_scan_interval_seconds = max(
            15, int(os.getenv('DIVERGENCE_SCAN_INTERVAL_SECONDS', '60'))
        )
        self._divergence_kline_count = max(120, int(os.getenv('DIVERGENCE_KLINE_COUNT', '240')))
        self._divergence_lookback = max(2, int(os.getenv('DIVERGENCE_LOOKBACK', '3')))
        self._divergence_last_scan_at = {}
        self._divergence_last_signal = {}
        self._divergence_bootstrapped = set()
        self._rsi_alert_config = normalize_rsi_alert_config(DEFAULT_RSI_ALERT_CONFIG)
        self._default_point_monitor_mode = 'both'
        self._load_divergence_settings_from_storage()
        self._load_rsi_settings_from_storage()
        self._load_stock_template_settings_from_storage()

    def check_all_conditions(self, stock):
        alerts = []
        stock_cfg = self.config.MONITOR_STOCKS.get(stock, {}) or {}
        candles = self.stock_data.get_stock_data(stock)
        if not candles:
            return alerts
        current_price = candles[-1].get('close')
        if current_price is None:
            return alerts

        if not self._is_price_in_trigger_range(stock, current_price):
            return alerts

        # 检查固定价格阈值条件
        price_threshold_alerts = self._check_price_thresholds(stock, candles)
        alerts.extend(price_threshold_alerts)

        # 检查涨跌幅百分比阈值条件
        change_threshold_alerts = self._check_change_thresholds(stock, candles)
        alerts.extend(change_threshold_alerts)

        # ma_alerts = self._check_ma_breakdown(stock)
        # alerts.extend(ma_alerts)

        # 检查时间窗口内涨跌条件
        if stock_cfg.get("normal_movement"):
            conditions = self._check_time_window_conditions(stock, 20)
            alerts.extend(conditions)

        if stock_cfg.get("common", False):
            for period in get_enabled_rsi_windows(stock_cfg, self.get_rsi_alert_config()):
                common_alerts = self._check_common_by_min(stock, period)
                alerts.extend(common_alerts)

        divergence_alerts = self._check_divergence_alerts(stock)
        alerts.extend(divergence_alerts)

        point_mode = self._get_point_monitor_mode(stock_cfg)
        return [
            item for item in alerts
            if self._is_alert_allowed_by_point_mode(item, point_mode, stock_cfg)
        ]

    def _get_point_monitor_mode(self, stock_cfg):
        if not self._to_bool(stock_cfg.get('is_monitor'), True):
            return 'off'
        mode = str(stock_cfg.get('point_monitor_mode') or '').strip().lower()
        if mode in {'buy', 'sell', 'both', 'off'}:
            return mode
        if self._to_bool(stock_cfg.get('point_monitor_enabled'), False):
            return 'both'
        return 'both'

    def _extract_alert_payload(self, alert_item):
        if isinstance(alert_item, dict):
            return alert_item
        if isinstance(alert_item, tuple) and alert_item and isinstance(alert_item[0], dict):
            return alert_item[0]
        return None

    def _is_buy_side_alert(self, alert_payload, stock_cfg=None):
        alert_type = str(alert_payload.get('alert_type') or '').strip()
        message = str(alert_payload.get('alert_message') or '')
        if alert_type in {'买点', '底背离'}:
            return True
        if alert_type == '背离' and '底背离' in message:
            return True
        if alert_type == '观察':
            side = classify_rsi_message_side(message, stock_cfg, self.get_rsi_alert_config())
            return side == 'buy'
        return False

    def _is_sell_side_alert(self, alert_payload, stock_cfg=None):
        alert_type = str(alert_payload.get('alert_type') or '').strip()
        message = str(alert_payload.get('alert_message') or '')
        if alert_type in {'卖点', '顶背离'}:
            return True
        if alert_type == '背离' and '顶背离' in message:
            return True
        if alert_type == '观察':
            side = classify_rsi_message_side(message, stock_cfg, self.get_rsi_alert_config())
            return side == 'sell'
        return False

    def _is_alert_allowed_by_point_mode(self, alert_item, point_mode, stock_cfg=None):
        payload = self._extract_alert_payload(alert_item)
        if not payload:
            return False
        mode = str(point_mode or 'both').strip().lower()
        if mode == 'off':
            return False
        if mode == 'buy':
            return self._is_buy_side_alert(payload, stock_cfg)
        if mode == 'sell':
            return self._is_sell_side_alert(payload, stock_cfg)
        return self._is_buy_side_alert(payload, stock_cfg) or self._is_sell_side_alert(payload, stock_cfg)

    def _load_divergence_periods(self):
        raw = str(os.getenv('DIVERGENCE_MONITOR_PERIODS', 'm30') or '').strip().lower()
        return self._normalize_divergence_periods(raw)

    def _normalize_divergence_periods(self, periods):
        allowed = {'m1', 'm5', 'm15', 'm30', 'day', 'week', 'month'}
        if isinstance(periods, str):
            matched = re.findall(r"m1|m5|m15|m30|day|week|month", periods.lower())
            candidates = [item.strip().lower() for item in matched]
        elif isinstance(periods, (list, tuple, set)):
            candidates = [str(item or '').strip().lower() for item in periods]
        else:
            candidates = []

        values = []
        for period in candidates:
            if not period or period not in allowed:
                continue
            if period not in values:
                values.append(period)
        return values or ['m30']

    def _ensure_runtime_setting_table(self):
        conn = None
        cursor = None
        try:
            with db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {_RUNTIME_SETTING_TABLE} (
                        setting_key VARCHAR(128) NOT NULL PRIMARY KEY,
                        setting_value TEXT NULL,
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """
                )
        except Exception as e:
            _logger.warning("确保运行时配置表失败: %s", e)
        finally:
            if cursor:
                cursor.close()

    def _load_divergence_settings_from_storage(self):
        self._ensure_runtime_setting_table()
        rows = db_manager.execute_query(
            f"SELECT setting_value FROM {_RUNTIME_SETTING_TABLE} WHERE setting_key = %s LIMIT 1",
            (_DIVERGENCE_SETTING_KEY,),
        )
        if not rows:
            return
        raw_value = rows[0].get('setting_value')
        if not raw_value:
            return
        try:
            payload = json.loads(raw_value)
        except Exception:
            return
        self.update_divergence_config(
            periods=payload.get('periods'),
            scan_interval_seconds=payload.get('scan_interval_seconds'),
            kline_count=payload.get('kline_count'),
            lookback=payload.get('lookback'),
            persist=False,
            reset_state=True,
        )

    def _save_divergence_settings_to_storage(self):
        self._ensure_runtime_setting_table()
        payload = self.get_divergence_config()
        db_manager.execute_delete(_RUNTIME_SETTING_TABLE, "setting_key = %s", (_DIVERGENCE_SETTING_KEY,))
        db_manager.execute_insert(
            _RUNTIME_SETTING_TABLE,
            {
                'setting_key': _DIVERGENCE_SETTING_KEY,
                'setting_value': json.dumps(payload, ensure_ascii=False),
            }
        )

    def get_divergence_config(self):
        with self._divergence_lock:
            return {
                'periods': list(self._divergence_periods),
                'scan_interval_seconds': int(self._divergence_scan_interval_seconds),
                'kline_count': int(self._divergence_kline_count),
                'lookback': int(self._divergence_lookback),
            }

    def update_divergence_config(
        self,
        *,
        periods=None,
        scan_interval_seconds=None,
        kline_count=None,
        lookback=None,
        persist=True,
        reset_state=True,
    ):
        with self._divergence_lock:
            if periods is not None:
                self._divergence_periods = self._normalize_divergence_periods(periods)
            if scan_interval_seconds is not None:
                self._divergence_scan_interval_seconds = max(15, int(scan_interval_seconds))
            if kline_count is not None:
                self._divergence_kline_count = max(120, int(kline_count))
            if lookback is not None:
                self._divergence_lookback = max(2, int(lookback))

            if reset_state:
                self._divergence_last_scan_at = {}
                self._divergence_last_signal = {}
                self._divergence_bootstrapped = set()

        if persist:
            self._save_divergence_settings_to_storage()

    def _load_rsi_settings_from_storage(self):
        self._ensure_runtime_setting_table()
        rows = db_manager.execute_query(
            f"SELECT setting_value FROM {_RUNTIME_SETTING_TABLE} WHERE setting_key = %s LIMIT 1",
            (_RSI_SETTING_KEY,),
        )
        if not rows:
            return
        raw_value = rows[0].get('setting_value')
        if not raw_value:
            return
        try:
            payload = json.loads(raw_value)
        except Exception:
            return
        self.update_rsi_alert_config(payload, persist=False)

    def _save_rsi_settings_to_storage(self):
        self._ensure_runtime_setting_table()
        payload = self.get_rsi_alert_config()
        db_manager.execute_delete(_RUNTIME_SETTING_TABLE, "setting_key = %s", (_RSI_SETTING_KEY,))
        db_manager.execute_insert(
            _RUNTIME_SETTING_TABLE,
            {
                'setting_key': _RSI_SETTING_KEY,
                'setting_value': json.dumps(payload, ensure_ascii=False),
            }
        )

    def get_rsi_alert_config(self):
        with self._divergence_lock:
            return normalize_rsi_alert_config(self._rsi_alert_config)

    def update_rsi_alert_config(self, config=None, *, persist=True):
        with self._divergence_lock:
            self._rsi_alert_config = normalize_rsi_alert_config(
                config,
                base=self._rsi_alert_config,
            )
        if persist:
            self._save_rsi_settings_to_storage()

    def get_alert_monitor_settings(self):
        return {
            'divergence': self.get_divergence_config(),
            'rsi': self.get_rsi_alert_config(),
            'point_monitor_mode': self._default_point_monitor_mode,
        }

    def _load_stock_template_settings_from_storage(self):
        self._ensure_runtime_setting_table()
        rows = db_manager.execute_query(
            f"SELECT setting_value FROM {_RUNTIME_SETTING_TABLE} WHERE setting_key = %s LIMIT 1",
            (_STOCK_TEMPLATE_SETTING_KEY,),
        )
        if not rows:
            return
        raw_value = rows[0].get('setting_value')
        if not raw_value:
            return
        try:
            payload = json.loads(raw_value)
        except Exception:
            return
        if payload.get('point_monitor_mode') is not None:
            self._default_point_monitor_mode = normalize_point_monitor_mode(
                payload.get('point_monitor_mode'),
                self._default_point_monitor_mode,
            )

    def _save_stock_template_settings_to_storage(self):
        self._ensure_runtime_setting_table()
        payload = {'point_monitor_mode': self._default_point_monitor_mode}
        db_manager.execute_delete(_RUNTIME_SETTING_TABLE, "setting_key = %s", (_STOCK_TEMPLATE_SETTING_KEY,))
        db_manager.execute_insert(
            _RUNTIME_SETTING_TABLE,
            {
                'setting_key': _STOCK_TEMPLATE_SETTING_KEY,
                'setting_value': json.dumps(payload, ensure_ascii=False),
            },
        )

    def update_alert_monitor_settings(
        self,
        *,
        divergence=None,
        rsi=None,
        point_monitor_mode=None,
        persist=True,
        reset_divergence_state=True,
    ):
        if divergence is not None:
            self.update_divergence_config(
                periods=divergence.get('periods'),
                scan_interval_seconds=divergence.get('scan_interval_seconds'),
                kline_count=divergence.get('kline_count'),
                lookback=divergence.get('lookback'),
                persist=False,
                reset_state=reset_divergence_state,
            )
        if rsi is not None:
            self.update_rsi_alert_config(rsi, persist=False)
        if point_monitor_mode is not None:
            self._default_point_monitor_mode = normalize_point_monitor_mode(point_monitor_mode)
        if persist:
            self._save_divergence_settings_to_storage()
            self._save_rsi_settings_to_storage()
            self._save_stock_template_settings_to_storage()

    def _check_divergence_alerts(self, stock):
        alerts = []
        stock_cfg = self.config.MONITOR_STOCKS.get(stock, {}) or {}
        stock_divergence_cfg = self._get_stock_divergence_config(stock_cfg)
        if not stock_divergence_cfg.get('enabled'):
            return alerts

        divergence_cfg = self.get_divergence_config()
        periods = stock_divergence_cfg.get('periods') or divergence_cfg.get('periods') or []
        scan_interval_seconds = int(
            stock_divergence_cfg.get('scan_interval_seconds')
            or divergence_cfg.get('scan_interval_seconds')
            or 60
        )
        kline_count = int(
            stock_divergence_cfg.get('kline_count')
            or divergence_cfg.get('kline_count')
            or 240
        )
        lookback = int(
            stock_divergence_cfg.get('lookback')
            or divergence_cfg.get('lookback')
            or 3
        )

        if not periods:
            return alerts

        now_ts = time.time()
        for period in periods:
            if not self._should_scan_divergence(stock, period, now_ts, scan_interval_seconds):
                continue

            kline_rows = fetch_kline_rows(stock, period, kline_count)
            if len(kline_rows) < max(60, lookback * 12):
                continue

            close_values = [item['close'] for item in kline_rows]
            macd_dif = calculate_macd_dif_series(close_values)
            macd_divergence = detect_divergence(kline_rows, macd_dif, period)
            chart_top = select_chart_divergence_points(macd_divergence['top'])
            chart_bottom = select_chart_divergence_points(macd_divergence['bottom'])

            candidates = []
            if stock_divergence_cfg.get('macd_enabled'):
                if stock_divergence_cfg.get('top_enabled'):
                    candidates.append(('MACD', 'top', chart_top[-1] if chart_top else None))
                if stock_divergence_cfg.get('bottom_enabled'):
                    candidates.append(('MACD', 'bottom', chart_bottom[-1] if chart_bottom else None))
            if not candidates:
                continue

            signal_items = []
            recent_start_index = max(0, len(kline_rows) - _DIVERGENCE_RECENT_BAR_LIMIT.get(period, 24))
            for indicator, divergence_type, point in candidates:
                if not point:
                    continue
                signal_index = int(point.get('index', -1))
                if signal_index < recent_start_index or signal_index >= len(kline_rows):
                    continue
                row = kline_rows[signal_index]
                signal_time = row.get('time')
                signal_key = f"{stock}|{period}|{indicator}|{divergence_type}|{signal_time}"
                signal_items.append((indicator, divergence_type, point, row, signal_key))

            # 首次扫描也允许最新信号触发，避免明显新信号被静默掉
            bootstrap_key = f"{stock}|{period}"
            if bootstrap_key not in self._divergence_bootstrapped:
                self._divergence_bootstrapped.add(bootstrap_key)

            for indicator, divergence_type, point, row, signal_key in signal_items:
                state_key = f"{stock}|{period}|{indicator}|{divergence_type}"
                if self._divergence_last_signal.get(state_key) == signal_key:
                    continue

                self._divergence_last_signal[state_key] = signal_key
                divergence_label = '顶背离' if divergence_type == 'top' else '底背离'
                period_label = self._format_period_label(period)
                signal_time = self._format_signal_time(row.get('time'))
                indicator_value = point.get('indicatorValue')
                price_value = point.get('priceValue')

                msg_parts = [f"{period_label}{indicator}{divergence_label}"]
                if signal_time:
                    msg_parts.append(f"时间:{signal_time}")
                if isinstance(price_value, (int, float)):
                    msg_parts.append(f"价格:{float(price_value):.2f}")
                if isinstance(indicator_value, (int, float)):
                    msg_parts.append(f"{indicator}:{float(indicator_value):.2f}")
                alert_message = " | ".join(msg_parts)

                alert_data = self._create_alert_data(
                    stock,
                    alert_message,
                    _DIVERGENCE_PERIOD_WINDOW_SECONDS.get(period, 0),
                    '背离',
                    trigger_time=self._parse_signal_time_to_datetime(row.get('time')),
                    chart_period=period,
                )
                cooldown = _DIVERGENCE_PERIOD_COOLDOWN_SECONDS.get(period, self.config.ALERT_COOLDOWN)
                alerts.append((alert_data, cooldown))

        return alerts

    def _to_bool(self, value, default=False):
        if value is None:
            return bool(default)
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        text = str(value).strip().lower()
        if text in {'1', 'true', 'yes', 'on'}:
            return True
        if text in {'0', 'false', 'no', 'off'}:
            return False
        return bool(default)

    def _parse_stock_divergence_periods(self, raw_value):
        if isinstance(raw_value, (list, tuple, set)):
            return self._normalize_divergence_periods(raw_value)
        if isinstance(raw_value, str) and raw_value.strip():
            try:
                parsed = json.loads(raw_value)
            except Exception:
                parsed = raw_value
            return self._normalize_divergence_periods(parsed)
        return None

    def _int_or_none(self, value):
        if value is None or value == '':
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _get_stock_divergence_config(self, stock_cfg):
        periods = self._parse_stock_divergence_periods(stock_cfg.get('divergence_periods'))
        scan_interval = self._int_or_none(stock_cfg.get('divergence_scan_interval_seconds'))
        kline_count = self._int_or_none(stock_cfg.get('divergence_kline_count'))
        lookback = self._int_or_none(stock_cfg.get('divergence_lookback'))
        return {
            'enabled': self._to_bool(stock_cfg.get('divergence_enabled'), False),
            'macd_enabled': self._to_bool(stock_cfg.get('divergence_macd_enabled'), True),
            'rsi_enabled': False,
            'top_enabled': self._to_bool(stock_cfg.get('divergence_top_enabled'), True),
            'bottom_enabled': self._to_bool(stock_cfg.get('divergence_bottom_enabled'), True),
            'periods': periods,
            'scan_interval_seconds': max(15, scan_interval) if scan_interval is not None else None,
            'kline_count': max(120, kline_count) if kline_count is not None else None,
            'lookback': max(2, lookback) if lookback is not None else None,
        }

    def _should_scan_divergence(self, stock, period, now_ts, scan_interval_seconds):
        key = f"{stock}|{period}"
        last_scan = self._divergence_last_scan_at.get(key, 0.0)
        if now_ts - last_scan < max(1, int(scan_interval_seconds)):
            return False
        self._divergence_last_scan_at[key] = now_ts
        return True

    def _format_period_label(self, period):
        return {
            'm1': '1分钟',
            'm5': '5分钟',
            'm15': '15分钟',
            'm30': '30分钟',
            'day': '日K',
            'week': '周K',
            'month': '月K',
        }.get(period, period)

    @staticmethod
    def _minute_window_to_chart_period(window):
        return {
            1: 'm1',
            5: 'm5',
            30: 'm30',
        }.get(int(window))

    def _normalize_stock_code_for_kline(self, stock_code):
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

    def _parse_jsonp_payload(self, text):
        try:
            return json.loads(text)
        except Exception:
            pass

        match = re.search(r'=\s*(\{.*\})\s*;?\s*$', str(text or ''), re.DOTALL)
        if not match:
            return None
        try:
            return json.loads(match.group(1))
        except Exception:
            return None

    def _fetch_kline_rows(self, stock_code, period, count):
        formatted_code = self._normalize_stock_code_for_kline(stock_code)
        if not formatted_code:
            return []

        if period in {'day', 'week', 'month'}:
            url = (
                f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?"
                f"param={formatted_code},{period},,,{count},qfq&_var=kline_{period}"
            )
            kline_key = f"qfq{period}"
        else:
            url = (
                f"https://ifzq.gtimg.cn/appstock/app/kline/mkline?"
                f"param={formatted_code},{period},,{count}&_var=kline_{period}"
            )
            kline_key = period

        try:
            response = requests.get(url, timeout=8)
            payload = self._parse_jsonp_payload(response.text)
            if not payload or payload.get('code') != 0:
                return []

            stock_data = payload.get('data', {}).get(formatted_code) or {}
            raw_rows = stock_data.get(kline_key) or stock_data.get(period) or []
            rows = []
            for item in raw_rows:
                if not isinstance(item, (list, tuple)) or len(item) < 5:
                    continue
                try:
                    close_value = float(item[2])
                    high_value = float(item[3])
                    low_value = float(item[4])
                except (TypeError, ValueError):
                    continue
                try:
                    open_value = float(item[1])
                except (TypeError, ValueError):
                    open_value = close_value
                rows.append({
                    'time': str(item[0]),
                    'open': open_value,
                    'close': close_value,
                    'high': high_value,
                    'low': low_value,
                })
            if period not in {'day', 'week', 'month'} and rows:
                factor_bars = max(120, min(2000, int(count or 240) // 3 + 80))
                factors = fetch_daily_forward_factors(formatted_code, factor_bars)
                rows = apply_forward_adjust_to_quote_rows(rows, factors)
            return rows
        except Exception as e:
            _logger.debug("拉取背离K线失败 stock=%s period=%s err=%s", stock_code, period, e)
            return []

    def _calculate_macd_dif_series(self, close_values, short_period=12, long_period=26, signal_period=9):
        return calculate_macd_dif_series(close_values, short_period, long_period, signal_period)

    def _calculate_rsi_series(self, close_values, period=14):
        length = len(close_values)
        if length == 0:
            return []
        if length == 1:
            return [None]

        gains = [0.0]
        losses = [0.0]
        for i in range(1, length):
            change = close_values[i] - close_values[i - 1]
            gains.append(change if change > 0 else 0.0)
            losses.append(abs(change) if change < 0 else 0.0)

        rsi = []
        for i in range(length):
            if i < period:
                rsi.append(None)
                continue
            gain_sum = 0.0
            loss_sum = 0.0
            for j in range(i - period + 1, i + 1):
                gain_sum += gains[j]
                loss_sum += losses[j]
            avg_gain = gain_sum / period
            avg_loss = loss_sum / period
            if avg_loss == 0:
                rsi.append(100.0)
            else:
                rs = avg_gain / avg_loss
                rsi.append(round(100 - 100 / (1 + rs), 2))
        return rsi

    def _detect_divergence(self, price_rows, indicator_values, lookback_period=3):
        top_divergence = []
        bottom_divergence = []
        price_highs_raw = [item.get('high', item.get('close')) for item in price_rows]
        price_lows_raw = [item.get('low', item.get('close')) for item in price_rows]
        indicator_align_tolerance = 2

        def find_local_extremes(values, is_max=True):
            extremes = []
            for i in range(lookback_period, len(values) - lookback_period):
                if values[i] is None:
                    continue
                is_extreme = True
                for j in range(1, lookback_period + 1):
                    left = values[i - j]
                    right = values[i + j]
                    if left is None or right is None:
                        is_extreme = False
                        break
                    if is_max:
                        if values[i] < left or values[i] < right:
                            is_extreme = False
                            break
                    else:
                        if values[i] > left or values[i] > right:
                            is_extreme = False
                            break
                if is_extreme:
                    extremes.append({'index': i, 'value': values[i]})
            return extremes

        price_highs = find_local_extremes(price_highs_raw, True)
        price_lows = find_local_extremes(price_lows_raw, False)
        indicator_highs = find_local_extremes(indicator_values, True)
        indicator_lows = find_local_extremes(indicator_values, False)

        def find_nearest_indicator_extreme(extremes, target_index):
            candidates = [
                item for item in extremes
                if abs(int(item.get('index', -9999)) - int(target_index)) <= indicator_align_tolerance
            ]
            if not candidates:
                return None
            candidates.sort(
                key=lambda item: (
                    abs(int(item.get('index', 0)) - int(target_index)),
                    int(item.get('index', 0))
                )
            )
            return candidates[0]

        for i in range(1, len(price_highs)):
            current_price_high = price_highs[i]
            prev_price_high = price_highs[i - 1]
            if current_price_high['value'] <= prev_price_high['value']:
                continue
            prev_indicator_high = find_nearest_indicator_extreme(indicator_highs, prev_price_high['index'])
            current_indicator_high = find_nearest_indicator_extreme(indicator_highs, current_price_high['index'])
            if (
                prev_indicator_high and
                current_indicator_high and
                current_indicator_high['index'] > prev_indicator_high['index'] and
                current_indicator_high['value'] < prev_indicator_high['value']
            ):
                top_divergence.append({
                    'index': current_price_high['index'],
                    'priceValue': current_price_high['value'],
                    'indicatorValue': current_indicator_high['value'],
                })
                continue

            matched = [
                h for h in indicator_highs
                if prev_price_high['index'] - indicator_align_tolerance <= h['index'] <= current_price_high['index'] + indicator_align_tolerance
            ]
            if len(matched) < 2:
                continue
            last_two = matched[-2:]
            if last_two[1]['value'] < last_two[0]['value']:
                top_divergence.append({
                    'index': current_price_high['index'],
                    'priceValue': current_price_high['value'],
                    'indicatorValue': last_two[1]['value'],
                })

        for i in range(1, len(price_lows)):
            current_price_low = price_lows[i]
            prev_price_low = price_lows[i - 1]
            if current_price_low['value'] >= prev_price_low['value']:
                continue
            prev_indicator_low = find_nearest_indicator_extreme(indicator_lows, prev_price_low['index'])
            current_indicator_low = find_nearest_indicator_extreme(indicator_lows, current_price_low['index'])
            if (
                prev_indicator_low and
                current_indicator_low and
                current_indicator_low['index'] > prev_indicator_low['index'] and
                current_indicator_low['value'] > prev_indicator_low['value']
            ):
                bottom_divergence.append({
                    'index': current_price_low['index'],
                    'priceValue': current_price_low['value'],
                    'indicatorValue': current_indicator_low['value'],
                })
                continue

            matched = [
                l for l in indicator_lows
                if prev_price_low['index'] - indicator_align_tolerance <= l['index'] <= current_price_low['index'] + indicator_align_tolerance
            ]
            if len(matched) < 2:
                continue
            last_two = matched[-2:]
            if last_two[1]['value'] > last_two[0]['value']:
                bottom_divergence.append({
                    'index': current_price_low['index'],
                    'priceValue': current_price_low['value'],
                    'indicatorValue': last_two[1]['value'],
                })

        return {'top': top_divergence, 'bottom': bottom_divergence}

    def _format_signal_time(self, raw_time):
        value = str(raw_time or '').strip()
        if not value:
            return ''
        if re.fullmatch(r'\d{12}', value):
            return f"{value[0:4]}-{value[4:6]}-{value[6:8]} {value[8:10]}:{value[10:12]}"
        if re.fullmatch(r'\d{8}', value):
            return f"{value[0:4]}-{value[4:6]}-{value[6:8]}"
        return value

    def _parse_signal_time_to_datetime(self, raw_time):
        value = str(raw_time or '').strip()
        if not value:
            return None
        if re.fullmatch(r'\d{12}', value):
            return datetime(
                int(value[0:4]),
                int(value[4:6]),
                int(value[6:8]),
                int(value[8:10]),
                int(value[10:12]),
            )
        if re.fullmatch(r'\d{8}', value):
            return datetime(int(value[0:4]), int(value[4:6]), int(value[6:8]))
        formatted = self._format_signal_time(value)
        if formatted and formatted != value:
            try:
                if len(formatted) >= 16:
                    return datetime.strptime(formatted[:16], '%Y-%m-%d %H:%M')
                if len(formatted) >= 10:
                    return datetime.strptime(formatted[:10], '%Y-%m-%d')
            except ValueError:
                return None
        try:
            return datetime.strptime(value[:19], '%Y-%m-%d %H:%M:%S')
        except ValueError:
            pass
        try:
            return datetime.strptime(value[:16], '%Y-%m-%d %H:%M')
        except ValueError:
            return None

    def _is_price_in_trigger_range(self, stock, current_price):
        cfg = self.config.MONITOR_STOCKS.get(stock, {}) or {}
        min_price = cfg.get('trigger_min_price')
        max_price = cfg.get('trigger_max_price')

        try:
            min_price = float(min_price) if min_price is not None else None
        except (TypeError, ValueError):
            min_price = None
        try:
            max_price = float(max_price) if max_price is not None else None
        except (TypeError, ValueError):
            max_price = None

        if min_price is not None and current_price < min_price:
            return False
        if max_price is not None and current_price > max_price:
            return False
        return True

    def _check_time_window_conditions(self, stock, window_sec):
        triggered_conditions = []
        candles = self.stock_data.get_stock_data(stock)
        stock_cfg = self.config.MONITOR_STOCKS.get(stock, {}) or {}
        price_alerts = self._check_price_movement(stock, candles, window_sec, stock_cfg)
        triggered_conditions.extend(price_alerts)
        return triggered_conditions

    def _check_price_movement(self, stock, price_array, window_sec, thresholds_config):
        if not price_array:
            return []

        window_str = str(window_sec)
        thresholds =     {
                            "price_drop": [
                              -0.7
                            ],
                            "price_rise": [
                              0.7
                            ]
                          }

        window_length = window_sec // self.config.BASE_INTERVAL

        recent_prices = price_array[-window_length:] if len(price_array) >= window_length else price_array
        prices = [candle['close'] for candle in recent_prices]

        if not prices:
            return []

        highest_price = max(prices)
        lowest_price = min(prices)
        current_price = prices[-1]

        drawdown_from_high = (current_price - highest_price) / highest_price * 100
        gain_from_low = (current_price - lowest_price) / lowest_price * 100

        return self._create_movement_alerts(stock, window_sec, drawdown_from_high, gain_from_low, thresholds)

    def _create_movement_alerts(self, stock, window_sec, drawdown, gain, thresholds):
        """创建价格变动警报"""
        alerts = []

        # 检查下跌阈值
        for threshold in thresholds.get("price_drop", []):
            if drawdown <= threshold:
                alerts.append(self._create_alert_data(
                    stock, f"price_drop_{abs(threshold)}%_{window_sec}s", window_sec
                ))

        # 检查上涨阈值
        for threshold in thresholds.get("price_rise", []):
            if gain >= threshold:
                alerts.append(self._create_alert_data(
                    stock, f"price_rise_{threshold}%_{window_sec}s", window_sec
                ))

        return alerts

    def _check_price_thresholds(self, stock, candles=None):
        candles = candles or self.stock_data.get_stock_data(stock)
        if not candles:
            return []
        current_price = candles[-1]['close']
        stock_cfg = self.config.MONITOR_STOCKS.get(stock, {}) or {}
        price_thresholds = stock_cfg.get("price_thresholds", [])
        alerts = []

        for threshold_config in price_thresholds:
            try:
                threshold_price = float(threshold_config["price"])
            except (TypeError, ValueError, KeyError):
                continue
            direction = str(threshold_config.get("direction") or "").strip().lower()
            if direction not in {"above", "below"}:
                continue
            alert_id = f"price_{direction}_{threshold_price}"

            condition_met = (
                    (direction == "above" and current_price >= threshold_price) or
                    (direction == "below" and current_price <= threshold_price)
            )

            if condition_met:
                direction_label = "上涨到" if direction == "above" else "下跌到"
                alert_message = f"价格{direction_label}{threshold_price:.2f} | 当前价:{current_price:.2f}"
                alerts.append(self._create_alert_data(stock, alert_message, alert_type='价格告警'))

        return alerts

    def _check_ma_breakdown(self, stock):
        triggered_alerts = []
        stock_cfg = self.config.MONITOR_STOCKS.get(stock, {}) or {}
        if not stock_cfg:
            return triggered_alerts
        if not stock_cfg.get("break_ma", True):
            return triggered_alerts

        candles = self.stock_data.get_stock_data(stock)
        if not candles:
            return triggered_alerts

        data = IndexAnalysis.my_pro_bar(stock)
        ma = IndexAnalysis.calculate_realtime_ma(data, candles[-1])
        current_price = candles[-1]['low']
        ma_types = stock_cfg.get("ma_types", [5, 10, 20, 30, 60, 120])

        for ma_type in ma_types:
            ma_key = f"ma{ma_type}"
            if ma_key not in ma:
                continue

            ma_value = ma[ma_key]
            if current_price <= ma_value < candles[-1]['pre_close']:
                alert_message = f"{stock} break{ma_type}ma"
                alert_data = self._create_alert_data(stock, alert_message)
                # 保留冷却时间逻辑
                triggered_alerts.append((alert_data, 1000 * 100))

        return triggered_alerts

    def _check_change_thresholds(self, stock, candles=None):
        stock_cfg = self.config.MONITOR_STOCKS.get(stock, {}) or {}
        change_thresholds = stock_cfg.get("change_thresholds", [])
        if not change_thresholds:
            return []

        candles = candles or self.stock_data.get_stock_data(stock)
        if not candles:
            return []

        current_price = candles[-1]['close']
        pre_close = candles[-1]['pre_close']
        change_percent = (current_price - pre_close) / pre_close * 100
        alerts = []

        for threshold_config in change_thresholds:
            threshold_change = threshold_config["change"]
            direction = threshold_config["direction"]
            alert_id = f"change_{direction}_{threshold_change}"

            condition_met = (
                    (direction == "above" and change_percent >= threshold_change) or
                    (direction == "below" and change_percent <= threshold_change)
            )

            if condition_met:
                alert_message = f"change_threshold_{alert_id}"
                alerts.append(self._create_alert_data(stock, alert_message))

        return alerts

    def _check_common_by_min(self, stock, window=1):
        results_min = IndexAnalysis.rt_min(stock, window)
        if results_min is None or results_min.empty:
            return []

        if not self._is_new_candle_data(stock, window, results_min):
            return []

        if len(results_min) < 4:
            return []

        return self._analyze_technical_patterns(stock, window, results_min)

    def _resolve_rsi_period_config(self, stock, window):
        stock_cfg = self.config.MONITOR_STOCKS.get(stock, {}) or {}
        return get_rsi_period_config(stock_cfg, window, self.get_rsi_alert_config())

    def _analyze_technical_patterns(self, stock, window, results_min):
        """分析技术形态模式"""
        alerts = []
        last_four = results_min.iloc[-4:]
        last_k, prev_k, prev_prev_k = last_four.iloc[-2], last_four.iloc[-3], last_four.iloc[-4]

        # RSI分析
        rsi_alerts = self._analyze_rsi_patterns(stock, window, results_min, last_k, prev_k, prev_prev_k)
        alerts.extend(rsi_alerts)

        # K线形态分析
        pattern_alerts = self._analyze_candle_patterns(stock, window, last_k, prev_k, prev_prev_k, results_min)
        alerts.extend(pattern_alerts)

        return alerts

    def _analyze_rsi_patterns(self, stock, window, results_min, last_k, prev_k, prev_prev_k):
        """分析RSI相关模式"""
        alerts = []
        period_cfg = self._resolve_rsi_period_config(stock, window)
        if not period_cfg:
            return alerts

        rsi_6 = IndicatorCalculation.calculate_rsi(results_min[:-1], 6).__round__(1)
        pre_rsi_6 = IndicatorCalculation.calculate_rsi(results_min[:-2], 6).__round__(1)

        # RSI边界警报
        rsi_alert = self._check_rsi_boundary(stock, window, rsi_6, pre_rsi_6, period_cfg)
        if rsi_alert:
            alerts.append(rsi_alert)

        # RSI极端值模式
        extreme_alerts = self._check_rsi_extreme_patterns(stock, window, pre_rsi_6, last_k, prev_k, period_cfg)
        alerts.extend(extreme_alerts)

        return alerts

    def _get_rsi_thresholds(self, period_cfg):
        if not period_cfg:
            return None, None
        return float(period_cfg['low']), float(period_cfg['high'])

    def _check_rsi_boundary(self, stock, window, rsi_6, pre_rsi_6, period_cfg):
        """检查RSI边界条件"""
        low_threshold, high_threshold = self._get_rsi_thresholds(period_cfg)
        if low_threshold is None or high_threshold is None:
            return None

        state_key = f"{stock}_{window}"
        if state_key not in self._rsi_trigger_states:
            self._rsi_trigger_states[state_key] = {'last_rsi_triggered': False}

        current_state = self._rsi_trigger_states[state_key]
        boundary_low_enabled = bool(period_cfg.get('boundary_low'))
        boundary_high_enabled = bool(period_cfg.get('boundary_high'))
        if not boundary_low_enabled and not boundary_high_enabled:
            current_state['last_rsi_triggered'] = False
            return None

        if not low_threshold <= rsi_6 <= high_threshold:
            is_low = rsi_6 <= low_threshold
            is_high = rsi_6 >= high_threshold
            if (is_low and not boundary_low_enabled) or (is_high and not boundary_high_enabled):
                current_state['last_rsi_triggered'] = False
                return None

            is_consecutive_trigger = (
                pre_rsi_6 is not None and not low_threshold <= pre_rsi_6 <= high_threshold
            )

            if not is_consecutive_trigger or not current_state['last_rsi_triggered']:
                current_state['last_rsi_triggered'] = True
                alert_type = '买点' if is_low else '卖点'
                return self._create_alert_data(
                    stock,
                    f"({window}min)rsi_6:{rsi_6}",
                    window,
                    alert_type,
                    chart_period=self._minute_window_to_chart_period(window),
                )
            current_state['last_rsi_triggered'] = True
        else:
            current_state['last_rsi_triggered'] = False

        return None

    def _check_rsi_extreme_patterns(self, stock, window, pre_rsi_6, last_k, prev_k, period_cfg):
        """检查RSI极端值的K线模式"""
        alerts = []
        low_threshold, high_threshold = self._get_rsi_thresholds(period_cfg)
        if low_threshold is None or high_threshold is None:
            return alerts

        if period_cfg.get('reversal_low') and pre_rsi_6 <= low_threshold and self._is_bullish_reversal(last_k, prev_k):
            alerts.append(self._create_alert_data(
                stock,
                f"({window}min)rsi_6_up",
                window,
                '买点',
                chart_period=self._minute_window_to_chart_period(window),
            ))

        if period_cfg.get('reversal_high') and pre_rsi_6 >= high_threshold and self._is_bearish_reversal(last_k, prev_k):
            alerts.append(self._create_alert_data(
                stock,
                f"({window}min)rsi_6_down",
                window,
                '卖点',
                chart_period=self._minute_window_to_chart_period(window),
            ))

        return alerts

    def _analyze_candle_patterns(self, stock, window, last_k, prev_k, prev_prev_k, results_min):
        """分析K线形态模式"""
        alerts = []
        period_cfg = self._resolve_rsi_period_config(stock, window)
        if not period_cfg or not period_cfg.get('engulfing'):
            return alerts

        rsi_6 = None
        try:
            rsi_6 = float(IndicatorCalculation.calculate_rsi(results_min[:-1], 6))
        except Exception:
            rsi_6 = None

        engulfing_alert = self._check_engulfing_pattern(stock, window, last_k, prev_k, rsi_6, period_cfg)
        if engulfing_alert:
            alerts.append(engulfing_alert)

        triple_pattern_alerts = self._check_triple_candle_patterns(stock, window, last_k, prev_k, prev_prev_k)
        alerts.extend(triple_pattern_alerts)

        return alerts

    def _check_engulfing_pattern(self, stock, window, last_k, prev_k, rsi_6=None, period_cfg=None):
        """检查吞没形态"""
        if rsi_6 is None or not period_cfg:
            return None

        low_threshold, high_threshold = self._get_rsi_thresholds(period_cfg)
        if low_threshold is None or high_threshold is None:
            return None

        if (last_k['open'] < prev_k['close'] < prev_k['open'] < last_k['close'] and
                last_k['close'] > last_k['open'] and last_k['amount'] > prev_k['amount'] and
                rsi_6 < low_threshold):
            return self._create_alert_data(
                stock,
                f"({window}min)engulfing_up",
                window,
                '买点',
                chart_period=self._minute_window_to_chart_period(window),
            )

        if (last_k['open'] > prev_k['close'] > prev_k['open'] > last_k['close'] and
              last_k['close'] < last_k['open'] and last_k['amount'] > prev_k['amount'] and
              rsi_6 > high_threshold):
            return self._create_alert_data(
                stock,
                f"({window}min)engulfing_down",
                window,
                '卖点',
                chart_period=self._minute_window_to_chart_period(window),
            )

        return None

    def _is_new_candle_data(self, stock, window, results_min=None):
        """检查是否有新的K线数据"""
        if results_min is None:
            results_min = IndexAnalysis.rt_min(stock, window)
        if results_min is None or results_min.empty:
            return False

        current_time = results_min.iloc[-1]['candle_end_time']
        state_key = f"{stock}_{window}"
        last_time = self._last_candle_times.get(state_key)

        if last_time is None or current_time > last_time:
            self._last_candle_times[state_key] = current_time
            return True
        return False

    def _check_triple_candle_patterns(self, stock, window, last_k, prev_k, prev_prev_k):
        """检查三根K线组合模式（已停用）"""
        return []

    def _is_bullish_reversal(self, last_k, prev_k):
        """判断是否看涨反转形态"""
        return (prev_k['close'] < prev_k['open'] and  # 前一根阴线
                last_k['close'] >= last_k['open'] and  # 当前阳线
                last_k['amount'] > prev_k['amount'])  # 放量

    def _is_bearish_reversal(self, last_k, prev_k):
        """判断是否看跌反转形态"""
        return (prev_k['close'] > prev_k['open'] and  # 前一根阳线
                last_k['close'] <= last_k['open'] and  # 当前阴线
                last_k['amount'] > prev_k['amount'])  # 放量

    def _create_alert_data(
        self,
        stock,
        alert_message,
        window_sec=None,
        alert_type='观察',
        trigger_time=None,
        chart_period=None,
    ):
        """创建统一的警报数据结构"""
        alert_data = {
            'stock_code': stock,
            'stock_name': get_stock_name(stock),
            'alert_type': alert_type,
            'alert_level': 2,
            'alert_message': alert_message,
            'trigger_time': trigger_time or datetime.now()
        }

        if window_sec is not None:
            alert_data['windows_sec'] = window_sec
        if chart_period is not None:
            alert_data['chart_period'] = chart_period

        return alert_data

    def calculate_ma_distances(self, stock_list):
        all_distances = {}

        for stock in stock_list:
            candles = self.stock_data.get_stock_data(stock)
            if not candles:
                continue

            ma_distances = self._calculate_single_stock_ma_distances(stock, candles)
            if ma_distances:
                all_distances[stock] = ma_distances

        return all_distances

    def _calculate_single_stock_ma_distances(self, stock, candles):
        data = IndexAnalysis.my_pro_bar(stock)
        ma = IndexAnalysis.calculate_realtime_ma(data, candles[-1])
        current_price = candles[-1]['close']

        stock_config = self.config.MONITOR_STOCKS.setdefault(stock, {})
        ma_types = stock_config.get("ma_types", [5, 10, 20, 30, 60, 120])
        ma_distances = {}

        for ma_type in ma_types:
            ma_key = f"ma{ma_type}"
            if ma_key not in ma or ma[ma_key] <= 0:
                continue

            ma_value = ma[ma_key]
            price_diff = current_price - ma_value
            percent_diff = (price_diff / ma_value) * 100

            ma_distances[ma_type] = {
                "diff": round(price_diff, 2),
                "percent": round(percent_diff, 2),
                "current_price": round(current_price, 2),
                "ma_value": round(ma_value, 2)
            }

        return ma_distances