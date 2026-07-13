import logging
import threading
from collections import OrderedDict

from utils.send_alert_message import send_alert_message
from utils.GetStockData import get_stock_name
from monitor.config.db_monitor import db_manager, stock_alert_dao
from monitor.config.market_calendar import is_alert_time_allowed
from monitor.config.market_time import now_in_market_tz

_logger = logging.getLogger(__name__)
_RUNTIME_SETTING_TABLE = 'monitor_runtime_settings'
_ALERT_PUSH_MUTE_SETTING_KEY = 'alert_push_muted'


class AlertSender:
    def __init__(self, config):
        self.config = config
        self.last_alert_time = {}
        self._send_lock = threading.Lock()
        self._push_muted = self._load_push_muted_from_storage()

        for stock in self.config.MONITOR_STOCKS.keys():
            self.last_alert_time[stock] = {}

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
        except Exception as exc:
            _logger.warning("确保运行时配置表失败: %s", exc)
        finally:
            if cursor:
                cursor.close()

    def _load_push_muted_from_storage(self):
        self._ensure_runtime_setting_table()
        rows = db_manager.execute_query(
            f"SELECT setting_value FROM {_RUNTIME_SETTING_TABLE} WHERE setting_key = %s LIMIT 1",
            (_ALERT_PUSH_MUTE_SETTING_KEY,),
        )
        if not rows:
            return False
        return str(rows[0].get('setting_value') or '').strip().lower() in {'1', 'true', 'yes', 'on'}

    def set_push_muted(self, muted, persist=True):
        with self._send_lock:
            self._push_muted = bool(muted)
            if persist:
                self._ensure_runtime_setting_table()
                db_manager.execute_delete(
                    _RUNTIME_SETTING_TABLE,
                    "setting_key = %s",
                    (_ALERT_PUSH_MUTE_SETTING_KEY,),
                )
                db_manager.execute_insert(
                    _RUNTIME_SETTING_TABLE,
                    {
                        'setting_key': _ALERT_PUSH_MUTE_SETTING_KEY,
                        'setting_value': '1' if self._push_muted else '0',
                    },
                )
            return self._push_muted

    def is_push_muted(self):
        return bool(self._push_muted)

    @staticmethod
    def _format_trigger_time(value):
        if hasattr(value, 'strftime'):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        text = str(value or '').strip()
        return text or '--'

    @staticmethod
    def _pick_chart_period(alerts):
        preferred_order = ('m1', 'm5', 'm15', 'm30', 'day', 'week', 'month')
        periods = []
        for item in alerts or []:
            period = str(item.get('chart_period') or '').strip().lower()
            if period and period not in periods:
                periods.append(period)
        if not periods:
            return None
        for preferred in preferred_order:
            if preferred in periods:
                return preferred
        return periods[0]

    def _group_alerts_by_stock(self, alerts):
        grouped = OrderedDict()
        for alert_data in alerts or []:
            stock_code = str(alert_data.get('stock_code') or '').strip() or 'UNKNOWN'
            grouped.setdefault(stock_code, []).append(alert_data)
        return grouped

    def _build_round_push_message(self, alerts, trigger_time):
        """将同一轮轮询触发的多股票告警合并成一条推送文案。"""
        grouped = self._group_alerts_by_stock(alerts)
        stock_count = len(grouped)
        alert_count = len(alerts or [])
        time_text = self._format_trigger_time(trigger_time)

        if stock_count <= 1 and alert_count <= 1:
            only = (alerts or [None])[0] or {}
            stock_name = str(only.get('stock_name') or '').strip() or get_stock_name(only.get('stock_code')) or ''
            stock_code = str(only.get('stock_code') or '').strip()
            header = f"【{stock_name or stock_code}】{stock_code} · 告警"
        elif stock_count <= 1:
            only_code = next(iter(grouped.keys()))
            only_name = str((grouped[only_code][0] or {}).get('stock_name') or '').strip() or get_stock_name(only_code) or only_code
            header = f"【{only_name}】{only_code} · 同时触发 {alert_count} 条告警"
        else:
            header = f"【轮询告警】{stock_count} 只股票 · 共 {alert_count} 条"

        lines = [
            header,
            f"时间：{time_text}",
            "────────",
        ]

        global_index = 1
        for stock_idx, (stock_code, stock_alerts) in enumerate(grouped.items()):
            stock_name = str((stock_alerts[0] or {}).get('stock_name') or '').strip() or get_stock_name(stock_code) or stock_code
            if stock_count > 1:
                lines.append(f"■ {stock_name} {stock_code}")
            for alert_data in stock_alerts:
                alert_type = str(alert_data.get('alert_type') or '观察').strip() or '观察'
                alert_message = str(alert_data.get('alert_message') or '').strip() or '--'
                prefix = "  " if stock_count > 1 else ""
                lines.append(f"{prefix}[{global_index}] {alert_type}")
                lines.append(f"{prefix}    {alert_message}")
                global_index += 1
            if stock_idx < stock_count - 1:
                lines.append("")

        return "\n".join(lines)

    def prepare_alerts(self, stock, alerts_with_cooldown, force_send=False, current_time=None):
        """
        过滤冷却并补齐字段，返回本轮可入库/可推送的告警列表。
        不写库、不推送，供一轮轮询汇总后再统一 flush。
        """
        if not force_send and not self._is_alert_time_allowed():
            return []
        current_time = current_time or now_in_market_tz().replace(tzinfo=None)
        valid_alerts = []
        stock_alert_state = self.last_alert_time.setdefault(stock, {})

        for alert_item in alerts_with_cooldown or []:
            if isinstance(alert_item, tuple) and len(alert_item) >= 2:
                alert_data, cooldown = alert_item
            else:
                alert_data = alert_item
                cooldown = self.config.ALERT_COOLDOWN

            if not isinstance(cooldown, (int, float)) or cooldown <= 0:
                cooldown = self.config.ALERT_COOLDOWN

            alert_message = alert_data['alert_message']
            last_trigger = stock_alert_state.get(alert_message)
            elapsed = (current_time - last_trigger).total_seconds() if last_trigger else cooldown
            if not last_trigger or elapsed >= cooldown:
                prepared = dict(alert_data)
                prepared.setdefault('trigger_time', current_time)
                prepared.setdefault('stock_name', get_stock_name(stock))
                prepared.setdefault('stock_code', stock)
                valid_alerts.append(prepared)
                stock_alert_state[alert_message] = current_time

        return valid_alerts

    def flush_round_alerts(self, alerts):
        """同一轮轮询的告警：逐条入库，合并成一条推送。"""
        if not alerts:
            return

        push_alerts = []
        with self._send_lock:
            for alert_data in alerts:
                prepared = dict(alert_data)
                prepared.setdefault('trigger_time', now_in_market_tz().replace(tzinfo=None))
                prepared.setdefault('stock_name', get_stock_name(prepared.get('stock_code')))
                prepared.setdefault('stock_code', prepared.get('stock_code'))

                if stock_alert_dao.has_duplicate_alert(
                    prepared['stock_code'],
                    prepared['alert_message'],
                    prepared['trigger_time'],
                ):
                    _logger.info(
                        "跳过重复告警推送: stock=%s trigger_time=%s",
                        prepared['stock_code'],
                        prepared['trigger_time'],
                    )
                    continue

                alert_id = stock_alert_dao.insert_alert(prepared)
                if not alert_id:
                    _logger.error(
                        "告警入库失败，已跳过推送: stock=%s trigger_time=%s message=%s",
                        prepared.get('stock_code'),
                        prepared.get('trigger_time'),
                        prepared.get('alert_message'),
                    )
                    continue
                push_alerts.append(prepared)

            if not push_alerts:
                return

            chart_period = self._pick_chart_period(push_alerts)
            chart_stock = str(push_alerts[0].get('stock_code') or '').strip()
            for item in push_alerts:
                item.pop('chart_period', None)

            stock_count = len({str(item.get('stock_code') or '') for item in push_alerts})
            if self._push_muted:
                _logger.info(
                    "告警推送已静默，仅记录入库: stocks=%s alerts=%s",
                    stock_count,
                    len(push_alerts),
                )
                return

            alert_info = self._build_round_push_message(
                push_alerts,
                push_alerts[0].get('trigger_time'),
            )
            send_alert_message(alert_info, chart_stock, chart_period=chart_period)

    def send_alert(self, stock, alerts_with_cooldown, force_send=False):
        """兼容单股票即时发送（例如手动触发）。"""
        prepared = self.prepare_alerts(stock, alerts_with_cooldown, force_send=force_send)
        self.flush_round_alerts(prepared)

    def _is_alert_time_allowed(self):
        """仅在交易日连续竞价时段触发并入库。"""
        return is_alert_time_allowed()
