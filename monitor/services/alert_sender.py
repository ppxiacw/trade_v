import logging
import threading
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

    def send_alert(self, stock, alerts_with_cooldown, force_send=False):
        if not force_send and not self._is_alert_time_allowed():
            return
        current_time = now_in_market_tz().replace(tzinfo=None)
        valid_alerts = []
        stock_alert_state = self.last_alert_time.setdefault(stock, {})

        for alert_item in alerts_with_cooldown:
            # 判断 alert_item 是否为 (alert_data, cooldown) 元组（带冷却时间）
            if isinstance(alert_item, tuple) and len(alert_item) >= 2:
                alert_data, cooldown = alert_item
            else:
                # 只有alert_data，使用默认冷却时间
                alert_data = alert_item
                cooldown = self.config.ALERT_COOLDOWN

            # 如果 cooldown 无效 (为 None 或非正数)，使用默认冷却时间
            if not isinstance(cooldown, (int, float)) or cooldown <= 0:
                cooldown = self.config.ALERT_COOLDOWN

            # 使用alert_message作为冷却时间的键
            alert_message = alert_data['alert_message']
            last_trigger = stock_alert_state.get(alert_message)

            # 判断是否已经过了冷却时间
            elapsed = (current_time - last_trigger).total_seconds() if last_trigger else cooldown
            if not last_trigger or elapsed >= cooldown:
                valid_alerts.append(alert_data)
                stock_alert_state[alert_message] = current_time

        if not valid_alerts:
            return

        for alert_data in valid_alerts:
            # 确保alert_data中有所有必需的字段
            if 'trigger_time' not in alert_data:
                alert_data['trigger_time'] = current_time
            if 'stock_name' not in alert_data:
                alert_data['stock_name'] = get_stock_name(stock)
            if 'stock_code' not in alert_data:
                alert_data['stock_code'] = stock

            with self._send_lock:
                if stock_alert_dao.has_duplicate_alert(
                    alert_data['stock_code'],
                    alert_data['alert_message'],
                    alert_data['trigger_time'],
                ):
                    _logger.info(
                        "跳过重复告警推送: stock=%s trigger_time=%s",
                        alert_data['stock_code'],
                        alert_data['trigger_time'],
                    )
                    continue

                # 构建显示消息
                alert_info = f"{alert_data['stock_name']} {alert_data['alert_message']} 警报 {alert_data['trigger_time']}"
                chart_period = alert_data.pop('chart_period', None)

                if self._push_muted:
                    _logger.info("告警推送已静默，仅记录入库: stock=%s", alert_data['stock_code'])
                else:
                    send_alert_message(alert_info, stock, chart_period=chart_period)

                stock_alert_dao.insert_alert(alert_data)

    def _is_alert_time_allowed(self):
        """仅在交易日连续竞价时段触发并入库。"""
        return is_alert_time_allowed()

