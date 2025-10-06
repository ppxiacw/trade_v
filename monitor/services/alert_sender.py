from datetime import datetime
from utils.send_dingding import send_dingtalk_message
from utils.GetStockData import get_stock_name


class AlertSender:
    def __init__(self, config):
        self.config = config
        self.alerts_history = []
        self.last_alert_time = {}

        for stock in self.config.MONITOR_STOCKS.keys():
            self.last_alert_time[stock] = {}

    def send_alert(self, stock, alerts_with_cooldown):
        current_time = datetime.now()
        valid_alerts = []

        for alert_item in alerts_with_cooldown:
            # 判断 alert_item 是否为 (message, cooldown) 元组
            if isinstance(alert_item, tuple) and len(alert_item) >= 2:
                alert_msg, cooldown = alert_item
            else:
                # 仅消息，使用默认冷却时间
                alert_msg = alert_item
                cooldown = self.config.ALERT_COOLDOWN

            # 如果 cooldown 无效 (为 None 或非正数)，使用默认冷却时间
            if not isinstance(cooldown, (int, float)) or cooldown <= 0:
                cooldown = self.config.ALERT_COOLDOWN

            last_trigger = self.last_alert_time[stock].get(alert_msg)

            # 判断是否已经过了冷却时间
            if not last_trigger or (current_time - last_trigger).seconds >= cooldown:
                valid_alerts.append(alert_msg)
                self.last_alert_time[stock][alert_msg] = current_time

        if not valid_alerts:
            return

        for alert_msg in valid_alerts:
            alert_info = f"{get_stock_name(stock)} {alert_msg} 警报 {current_time.strftime('%H:%M:%S')}"
            self.alerts_history.append(alert_info)

            if self.config.DEBUG_MODE:
                print(f"[DEBUG] [发送] {alert_info}")
            else:
                send_dingtalk_message(alert_info, stock)


    def get_alert_history(self):
        return self.alerts_history
