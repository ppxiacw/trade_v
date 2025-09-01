from datetime import datetime
from utils.send_dingding import send_dingtalk_message
from utils.GetStockData import result_dict


class AlertSender:
    def __init__(self, config):
        self.config = config
        self.alerts_history = []
        self.last_alert_time = {}

        for stock in self.config.MONITOR_STOCKS.keys():
            self.last_alert_time[stock] = {}

    def send_alert(self, stock, window_sec, conditions):
        current_time = datetime.now()
        valid_conditions = []

        for condition in conditions:
            last_trigger_time = self.last_alert_time[stock].get(condition)

            if last_trigger_time is None or \
                    (current_time - last_trigger_time).total_seconds() > self.config.ALERT_COOLDOWN:
                valid_conditions.append(condition)
                self.last_alert_time[stock][condition] = current_time

        if not valid_conditions:
            return

        price_alerts = []
        for condition in valid_conditions:
            if condition.startswith("price_drop_"):
                parts = condition.split("_")
                if len(parts) >= 3:
                    threshold = parts[2]
                    window_str = "_".join(parts[3:]) if len(parts) > 3 else f"{window_sec}秒"
                    price_alerts.append(f"下跌{threshold}%({window_str})")
            elif condition.startswith("price_rise_"):
                parts = condition.split("_")
                if len(parts) >= 3:
                    threshold = parts[2]
                    window_str = "_".join(parts[3:]) if len(parts) > 3 else f"{window_sec}秒"
                    price_alerts.append(f"上涨{threshold}%({window_str})")
            else:
                price_alerts.append(condition)

        conditions_str = "、".join(price_alerts)
        alert_info = f"{self.get_stock_name(stock)} {conditions_str}警报 {current_time.strftime('%H:%M:%S')}"
        self.alerts_history.append(alert_info)

        if self.config.DEBUG_MODE:
            print(f"[DEBUG] 警报触发: {alert_info}")
        else:
            send_dingtalk_message(alert_info, stock)

    def get_stock_name(self, stock_code):
        try:
            return result_dict[stock_code]['name']
        except Exception as e:
            print(f"获取股票名称失败: {e}")
            return stock_code

    def get_alert_history(self):
        return self.alerts_history