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

        conditions_str = "、".join(valid_conditions)
        alert_info = f"{self.get_stock_name(stock)} {conditions_str}警报 {current_time.strftime('%H:%M:%S')}"
        self.alerts_history.append(alert_info)

        if self.config.DEBUG_MODE:
            print(f"[DEBUG] 警报触发: {alert_info}")
        else:
            send_dingtalk_message(alert_info, stock)

    def get_stock_name(self, stock_code):
        try:
            # 优先尝试从 config 中获取股票名称
            if stock_code in self.config.MONITOR_STOCKS:
                name = self.config.MONITOR_STOCKS[stock_code].get('name')
                if name:  # 如果有值则返回
                    return name

            # 如果 config 中没有，尝试从 result_dict 中获取
            if stock_code in result_dict:
                name = result_dict[stock_code].get('name')
                if name:  # 如果有值则返回
                    return name

            return stock_code
        except Exception as e:
            return stock_code

    def get_alert_history(self):
        return self.alerts_history
