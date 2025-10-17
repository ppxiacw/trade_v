from datetime import datetime
from utils.send_dingding import send_dingtalk_message
from utils.GetStockData import get_stock_name
from monitor.config.db_monitor import stock_alert_dao


class AlertSender:
    def __init__(self, config):
        self.config = config
        self.last_alert_time = {}

        for stock in self.config.MONITOR_STOCKS.keys():
            self.last_alert_time[stock] = {}

    def send_alert(self, stock, alerts_with_cooldown):
        current_time = datetime.now()
        valid_alerts = []

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
            last_trigger = self.last_alert_time[stock].get(alert_message)

            # 判断是否已经过了冷却时间
            if not last_trigger or (current_time - last_trigger).seconds >= cooldown:
                valid_alerts.append(alert_data)
                self.last_alert_time[stock][alert_message] = current_time

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

            # 构建显示消息
            alert_info = f"{alert_data['stock_name']} {alert_data['alert_message']} 警报 {alert_data['trigger_time']}"

            # 发送钉钉消息
            send_dingtalk_message(alert_info, stock)

            # 插入数据库 - 直接使用alert_data
            stock_alert_dao.insert_alert(alert_data)

