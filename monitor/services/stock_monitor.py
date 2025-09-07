import threading
import time
from datetime import datetime


class StockMonitor:
    def __init__(self, config, data_fetcher, alert_checker, alert_sender, stock_data):
        self.config = config
        self.data_fetcher = data_fetcher
        self.alert_checker = alert_checker
        self.alert_sender = alert_sender
        self.stock_data = stock_data
        self.lock = threading.Lock()

    def start_monitoring(self):
        print(f"开始监控 {len(self.config.MONITOR_STOCKS)} 只股票...")

        while True:
            try:
                stock_codes = list(self.config.MONITOR_STOCKS.keys())
                data_list = self.data_fetcher.fetch_realtime_data(stock_codes)

                if len(data_list) != 0:
                    with self.lock:
                        self.stock_data.update_data(data_list)

                    for stock in stock_codes:
                        alerts = self.alert_checker.check_all_conditions(stock)
                        if alerts:
                            self.alert_sender.send_alert(stock, 0, alerts)

                time.sleep(self.config.BASE_INTERVAL)
            except KeyboardInterrupt:
                print("\n监控已停止")
                break