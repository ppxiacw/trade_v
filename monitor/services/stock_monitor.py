import concurrent.futures
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
        print(f"开始监控 {len(self.config.MONITOR_STOCKS)} 个")
        stock_codes = list(self.config.MONITOR_STOCKS.keys())

        # 使用线程池处理数据和检查
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)

        while True:
            try:

                # 获取数据
                data_list = self.data_fetcher.fetch_realtime_data(stock_codes)

                if not data_list:
                    time.sleep(1)  # 数据为空时短暂休眠
                    continue

                # 并行处理数据更新和警报检查
                with self.lock:
                    self.stock_data.update_data(data_list)

                futures = []
                for stock in stock_codes:
                    futures.append(executor.submit(self.check_and_send_alerts, stock))

                # 等待所有检查完成
                concurrent.futures.wait(futures)



            except Exception as e:
                print(f"监控循环出错: {str(e)}")
                time.sleep(5)  # 出错时暂停5秒再重试

    def check_and_send_alerts(self, stock):
        """并行检查并发送警报的辅助方法"""
        try:
            alerts = self.alert_checker.check_all_conditions(stock)
            if alerts:
                self.alert_sender.send_alert(stock, alerts)
        except Exception as e:
            print(f"检查股票{alerts}警报时出错: {str(e)}")
