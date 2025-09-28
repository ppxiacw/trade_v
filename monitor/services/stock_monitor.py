import concurrent.futures
import threading
import time
from datetime import datetime, time as dt_time


class StockMonitor:
    def __init__(self, config, data_fetcher, alert_checker, alert_sender, stock_data):
        self.config = config
        self.data_fetcher = data_fetcher
        self.alert_checker = alert_checker
        self.alert_sender = alert_sender
        self.stock_data = stock_data
        self.lock = threading.Lock()

    def is_market_open(self):
        """检查当前时间是否在股票市场的开盘时间内"""
        now = datetime.now().time()
        # 假设开盘时间为 9:30 AM 到 4:00 PM
        market_open = dt_time(11, 31)
        market_close = dt_time(12, 59)
        return market_open <= now <= market_close

    def start_monitoring(self):
        print(f"开始监控 {len(self.config.MONITOR_STOCKS)} 个")
        stock_codes = list(self.config.MONITOR_STOCKS.keys())

        # 使用线程池处理数据和检查
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)

        while True:
            # 检查市场是否开盘
            if self.is_market_open():
                print("市场已收盘，停止监控")
                time.sleep(60)  # 每分钟检查一次市场是否重新开盘
                continue

            # 获取数据
            data_list = self.data_fetcher.fetch_realtime_data(stock_codes)


            # 并行处理数据更新和警报检查
            with self.lock:
                self.stock_data.update_data(data_list)

            futures = []
            for stock in stock_codes:
                futures.append(executor.submit(self.check_and_send_alerts, stock))

            # 等待所有检查完成
            concurrent.futures.wait(futures)




    def check_and_send_alerts(self, stock):
        """并行检查并发送警报的辅助方法"""
        try:
            alerts = self.alert_checker.check_all_conditions(stock)
            if alerts:
                self.alert_sender.send_alert(stock, alerts)
        except Exception as e:
            print(f"检查股票{alerts}警报时出错: {str(e)}")
