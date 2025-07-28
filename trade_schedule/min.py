import tushare as ts
import pandas as pd
import time
import threading
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
import matplotlib.pyplot as plt
import numpy as np
from collections import deque
from config.tushare_utils import IndexAnalysis
from dto.StockDataDay import StockDataDay

# 配置信息
CONFIG = {
    "TUSHARE_TOKEN": "410070664c78124d98ca5e81c3921530bd27534856b174c702d698a5",  # 替换为你的实际Token
    "MONITOR_STOCKS": ["600000.SH", "000001.SZ", "399001.SZ"],  # 监控的股票列表
    "ALERT_THRESHOLDS": {
        "1min": {"volume_ratio": 1.8, "price_change": -0.8},  # 1分钟放量下跌阈值
        "5min": {"volume_ratio": 2.5, "price_change": -1.5}  # 5分钟放量下跌阈值
    },
    "MONITOR_INTERVAL": 60,  # 监控间隔(秒)
    "DATA_RETENTION": 20,  # 保留多少分钟的数据
    "EMAIL_SETTINGS": {
        "enabled": True,  # 是否启用邮件通知
        "sender": "your_email@example.com",
        "password": "your_email_password",
        "receiver": "alert_receiver@example.com",
        "smtp_server": "smtp.example.com",
        "smtp_port": 587
    }
}


class StockMonitor:
    def __init__(self, config):
        self.config = config
        self.data_storage = {}  # 存储各股票的历史数据
        self.alerts_history = []  # 存储历史警报
        self.initialize_data_storage()

        # 设置Tushare Token
        ts.set_token(config["TUSHARE_TOKEN"])
        self.pro = ts.pro_api()

        # 创建锁用于线程安全
        self.lock = threading.Lock()

        # 启动监控线程
        self.monitor_thread = threading.Thread(target=self.start_monitoring, daemon=True)
        self.monitor_thread.start()

    def initialize_data_storage(self):
        """初始化数据存储结构"""
        for stock in self.config["MONITOR_STOCKS"]:
            self.data_storage[stock] = {
                "1min": {
                    "timestamps": deque(maxlen=self.config["DATA_RETENTION"]),
                    "prices": deque(maxlen=self.config["DATA_RETENTION"]),
                    "volumes": deque(maxlen=self.config["DATA_RETENTION"])
                },
                "5min": {
                    "timestamps": deque(maxlen=self.config["DATA_RETENTION"] // 5),
                    "prices": deque(maxlen=self.config["DATA_RETENTION"] // 5),
                    "volumes": deque(maxlen=self.config["DATA_RETENTION"] // 5)
                }
            }

    def fetch_realtime_data(self):
        """获取实时行情数据"""

        return IndexAnalysis.realtime_quote(ts_code=",".join(self.config["MONITOR_STOCKS"]))



    def update_data_storage(self, min_list):
        """更新数据存储"""
        if min_list.__len__()==0:
            return

        current_time = datetime.now()

        with self.lock:
            for row in min_list:
                stock = row.ts_code
                if stock not in self.data_storage:
                    continue

                # 更新1分钟数据
                self.data_storage[stock]["1min"]["times"].append(current_time)
                self.data_storage[stock]["1min"]["closes"].append(row['close'])
                self.data_storage[stock]["1min"]["vols"].append(row['vol'])

                # 每5分钟更新一次5分钟数据
                if current_time.minute % 5 == 0 and current_time.second < 10:
                    if self.data_storage[stock]["1min"]["prices"]:
                        # 计算5分钟收盘价（取最新价）
                        five_min_price = row['price']

                        # 计算5分钟成交量（累计）
                        if len(self.data_storage[stock]["1min"]["volumes"]) >= 5:
                            five_min_volume = sum(list(self.data_storage[stock]["1min"]["volumes"])[-5:])
                        else:
                            five_min_volume = row['volume']

                        self.data_storage[stock]["5min"]["timestamps"].append(current_time)
                        self.data_storage[stock]["5min"]["prices"].append(five_min_price)
                        self.data_storage[stock]["5min"]["volumes"].append(five_min_volume)

    def detect_volume_spike(self, stock, timeframe):
        """检测放量下跌情况"""
        if not self.data_storage[stock][timeframe]["prices"] or len(self.data_storage[stock][timeframe]["prices"]) < 2:
            return False

        # 获取最近两个时间段的数据
        current_price = self.data_storage[stock][timeframe]["prices"][-1]
        previous_price = self.data_storage[stock][timeframe]["prices"][-2]

        current_volume = self.data_storage[stock][timeframe]["volumes"][-1]
        previous_volume = self.data_storage[stock][timeframe]["volumes"][-2]

        # 计算价格变化百分比
        price_change_pct = (current_price - previous_price) / previous_price * 100

        # 计算成交量比率
        if previous_volume > 0:
            volume_ratio = current_volume / previous_volume
        else:
            volume_ratio = 0  # 避免除零错误

        # 获取阈值
        thresholds = self.config["ALERT_THRESHOLDS"][timeframe]

        # 检测放量下跌
        if price_change_pct < thresholds["price_change"] and volume_ratio > thresholds["volume_ratio"]:
            return {
                "stock": stock,
                "timeframe": timeframe,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "price_change": round(price_change_pct, 2),
                "volume_ratio": round(volume_ratio, 2),
                "current_price": current_price,
                "previous_price": previous_price,
                "current_volume": current_volume,
                "previous_volume": previous_volume
            }

        return False

    def send_alert(self, alert_data):
        """发送警报通知"""
        # 添加到历史记录
        self.alerts_history.append(alert_data)

        # 创建警报消息
        stock_name = self.get_stock_name(alert_data["stock"])
        timeframe = alert_data["timeframe"]

        message = (
            f"🚨 放量下跌警报 🚨\n\n"
            f"股票: {stock_name} ({alert_data['stock']})\n"
            f"时间范围: {timeframe}\n"
            f"发生时间: {alert_data['timestamp']}\n\n"
            f"📉 价格变化: {alert_data['price_change']}%\n"
            f"📈 成交量比率: {alert_data['volume_ratio']}倍\n\n"
            f"当前价: {alert_data['current_price']}\n"
            f"前一时段价: {alert_data['previous_price']}\n"
            f"当前成交量: {format(alert_data['current_volume'], ',')}股\n"
            f"前一时段成交量: {format(alert_data['previous_volume'], ',')}股"
        )

        print(f"\n{'=' * 50}")
        print(message)
        print(f"{'=' * 50}\n")

        # 发送邮件通知
        if self.config["EMAIL_SETTINGS"]["enabled"]:
            self.send_email("股票放量下跌警报", message)

    def send_email(self, subject, body):
        """发送电子邮件通知"""
        try:
            msg = MIMEText(body, 'plain', 'utf-8')
            msg['Subject'] = subject
            msg['From'] = self.config["EMAIL_SETTINGS"]["sender"]
            msg['To'] = self.config["EMAIL_SETTINGS"]["receiver"]

            server = smtplib.SMTP(
                self.config["EMAIL_SETTINGS"]["smtp_server"],
                self.config["EMAIL_SETTINGS"]["smtp_port"]
            )
            server.starttls()
            server.login(
                self.config["EMAIL_SETTINGS"]["sender"],
                self.config["EMAIL_SETTINGS"]["password"]
            )
            server.sendmail(
                self.config["EMAIL_SETTINGS"]["sender"],
                [self.config["EMAIL_SETTINGS"]["receiver"]],
                msg.as_string()
            )
            server.quit()
            print("邮件通知已发送")
        except Exception as e:
            print(f"发送邮件失败: {e}")

    def get_stock_name(self, stock_code):
        """获取股票名称"""
        try:
            # 这里可以使用Tushare的stock_basic接口获取股票名称
            # 简化处理：使用一个映射表
            name_map = {
                "600000.SH": "浦发银行",
                "000001.SZ": "平安银行",
                "399001.SZ": "深证成指"
            }
            return name_map.get(stock_code, stock_code)
        except:
            return stock_code

    def visualize_data(self, stock):
        """可视化股票数据"""
        if stock not in self.data_storage:
            print(f"没有 {stock} 的数据")
            return

        plt.figure(figsize=(14, 10))

        # 1分钟K线图
        plt.subplot(2, 1, 1)
        timestamps = list(self.data_storage[stock]["1min"]["timestamps"])
        prices = list(self.data_storage[stock]["1min"]["prices"])

        if len(timestamps) > 1 and len(prices) > 1:
            plt.plot(timestamps, prices, 'b-', label='价格')
            plt.title(f"{self.get_stock_name(stock)} 1分钟价格走势")
            plt.xlabel("时间")
            plt.ylabel("价格")
            plt.grid(True)
            plt.legend()

        # 成交量图
        plt.subplot(2, 1, 2)
        volumes = list(self.data_storage[stock]["1min"]["volumes"])

        if len(timestamps) > 1 and len(volumes) > 1:
            plt.bar(timestamps, volumes, color='g', alpha=0.7, label='成交量')
            plt.title(f"{self.get_stock_name(stock)} 1分钟成交量")
            plt.xlabel("时间")
            plt.ylabel("成交量(股)")
            plt.grid(True)
            plt.legend()

        plt.tight_layout()
        plt.savefig(f"{stock}_monitor.png")
        print(f"已保存 {stock} 监控图表")

    def start_monitoring(self):
        """开始监控"""
        print(f"开始监控 {len(self.config['MONITOR_STOCKS'])} 只股票...")
        print("按 Ctrl+C 停止监控")

        while True:
            try:
                # 获取实时数据
                min_list = self.fetch_realtime_data()

                if min_list.__len__()!=0:
                    # 更新数据存储
                    self.update_data_storage(min_list)

                    # 检测放量下跌
                    for stock in self.config["MONITOR_STOCKS"]:
                        for timeframe in ["1min", "5min"]:
                            alert = self.detect_volume_spike(stock, timeframe)
                            if alert:
                                self.send_alert(alert)

                # 等待下一个监控周期
                time.sleep(self.config["MONITOR_INTERVAL"])

            except KeyboardInterrupt:
                print("\n监控已停止")
                break



# 启动监控系统
if __name__ == "__main__":
    monitor = StockMonitor(CONFIG)

    # 保持主线程运行
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("程序已退出")

        # 退出前保存可视化图表
        for stock in CONFIG["MONITOR_STOCKS"]:
            monitor.visualize_data(stock)