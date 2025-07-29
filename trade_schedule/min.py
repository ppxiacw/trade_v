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
from utils import StockAnalysis

stockAnalysis = StockAnalysis()
from config.send_dingding import send_dingtalk_message
# 配置信息
CONFIG = {
    "TUSHARE_TOKEN": "410070664c78124d98ca5e81c3921530bd27534856b174c702d698a5",  # 替换为你的实际Token
    "MONITOR_STOCKS": ["600000.SH", "000001.SZ", "399001.SZ"],  # 监控的股票列表
    "ALERT_THRESHOLDS": {
        "1min": {"volume_ratio": 1.8, "price_change": -0.8},  # 1分钟放量下跌阈值
        "5min": {"volume_ratio": 2.5, "price_change": -1.5}  # 5分钟放量下跌阈值
    },
    "MONITOR_INTERVAL": 60,  # 监控间隔(秒)
    "DATA_RETENTION": 8000,  # 保留多少分钟的数据
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

        # 加载历史数据初始化存储
        self.load_his_data()  # 添加这行代码

        # 创建锁用于线程安全
        self.lock = threading.Lock()

        # 启动监控线程
        self.monitor_thread = threading.Thread(target=self.start_monitoring, daemon=True)
        self.monitor_thread.start()

    def load_his_data(self):
        """使用历史分钟数据初始化数据存储"""
        print("开始加载历史数据初始化存储...")

        start_day = start_day =  stockAnalysis.get_date_by_step(stockAnalysis.get_today(),-20).replace('-','')

        # 获取当前日期和时间
        today = datetime.now().strftime("%Y%m%d")
        current_time = datetime.now().strftime("%H:%M:%S")

        for stock in self.config["MONITOR_STOCKS"]:
            print(f"加载 {stock} 的历史数据...")

            try:
                # 获取1分钟历史数据
                df_1min = ts.pro_bar(ts_code=stock,
                                     freq='1min',
                                     start_date=start_day + " 09:30:00",
                                     end_date=today + " " + current_time,
                                     limit=self.config["DATA_RETENTION"])

                if df_1min is not None and not df_1min.empty:
                    # 按时间顺序排序（从旧到新）
                    df_1min = df_1min.sort_values('trade_time', ascending=True)

                    for _, row in df_1min.iterrows():
                        candle_data = {
                            'time': row['trade_time'],
                            'open': row['open'],
                            'close': row['close'],
                            'vol': row['vol']
                        }
                        self.data_storage[stock]["1min"]["candles"].append(candle_data)

                # 获取5分钟历史数据
                # 计算需要的5分钟数据条数
                five_min_count = max(1, self.config["DATA_RETENTION"] // 5)
                df_5min = ts.pro_bar(ts_code=stock,
                                     freq='5min',
                                     start_date=start_day + " 09:30:00",
                                     end_date=today + " " + current_time,
                                     limit=five_min_count)

                if df_5min is not None and not df_5min.empty:
                    # 按时间顺序排序（从旧到新）
                    df_5min = df_5min.sort_values('trade_time', ascending=True)

                    for _, row in df_5min.iterrows():
                        candle_data = {
                            'time': row['trade_time'],
                            'open': row['open'],
                            'close': row['close'],
                            'vol': row['vol']
                        }
                        self.data_storage[stock]["5min"]["candles"].append(candle_data)

                print(f"  {stock} 加载完成: 1min={len(self.data_storage[stock]['1min']['candles'])}, 5min={len(self.data_storage[stock]['5min']['candles'])}")

            except Exception as e:
                print(f"  加载 {stock} 历史数据失败: {str(e)}")

        print("历史数据初始化完成")


    def initialize_data_storage(self):
        """初始化数据存储结构"""
        for stock in self.config["MONITOR_STOCKS"]:
            self.data_storage[stock] = {
                "1min": {
                    # 使用candles列表存储K线数据，每个元素是一个字典
                    "candles": deque(maxlen=self.config["DATA_RETENTION"])
                },
                "5min": {
                    "candles": deque(maxlen=self.config["DATA_RETENTION"] // 5)
                }
            }

    def fetch_realtime_data(self):
        """获取实时行情数据"""
        return IndexAnalysis.realtime_quote(ts_code=",".join(self.config["MONITOR_STOCKS"]))



    def update_data_storage(self, min_list):
        """更新数据存储"""
        if len(min_list) == 0:
            return

        current_time = datetime.now()

        with self.lock:
            for row in min_list:
                stock = row.ts_code
                if stock not in self.data_storage:
                    continue

                # 更新1分钟数据 - 使用新的字典结构
                candle_data = {
                    'time': row.time,
                    'open': row.open,  # 添加开盘价
                    'close': row.close,
                    'vol': row.vol
                }
                self.data_storage[stock]["1min"]["candles"].append(candle_data)

                # 每5分钟更新一次5分钟数据
                if current_time.minute % 5 == 0 and current_time.second < 10:
                    if self.data_storage[stock]["1min"]["candles"]:
                        # 获取最近5根1分钟K线
                        last_five = list(self.data_storage[stock]["1min"]["candles"])[-5:]

                        # 计算5分钟K线的开盘价（第一个1分钟K线的开盘价）
                        five_min_open = last_five[0]['open'] if last_five else row.close
                        # 计算5分钟K线的收盘价（最后一个1分钟K线的收盘价）
                        five_min_close = last_five[-1]['close'] if last_five else row.close
                        # 计算5分钟成交量（5根1分钟K线成交量之和）
                        five_min_vol = sum(candle['vol'] for candle in last_five) if last_five else row.vol

                        # 创建5分钟K线数据
                        five_min_candle = {
                            'time': current_time.strftime("%Y-%m-%d %H:%M:%S"),
                            'open': five_min_open,
                            'close': five_min_close,
                            'vol': five_min_vol
                        }
                        self.data_storage[stock]["5min"]["candles"].append(five_min_candle)

    def detect_volume_spike(self, stock, timeframe):
        """检测放量下跌情况"""
        candles = self.data_storage[stock][timeframe]["candles"]
        if not candles or len(candles) < 2:
            return False

        # 获取最近两个K线的数据
        current_candle = candles[-1]
        previous_candle = candles[-2]

        # 计算价格变化百分比
        price_change_pct = (current_candle['close'] - previous_candle['close']) / previous_candle['close'] * 100

        # 计算成交量比率
        if previous_candle['vol'] > 0:
            volume_ratio = current_candle['vol'] / previous_candle['vol']
        else:
            volume_ratio = 0

        # 获取阈值
        thresholds = self.config["ALERT_THRESHOLDS"][timeframe]

        # 检测放量下跌
        if price_change_pct < thresholds["price_change"] and volume_ratio > thresholds["volume_ratio"]:
            return stock

        return stock

    def send_alert(self, stock):
        """发送警报通知"""
        self.alerts_history.append(stock)
        send_dingtalk_message("分时监控", stock)

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
        candles = list(self.data_storage[stock]["1min"]["candles"])
        times = [candle['time'] for candle in candles]
        closes = [candle['close'] for candle in candles]
        opens = [candle['open'] for candle in candles]  # 使用开盘价

        if len(times) > 1 and len(closes) > 1:
            # 绘制收盘价线
            plt.plot(times, closes, 'b-', label='收盘价')
            # 绘制开盘价线
            plt.plot(times, opens, 'g--', label='开盘价')
            plt.title(f"{self.get_stock_name(stock)} 1分钟价格走势")
            plt.xlabel("时间")
            plt.ylabel("价格")
            plt.grid(True)
            plt.legend()

        # 成交量图
        plt.subplot(2, 1, 2)
        vols = [candle['vol'] for candle in candles]

        if len(times) > 1 and len(vols) > 1:
            plt.bar(times, vols, color='g', alpha=0.7, label='成交量')
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
                min_list = self.fetch_realtime_data()
                if len(min_list) != 0:
                    self.update_data_storage(min_list)
                    for stock in self.config["MONITOR_STOCKS"]:
                        for timeframe in ["1min", "5min"]:
                            alert = self.detect_volume_spike(stock, timeframe)
                            if True:
                                self.send_alert(alert)
                time.sleep(self.config["MONITOR_INTERVAL"])
            except KeyboardInterrupt:
                print("\n监控已停止")
                break

# 启动监控系统
if __name__ == "__main__":
    monitor = StockMonitor(CONFIG)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("程序已退出")
        for stock in CONFIG["MONITOR_STOCKS"]:
            monitor.visualize_data(stock)