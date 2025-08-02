import tushare as ts
import pandas as pd
import time
import threading
from datetime import datetime
import matplotlib.pyplot as plt
from collections import deque
from utils.tushare_utils import IndexAnalysis
from utils.date_utils import Date_utils

stockAnalysis = Date_utils()
from utils.send_dingding import send_dingtalk_message

start_day = stockAnalysis.get_date_by_step(stockAnalysis.get_today(), -1).replace('-', '')

# 配置信息
CONFIG = {
    "TUSHARE_TOKEN": "410070664c78124d98ca5e81c3921530bd27534856b174c702d698a5",  # 替换为你的实际Token
    # "MONITOR_STOCKS": ["600000.SH", "000001.SZ", "399001.SZ"],  # 监控的股票列表
    "MONITOR_STOCKS": ["600000.SH"],  # 监控的股票列表
    "ALERT_THRESHOLDS": {
        "1min": {"volume_ratio": 1.8, "price_change": -0.8},  # 1分钟放量下跌阈值
        "5min": {"volume_ratio": 2.5, "price_change": -1.5}  # 5分钟放量下跌阈值
    },
    "MONITOR_INTERVAL": 60,  # 监控间隔(秒)
    "DATA_RETENTION": 8000,  # 保留多少分钟的数据
    "DEBUG_MODE": True  # 调试模式开关
}


class StockMonitor:
    def __init__(self, config):
        self.config = config
        self.data_storage = {}  # 存储各股票的历史数据
        self.alerts_history = []  # 存储历史警报
        self.initialize_data_storage()
        self.manual_data_queue = []  # 存储手动输入的数据

        # 设置Tushare Token
        ts.set_token(config["TUSHARE_TOKEN"])
        self.pro = ts.pro_api()

        # 加载历史数据初始化存储
        self.load_his_data()  # 添加这行代码

        # 创建锁用于线程安全
        self.lock = threading.Lock()

        # 启动监控线程
        if not config["DEBUG_MODE"]:
            self.monitor_thread = threading.Thread(target=self.start_monitoring, daemon=True)
            self.monitor_thread.start()
        else:
            print("调试模式已启用，监控线程未启动。使用手动触发功能进行测试。")

    def load_his_data(self):
        """使用历史分钟数据初始化数据存储"""
        print("开始加载历史数据初始化存储...")

        # 在调试模式下使用最近日期，避免非交易日错误
        if self.config["DEBUG_MODE"]:
            today = datetime.now().strftime("%Y%m%d")
            current_time = datetime.now().strftime("%H:%M:%S")
        else:
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

                print(
                    f"  {stock} 加载完成: 1min={len(self.data_storage[stock]['1min']['candles'])}, 5min={len(self.data_storage[stock]['5min']['candles'])}")

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
        if self.config["DEBUG_MODE"]:
            return self.get_manual_data()
        else:
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

    ############################################################
    # 重构后的检测方法，支持多种警报条件的灵活扩展
    ############################################################

    def check_alert_conditions(self, stock, timeframe):
        """
        检查所有警报条件
        返回: 满足的警报条件列表 (空列表表示没有警报)
        """
        triggered_conditions = []

        # 获取最近的K线数据
        candles = self.data_storage[stock][timeframe]["candles"]
        if len(candles) < 2:
            return triggered_conditions

        current_candle = candles[-1]
        previous_candle = candles[-2]

        # 获取阈值配置
        thresholds = self.config["ALERT_THRESHOLDS"][timeframe]

        # 检查各个警报条件
        if self._check_volume_spike(current_candle, previous_candle, thresholds):
            triggered_conditions.append("volume_spike")

        if self._check_price_drop(current_candle, previous_candle, thresholds):
            triggered_conditions.append("price_drop")

        # 在这里可以添加更多的检测条件
        # 例如:
        # if self._check_rsi_oversold(stock, timeframe):
        #     triggered_conditions.append("rsi_oversold")
        #
        # if self._check_macd_crossover(stock, timeframe):
        #     triggered_conditions.append("macd_crossover")

        return triggered_conditions

    def _check_volume_spike(self, current_candle, previous_candle, thresholds):
        """检测成交量激增"""
        if previous_candle['vol'] > 0:
            volume_ratio = current_candle['vol'] / previous_candle['vol']
            return volume_ratio > thresholds.get("volume_ratio", 1.0)
        return False

    def _check_price_drop(self, current_candle, previous_candle, thresholds):
        """检测价格下跌"""
        price_change_pct = (current_candle['close'] - previous_candle['close']) / previous_candle['close'] * 100
        return price_change_pct < thresholds.get("price_change", 0.0)


    def send_alert(self, stock, timeframe, conditions):
        """发送警报通知"""
        condition_names = {
            "volume_spike": "成交量激增",
            "price_drop": "价格下跌",
            # 添加其他条件的描述
        }

        # 将条件代码转换为可读名称
        readable_conditions = [condition_names.get(c, c) for c in conditions]
        conditions_str = "、".join(readable_conditions)

        alert_info = f"{self.get_stock_name(stock)} {timeframe} {conditions_str}警报 {datetime.now().strftime('%H:%M:%S')}"
        self.alerts_history.append(alert_info)

        if self.config["DEBUG_MODE"]:
            print(f"[DEBUG] 警报触发: {alert_info}")
        else:
            send_dingtalk_message("分时监控", alert_info)

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
                            # 使用重构后的检测方法
                            conditions = self.check_alert_conditions(stock, timeframe)
                            if conditions:
                                self.send_alert(stock, timeframe, conditions)
                time.sleep(self.config["MONITOR_INTERVAL"])
            except KeyboardInterrupt:
                print("\n监控已停止")
                break

    def input_manual_data(self):
        """手动输入股票数据"""
        print("\n===== 手动输入数据 =====")
        for stock in self.config["MONITOR_STOCKS"]:
            stock_name = self.get_stock_name(stock)
            print(f"输入 {stock_name}({stock}) 的数据:")

            time_str = input("  时间(格式YYYY-MM-DD HH:MM:SS，直接回车使用当前时间): ")
            if not time_str:
                time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            open_price = float(input("  开盘价: "))
            close_price = float(input("  收盘价: "))
            volume = int(input("  成交量(股): "))

            # 创建模拟数据点
            data_point = pd.Series({
                'ts_code': stock,
                'open': open_price,
                'close': close_price,
                'vol': volume,
                'time': time_str
            })

            self.manual_data_queue.append(data_point)
            print(f"已添加 {stock} 的数据到队列")
        print("=" * 30)

    def get_manual_data(self):
        """从手动输入队列获取数据"""
        if not self.manual_data_queue:
            print("手动数据队列为空，请先输入数据")
            return []

        # 每次返回一条数据
        return [self.manual_data_queue.pop(0)]

    def manual_trigger_detection(self):
        """手动触发检测"""
        print("\n===== 手动触发检测 =====")

        # 如果队列为空，提示输入数据
        if not self.manual_data_queue:
            print("手动数据队列为空，请先输入数据")
            self.input_manual_data()

        # 获取并处理手动数据
        min_list = self.get_manual_data()
        if min_list:
            self.update_data_storage(min_list)

            # 执行检测
            for stock in self.config["MONITOR_STOCKS"]:
                for timeframe in ["1min", "5min"]:
                    # 使用重构后的检测方法
                    conditions = self.check_alert_conditions(stock, timeframe)
                    if conditions:
                        print(f"检测到警报条件: {stock} {timeframe} - {', '.join(conditions)}")
                        self.send_alert(stock, timeframe, conditions)
                    else:
                        print(f"未检测到警报条件: {stock} {timeframe}")

        # 显示最新数据
        for stock in self.config["MONITOR_STOCKS"]:
            if self.data_storage[stock]["1min"]["candles"]:
                last_candle = self.data_storage[stock]["1min"]["candles"][-1]
                print(
                    f"{stock} 最新数据: 时间={last_candle['time']}, 收盘价={last_candle['close']:.2f}, 成交量={last_candle['vol']}")

        print("=" * 30)

    def debug_menu(self):
        """调试菜单"""
        while True:
            print("\n===== 调试菜单 =====")
            print("1. 手动输入数据")
            print("2. 手动触发检测")
            print("3. 显示数据存储状态")
            print("4. 可视化股票数据")
            print("5. 显示警报历史")
            print("6. 退出")

            choice = input("请选择操作: ")

            if choice == "1":
                self.input_manual_data()
            elif choice == "2":
                self.manual_trigger_detection()
            elif choice == "3":
                self.display_data_storage()
            elif choice == "4":
                stock = input("输入股票代码(如600000.SH): ")
                self.visualize_data(stock)
            elif choice == "5":
                self.display_alert_history()
            elif choice == "6":
                print("退出调试菜单")
                break
            else:
                print("无效选择，请重新输入")

    def display_data_storage(self):
        """显示数据存储状态"""
        print("\n数据存储状态:")
        for stock, timeframes in self.data_storage.items():
            print(f"{self.get_stock_name(stock)}:")
            for timeframe, data in timeframes.items():
                count = len(data["candles"])
                if count > 0:
                    last_candle = data["candles"][-1]
                    print(f"  {timeframe}: {count}条数据, 最新: {last_candle['time']} "
                          f"收盘价={last_candle['close']:.2f} 成交量={last_candle['vol']}")
                else:
                    print(f"  {timeframe}: 无数据")

    def display_alert_history(self):
        """显示警报历史"""
        print("\n警报历史:")
        if not self.alerts_history:
            print("  无警报记录")
            return

        for i, alert in enumerate(self.alerts_history, 1):
            print(f"{i}. {alert}")


# 启动监控系统
if __name__ == "__main__":
    monitor = StockMonitor(CONFIG)

    if CONFIG["DEBUG_MODE"]:
        print("运行在调试模式，使用调试菜单进行测试")
        monitor.debug_menu()
    else:
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("程序已退出")
            for stock in CONFIG["MONITOR_STOCKS"]:
                monitor.visualize_data(stock)