import os

from flask import Flask, request, jsonify
import json
import tushare as ts
import pandas as pd
import time
import threading
from datetime import datetime
from utils.tushare_utils import IndexAnalysis
from utils.date_utils import Date_utils
from utils.GetStockData import result_dict
from utils.send_dingding import send_dingtalk_message
from dto.StockDataDay import StockDataDay
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

stockAnalysis = Date_utils()
start_day = stockAnalysis.get_date_by_step(stockAnalysis.get_today(), -1).replace('-', '')


# 加载监控股票配置
def load_monitor_stocks_config():
    try:
        # 获取当前脚本所在的目录
        current_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(current_dir, 'monitor_stocks.json')

        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

            # 确保阈值配置中的键是字符串（JSON中的键可能是字符串）
            for stock_code, stock_config in config.items():
                if "thresholds" in stock_config:
                    # 确保所有时间窗口键都是字符串
                    thresholds = {}
                    for window_sec, threshold_config in stock_config["thresholds"].items():
                        thresholds[str(window_sec)] = threshold_config
                    stock_config["thresholds"] = thresholds

                    # 从thresholds的key中生成windows_sec
                    if "windows_sec" not in stock_config:
                        stock_config["windows_sec"] = [int(sec) for sec in stock_config["thresholds"].keys()]

                # 确保price_thresholds存在，如果没有则初始化为空列表
                if "price_thresholds" not in stock_config:
                    stock_config["price_thresholds"] = []

                # 确保change_thresholds存在，如果没有则初始化为空列表
                if "change_thresholds" not in stock_config:
                    stock_config["change_thresholds"] = []

            return config
    except FileNotFoundError:
        print("警告: monitor_stocks.json 文件未找到")
    except json.JSONDecodeError:
        print("错误: monitor_stocks.json 文件格式不正确")
        return {}


# 配置信息 - 支持个股独立时间窗口（以秒为单位）
CONFIG = {
    "TUSHARE_TOKEN": "410070664c78124d98ca5e81c3921530bd27534856b174c702d698a5",
    "MONITOR_STOCKS": load_monitor_stocks_config(),  # 从JSON文件加载
    "BASE_INTERVAL": 1,  # 基础数据收集间隔(秒)
    "DATA_RETENTION_HOURS": 10,  # 保留多少小时的数据
    "DEBUG_MODE": True,  # 调试模式开关
    "ALERT_COOLDOWN": 300  # 警报冷却时间（秒），5分钟内不重复发送相同警报
}


class StockMonitor:
    def __init__(self, config):
        self.stock_name_cache = {}
        self.config = config
        self.data_storage = {}  # 存储各股票的历史数据
        self.alerts_history = []  # 存储历史警报
        self.manual_data_queue = []  # 存储手动输入的数据
        self.last_update_time = {}  # 记录每个股票的最后更新时间
        self.last_alert_time = {}  # 记录每个股票每种警报类型的最后触发时间
        self.price_threshold_alerted = {}  # 记录价格阈值是否已触发过
        self.change_threshold_alerted = {}  # 记录涨跌幅阈值是否已触发过
        self.initialize_data_storage()

        # 设置Tushare Token
        ts.set_token(config["TUSHARE_TOKEN"])
        self.pro = ts.pro_api()

        # 创建锁用于线程安全
        self.lock = threading.Lock()

        # 启动监控线程
        if not config["DEBUG_MODE"]:
            self.monitor_thread = threading.Thread(target=self.start_monitoring, daemon=True)
            self.monitor_thread.start()
        else:
            print("调试模式已启用，监控线程未启动。使用手动触发功能进行测试。")

    def initialize_data_storage(self):
        # 计算基础数据保留数量
        base_retention = self.config["DATA_RETENTION_HOURS"] * 3600 // self.config["BASE_INTERVAL"]

        for stock, config in self.config["MONITOR_STOCKS"].items():
            self.data_storage[stock] = {}
            self.last_update_time[stock] = datetime.now()
            self.last_alert_time[stock] = {}  # 初始化警报时间记录
            self.price_threshold_alerted[stock] = {}  # 初始化价格阈值警报记录
            self.change_threshold_alerted[stock] = {}  # 初始化涨跌幅阈值警报记录

            # 基础数据（最高频率）
            self.data_storage[stock]["base"] = {
                "candles": [],  # 使用数组代替deque
                "maxlen": base_retention,
                "interval": self.config["BASE_INTERVAL"]
            }

    def _add_to_array(self, array, item, maxlen):
        """向数组添加元素，保持数组长度不超过maxlen"""
        array.append(item)
        if len(array) > maxlen:
            # 移除最旧的元素
            return array[1:]
        return array

    def fetch_realtime_data(self):
        """获取实时行情数据"""
        if self.config["DEBUG_MODE"]:
            return self.get_manual_data()
        else:
            stock_codes = list(self.config["MONITOR_STOCKS"].keys())
            return IndexAnalysis.realtime_quote(ts_code=",".join(stock_codes))

    def update_data_storage(self, data_list):
        """更新数据存储 - 使用数组代替deque"""
        if len(data_list) == 0:
            return

        current_time = datetime.now()

        with self.lock:
            for row in data_list:
                stock = row.ts_code

                # 更新基础数据
                candle_data = {
                    'timestamp': current_time,
                    'open': row.open,
                    'high': getattr(row, 'high', row.high),
                    'low': getattr(row, 'low', row.low),
                    'close': row.close,
                    'vol': row.vol,
                    'pre_close': getattr(row, 'pre_close', 0)  # 添加昨收价格
                }

                # 使用数组代替deque
                base_data = self.data_storage[stock]["base"]
                base_data["candles"] = self._add_to_array(
                    base_data["candles"], candle_data, base_data["maxlen"]
                )

                # 更新最后更新时间
                self.last_update_time[stock] = current_time


    def check_alert_conditions(self, stock, window_sec):
        """
        检查指定时间窗口的警报条件
        返回: 满足的警报条件列表
        """
        triggered_conditions = []
        candles = self.data_storage[stock]["base"]["candles"]

        # 获取阈值配置
        window_str = str(window_sec)
        thresholds = self.config["MONITOR_STOCKS"][stock]["thresholds"].get(window_str, {})

        # 检查各个警报条件
        if self._check_volume_spike( thresholds, stock):
            triggered_conditions.append("volume_spike")

        price_alerts = self._check_price_movement(candles, window_sec, self.config["MONITOR_STOCKS"][stock])
        triggered_conditions.extend(price_alerts)

        # 检查价格阈值条件
        price_threshold_alerts = self._check_price_thresholds(stock)
        triggered_conditions.extend(price_threshold_alerts)

        # 检查涨跌幅阈值条件
        change_threshold_alerts = self._check_change_thresholds(stock)
        triggered_conditions.extend(change_threshold_alerts)

        return triggered_conditions

    def _check_volume_spike(self, thresholds, stock):
        five_min_klines = IndexAnalysis.rt_min(stock, 3)  # 5分钟窗口
        return False

    def _check_price_movement(self, price_array, window_sec, thresholds_config):
        """
        检测价格异动 - 遍历所有配置的价格变化阈值

        参数:
            price_array: 价格数据数组，每个元素包含'price'价格
            window_sec: 时间窗口（秒）
            thresholds_config: 阈值配置字典，包含多个价格变化阈值

        返回:
            list: 触发的价格警报列表
        """
        triggered_alerts = []

        if len(price_array) == 0:
            return triggered_alerts

        # 获取当前时间窗口的阈值配置
        window_str = str(window_sec)
        thresholds = thresholds_config["thresholds"].get(window_str, {})

        # 将窗口秒数转换为数据点数量
        window_length = window_sec // self.config["BASE_INTERVAL"]

        # 获取最近window_length个数据点
        recent_prices = price_array[-window_length:] if len(price_array) >= window_length else price_array

        # 提取价格
        prices = [candle['close'] for candle in recent_prices]

        # 计算窗口内的最高价和最低价
        highest_price = max(prices) if prices else 0
        lowest_price = min(prices) if prices else 0
        current_price = prices[-1] if prices else 0

        # 计算从最高点的回撤幅度（百分比）
        drawdown_from_high = (current_price - highest_price) / highest_price * 100 if highest_price else 0

        # 计算从最低点的上涨幅度（百分比）
        gain_from_low = (current_price - lowest_price) / lowest_price * 100 if lowest_price else 0

        # 检查下跌阈值
        price_drop_thresholds = thresholds.get("price_drop", [])
        for threshold in price_drop_thresholds:
            if drawdown_from_high <= threshold:  # 注意：drawdown_from_high是负值，所以使用<=
                alert_type = f"price_drop_{abs(threshold)}%_{window_sec}s"
                triggered_alerts.append(alert_type)

        # 检查上涨阈值
        price_rise_thresholds = thresholds.get("price_rise", [])
        for threshold in price_rise_thresholds:
            if gain_from_low >= threshold:
                alert_type = f"price_rise_{threshold}%_{window_sec}s"
                triggered_alerts.append(alert_type)

        return triggered_alerts

    def _check_price_thresholds(self, stock):
        """
        检查价格是否达到配置的阈值
        参数:
            stock: 股票代码
        返回:
            list: 触发的价格阈值警报列表
        """
        triggered_alerts = []

        # 获取最新价格
        candles = self.data_storage[stock]["base"]["candles"]
        if not candles:
            return triggered_alerts

        current_price = candles[-1]['close']

        # 获取价格阈值配置
        price_thresholds = self.config["MONITOR_STOCKS"][stock].get("price_thresholds", [])

        for threshold_config in price_thresholds:
            threshold_price = threshold_config["price"]
            direction = threshold_config["direction"]
            # 创建唯一的警报标识符
            alert_id = f"price_{direction}_{threshold_price}"

            # 检查是否满足条件
            condition_met = False
            if direction == "above" and current_price >= threshold_price:
                condition_met = True
            elif direction == "below" and current_price <= threshold_price:
                condition_met = True

            if condition_met:
                # 检查是否已经触发过此警报
                if not self.price_threshold_alerted[stock].get(alert_id, False):
                    triggered_alerts.append(f"price_threshold_{alert_id}")
                    self.price_threshold_alerted[stock][alert_id] = True
            else:
                # 重置警报状态
                self.price_threshold_alerted[stock][alert_id] = False

        return triggered_alerts

    def _check_change_thresholds(self, stock):
        """
        检查涨跌幅是否达到配置的阈值
        参数:
            stock: 股票代码
        返回:
            list: 触发的涨跌幅阈值警报列表
        """
        triggered_alerts = []

        # 获取最新价格和昨收价格
        candles = self.data_storage[stock]["base"]["candles"]
        if not candles:
            return triggered_alerts

        current_price = candles[-1]['close']
        pre_close = candles[-1]['pre_close']

        # 计算涨跌幅百分比
        if pre_close > 0:
            change_percent = (current_price - pre_close) / pre_close * 100
        else:
            return triggered_alerts

        # 获取涨跌幅阈值配置
        change_thresholds = self.config["MONITOR_STOCKS"][stock].get("change_thresholds", [])

        for threshold_config in change_thresholds:
            threshold_change = threshold_config["change"]
            direction = threshold_config["direction"]

            # 创建唯一的警报标识符
            alert_id = f"change_{direction}_{threshold_change}"

            # 检查是否满足条件
            condition_met = False
            if direction == "above" and change_percent >= threshold_change:
                condition_met = True
            elif direction == "below" and change_percent <= threshold_change:
                condition_met = True

            if condition_met:
                # 检查是否已经触发过此警报
                if not self.change_threshold_alerted[stock].get(alert_id, False):
                    triggered_alerts.append(f"change_threshold_{alert_id}")
                    self.change_threshold_alerted[stock][alert_id] = True
            else:
                # 重置警报状态
                self.change_threshold_alerted[stock][alert_id] = False

        return triggered_alerts

    def send_alert(self, stock, window_sec, conditions):
        """发送警报通知"""
        current_time = datetime.now()

        # 检查冷却时间
        valid_conditions = []
        for condition in conditions:
            # 获取该股票该类型警报的最后触发时间
            last_trigger_time = self.last_alert_time[stock].get(condition)

            # 如果从未触发过，或者超过了冷却时间，则允许触发
            if last_trigger_time is None or \
                    (current_time - last_trigger_time).total_seconds() > self.config["ALERT_COOLDOWN"]:
                valid_conditions.append(condition)
                # 更新最后触发时间
                self.last_alert_time[stock][condition] = current_time

        if not valid_conditions:
            return

        condition_names = {
            "volume_spike": "成交量激增",
        }

        # 处理价格变化警报
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
                # 将条件代码转换为可读名称
                readable_condition = condition_names.get(condition, condition)
                price_alerts.append(readable_condition)

        conditions_str = "、".join(price_alerts)

        alert_info = f"{self.get_stock_name(stock)} {conditions_str}警报 {current_time.strftime('%H:%M:%S')}"
        self.alerts_history.append(alert_info)

        if self.config["DEBUG_MODE"]:
            print(f"[DEBUG] 警报触发: {alert_info}")
        else:
            send_dingtalk_message(alert_info, stock)

    def get_stock_name(self, stock_code):
        # 如果缓存中没有，调用Tushare API获取
        try:
            return result_dict[stock_code]['name']
        except Exception as e:
            print(f"获取股票名称失败: {e}")
            return stock_code  # 出错时返回股票代码

    def start_monitoring(self):
        """开始监控"""
        print(f"开始监控 {len(self.config['MONITOR_STOCKS'])} 只股票...")
        print("按 Ctrl+C 停止监控")

        while True:
            try:
                data_list = self.fetch_realtime_data()
                if len(data_list) != 0:
                    self.update_data_storage(data_list)
                    for stock in self.config["MONITOR_STOCKS"].keys():
                        # 检查价格阈值条件
                        price_threshold_alerts = self._check_price_thresholds(stock)
                        if price_threshold_alerts:
                            self.send_alert(stock, 0, price_threshold_alerts)

                        # 检查涨跌幅阈值条件
                        change_threshold_alerts = self._check_change_thresholds(stock)
                        if change_threshold_alerts:
                            self.send_alert(stock, 0, change_threshold_alerts)

                        # 检查时间窗口条件
                        for window_sec in self.config["MONITOR_STOCKS"][stock]["windows_sec"]:
                            conditions = self.check_alert_conditions(stock, window_sec)
                            if conditions:
                                self.send_alert(stock, window_sec, conditions)
                time.sleep(self.config["BASE_INTERVAL"])
            except KeyboardInterrupt:
                print("\n监控已停止")
                break

    # 其余方法保持不变...
    def input_manual_data(self):
        """从JSON文件读取股票数据"""
        print("\n===== 从JSON文件读取数据 =====")

        json_file = "manual.json"
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data_list = json.load(f)

            for i, data in enumerate(data_list):
                # 转换时间格式
                if 'time' in data:
                    timestamp = datetime.strptime(data['time'], "%Y-%m-%d %H:%M:%S")
                else:
                    timestamp = datetime.now()

                # 创建数据点
                data_point = pd.Series({
                    'stock_code': data['ts_code'],
                    'open': float(data['open']),
                    'high': float(data['high']),
                    'low': float(data['low']),
                    'close': float(data['close']),
                    'vol': int(data['vol']),
                    'pre_close': float(data.get('pre_close', data['close'])),  # 添加昨收价格，如果没有则使用当前价格
                    'time': timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                })
                data_list[i] = StockDataDay.from_json(data_point)
            self.update_data_storage(data_list)
            print(f"成功从 {json_file} 读取了 {len(data_list)} 条数据")

        except json.JSONDecodeError:
            print(f"错误：文件 {json_file} 格式不正确")

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
        self.input_manual_data()
        CONFIG["DEBUG_MODE"] = True  # 强制开启调试模式
        # 执行检测
        for stock in self.config["MONITOR_STOCKS"].keys():
            # 检查价格阈值条件
            price_threshold_alerts = self._check_price_thresholds(stock)
            if price_threshold_alerts:
                self.send_alert(stock, 0, price_threshold_alerts)

            # 检查涨跌幅阈值条件
            change_threshold_alerts = self._check_change_thresholds(stock)
            if change_threshold_alerts:
                self.send_alert(stock, 0, change_threshold_alerts)

            for window_sec in self.config["MONITOR_STOCKS"][stock]["windows_sec"]:
                conditions = self.check_alert_conditions(stock, window_sec)
                if conditions:
                    self.send_alert(stock, window_sec, conditions)
                else:
                    print(f"未检测到警报条件: {stock} {window_sec}秒")

        # 显示最新数据
        for stock in self.config["MONITOR_STOCKS"].keys():
            if self.data_storage[stock]["base"]["candles"]:
                last_candle = self.data_storage[stock]["base"]["candles"][-1]
                pre_close = last_candle.get('pre_close', 0)
                change_percent = ((last_candle['close'] - pre_close) / pre_close * 100) if pre_close > 0 else 0

                print(
                    f"{stock} 最新数据: 时间={last_candle['timestamp'].strftime('%H:%M:%S')}, "
                    f"收盘价={last_candle['close']:.2f}, 涨跌幅={change_percent:.2f}%, 成交量={last_candle['vol']}")

        print("=" * 30)

    def reload_monitor_stocks_config(self):
        """重新加载监控股票配置"""
        print("重新加载监控股票配置...")
        try:
            with open('monitor_stocks.json', 'r', encoding='utf-8') as f:
                new_config = json.load(f)

            # 确保阈值配置中的键是字符串
            for stock_code, stock_config in new_config.items():
                if "thresholds" in stock_config:
                    thresholds = {}
                    for window_sec, threshold_config in stock_config["thresholds"].items():
                        thresholds[str(window_sec)] = threshold_config
                    stock_config["thresholds"] = thresholds

                    # 从thresholds的key中生成windows_sec
                    if "windows_sec" not in stock_config:
                        stock_config["windows_sec"] = [int(sec) for sec in stock_config["thresholds"].keys()]

                # 确保price_thresholds存在，如果没有则初始化为空列表
                if "price_thresholds" not in stock_config:
                    stock_config["price_thresholds"] = []

                # 确保change_thresholds存在，如果没有则初始化为空列表
                if "change_thresholds" not in stock_config:
                    stock_config["change_thresholds"] = []

            self.config["MONITOR_STOCKS"] = new_config
            self.initialize_data_storage()  # 重新初始化数据存储
            print(f"成功重新加载 {len(new_config)} 只股票的配置")
        except Exception as e:
            print(f"重新加载配置失败: {e}")

    def display_data_storage(self):
        """显示数据存储状态"""
        print("\n数据存储状态:")
        for stock, windows in self.data_storage.items():
            print(f"{self.get_stock_name(stock)}:")
            for window_sec, data in windows.items():
                count = len(data["candles"])
                if count > 0:
                    last_candle = data["candles"][-1]
                    pre_close = last_candle.get('pre_close', 0)
                    change_percent = ((last_candle['close'] - pre_close) / pre_close * 100) if pre_close > 0 else 0

                    # 将秒转换为更易读的时间单位
                    if window_sec == "base":
                        window_str = f"基础({data['interval']}秒)"
                    elif window_sec < 60:
                        window_str = f"{window_sec}秒"
                    elif window_sec < 3600:
                        window_str = f"{window_sec // 60}分钟"
                    else:
                        window_str = f"{window_sec // 3600}小时"

                    print(f"  {window_str}: {count}条数据, 最新: {last_candle['timestamp'].strftime('%H:%M:%S')} "
                          f"收盘价={last_candle['close']:.2f}, 涨跌幅={change_percent:.2f}%, 成交量={last_candle['vol']}")
                else:
                    if window_sec == "base":
                        window_str = f"基础({data['interval']}秒)"
                    else:
                        window_str = f"{window_sec}秒"
                    print(f"  {window_str}: 无数据")

    def display_alert_history(self):
        """显示警报历史"""
        print("\n警报历史:")
        if not self.alerts_history:
            print("  无警报记录")
            return

        for i, alert in enumerate(self.alerts_history, 1):
            print(f"{i}. {alert}")


@app.route('/api/alerts')
def get_alerts():
    return jsonify({
        'status': 'success',
        'alerts': monitor.display_alert_history()
    })


@app.route('/api/manual_trigger_detection')
def manual_input():
    monitor.manual_trigger_detection()
    return "success"


@app.route('/api/reload_config')
def reload_config():
    monitor.reload_monitor_stocks_config()
    return "success"


# 启动监控系统
if __name__ == "__main__":
    print(111)
    monitor = StockMonitor(CONFIG)
    app.run(host='0.0.0.0', port=5000)