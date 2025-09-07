import os
import json

class Config:
    def __init__(self):
        self.TUSHARE_TOKEN = "410070664c78124d98ca5e81c3921530bd27534856b174c702d698a5"
        self.BASE_INTERVAL = 1  # 基础数据收集间隔(秒)
        self.DATA_RETENTION_HOURS = 10  # 保留多少小时的数据
        self.DEBUG_MODE = False  # 调试模式开关
        self.ALERT_COOLDOWN = 300  # 警报冷却时间（秒），5分钟内不重复发送相同警报
        self.MONITOR_STOCKS = self.load_monitor_stocks_config()

    def load_monitor_stocks_config(self):
        try:
            current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            config_path = os.path.join(current_dir, 'monitor_stocks.json')

            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)

                for stock_code, stock_config in config.items():
                    if "thresholds" in stock_config:
                        thresholds = {}
                        for window_sec, threshold_config in stock_config["thresholds"].items():
                            thresholds[str(window_sec)] = threshold_config
                        stock_config["thresholds"] = thresholds

                        if "windows_sec" not in stock_config:
                            stock_config["windows_sec"] = [int(sec) for sec in stock_config["thresholds"].keys()]

                    if "price_thresholds" not in stock_config:
                        stock_config["price_thresholds"] = []

                    if "change_thresholds" not in stock_config:
                        stock_config["change_thresholds"] = []
                return config
        except FileNotFoundError:
            print("警告: monitor_stocks.json 文件未找到")
            return {}
        except json.JSONDecodeError:
            print("错误: monitor_stocks.json 文件格式不正确")
            return {}

    def reload_config(self):
        self.MONITOR_STOCKS = self.load_monitor_stocks_config()