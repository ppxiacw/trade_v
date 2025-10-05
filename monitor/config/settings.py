import os
import json

class Config:
    def __init__(self):
        self.TUSHARE_TOKEN = "410070664c78124d98ca5e81c3921530bd27534856b174c702d698a5"
        self.BASE_INTERVAL = 1  # 基础数据收集间隔(秒)
        self.CONFIG_LIST = None
        self.DATA_RETENTION_HOURS = 10  # 保留多少小时的数据
        self.DEBUG_MODE = False  # 调试模式开关
        self.ALERT_COOLDOWN = 300  # 警报冷却时间（秒），5分钟内不重复发送相同警报
        self.MONITOR_STOCKS = self.load_monitor_stocks_config()

    def load_monitor_stocks_config(self):
        try:
            current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            config_dir = os.path.join(current_dir, 'config_files')  # 假设 JSON 文件在 config_files 文件夹中
            config = {}

            # 遍历文件夹中的所有文件
            for filename in os.listdir(config_dir):
                if filename.endswith('.json'):
                    file_path = os.path.join(config_dir, filename)
                    with open(file_path, 'r', encoding='utf-8') as f:
                        file_config = json.load(f)
                        config.update(file_config)  # 将每个文件的配置合并到总的配置中

            # 处理每个股票的配置
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
            self.CONFIG_LIST = config
            return config
        except FileNotFoundError:
            print("警告: 配置文件文件夹未找到")
            return {}
        except json.JSONDecodeError:
            print("错误: 配置文件格式不正确")
            return {}

    def reload_config(self):
        self.MONITOR_STOCKS = self.load_monitor_stocks_config()