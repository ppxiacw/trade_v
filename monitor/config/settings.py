import os
import json
from utils.tushare_utils import token
from .db_monitor import exe_query

class Config:
    def __init__(self):
        self.TUSHARE_TOKEN = token
        self.BASE_INTERVAL = 1  # 基础数据收集间隔(秒)
        self.CONFIG_LIST = None
        self.DATA_RETENTION_HOURS = 10  # 保留多少小时的数据
        self.DEBUG_MODE = False  # 调试模式开关
        self.ALERT_COOLDOWN = 300  # 警报冷却时间（秒），5分钟内不重复发送相同警报
        self.MONITOR_STOCKS = self.load_monitor_stocks_config()

    def load_monitor_stocks_config(self):
        value = exe_query('select * from stocks')
        self.CONFIG_LIST = {item['stock_code']: item for item in value}
        return self.CONFIG_LIST


    def reload_config(self):
        self.MONITOR_STOCKS = self.load_monitor_stocks_config()