import os
import json
import logging
from utils.tushare_utils import token
from .db_monitor import exe_query, db_manager
from .stock_code import normalize_monitor_stock_code


_logger = logging.getLogger(__name__)

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
        value = exe_query('select * from stocks where is_monitor = 1')
        normalized_config = {}
        normalized_seen = {}

        for item in value:
            original_code = item.get('stock_code')
            normalized_code = normalize_monitor_stock_code(original_code, item.get('stock_name'))
            item['stock_code'] = normalized_code

            # 将历史/非标准代码规范化回数据库，避免后续路径参数与配置键不一致
            if item.get('id') and normalized_code and normalized_code != original_code:
                duplicate_id = normalized_seen.get(normalized_code)
                if duplicate_id and duplicate_id != item['id']:
                    _logger.warning(
                        "监控股票代码规范化冲突: id=%s code=%s -> %s 已被 id=%s 占用",
                        item['id'], original_code, normalized_code, duplicate_id
                    )
                else:
                    db_manager.execute_update(
                        'stocks',
                        {'stock_code': normalized_code},
                        'id = %s',
                        (item['id'],)
                    )

            normalized_seen[normalized_code] = item.get('id')
            normalized_config[normalized_code] = item

        self.CONFIG_LIST = normalized_config
        return self.CONFIG_LIST


    def reload_config(self):
        self.MONITOR_STOCKS = self.load_monitor_stocks_config()