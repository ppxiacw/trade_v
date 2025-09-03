import os

import tushare as ts
import pandas as pd
from datetime import datetime
import json
from utils.tushare_utils import IndexAnalysis
from dto.StockDataDay import StockDataDay


class DataFetcher:
    def __init__(self, config, debug_mode=False):
        self.config = config
        self.debug_mode = debug_mode

        if not debug_mode:
            ts.set_token(config.TUSHARE_TOKEN)
            self.pro = ts.pro_api()

    def fetch_realtime_data(self, stock_codes):
        if self.debug_mode:
            return self.get_manual_data()
        else:
            return IndexAnalysis.realtime_quote(ts_code=",".join(stock_codes))

    def get_manual_data(self, json_file="manual.json"):
        try:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(current_dir, 'manual.json')
            with open(config_path, 'r', encoding='utf-8') as f:
                data_list = json.load(f)

            result = []
            for i, data in enumerate(data_list):
                if 'time' in data:
                    timestamp = datetime.strptime(data['time'], "%Y-%m-%d %H:%M:%S")
                else:
                    timestamp = datetime.now()

                data_point = pd.Series({
                    'stock_code': data['ts_code'],
                    'open': float(data['open']),
                    'high': float(data['high']),
                    'low': float(data['low']),
                    'close': float(data['close']),
                    'vol': int(data['vol']),
                    'pre_close': float(data.get('pre_close', data['close'])),
                    'time': timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                })
                result.append(StockDataDay.from_json(data_point))

            return result
        except Exception as e:
            print(f"获取手动数据失败: {e}")
            return []