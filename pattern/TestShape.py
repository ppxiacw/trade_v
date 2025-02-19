import json

import pandas as pd
from flask import send_file

from  pattern.RedStar import RedStar
from pattern.NewHigh import NewHigh
from config.tushare_utils import IndexAnalysis
from trade_schedule import AppendMarketData,UpdateFiles

import os

UpdateFiles.new_high_()

# 获取当前脚本的完整路径
current_path = os.path.abspath(__file__)

# 获取当前脚本的目录
dir_path = os.path.dirname(current_path)

# 获取当前脚本的上级目录
parent_dir_path = os.path.dirname(dir_path)

# 构造相对路径
relative_path = os.path.join(parent_dir_path, 'files')

large_cap_stocks = pd.read_csv(f'{relative_path}/stock_list_filter.csv',dtype={'symbol':str})
large_cap_stocks = large_cap_stocks[~large_cap_stocks['name'].str.contains('ST', na=False)]
large_cap_stocks = large_cap_stocks['ts_code'].tolist()

new_high_codes = pd.read_csv(f'{relative_path}/new_high.csv',dtype={'symbol':str})['TS_CODE'].tolist()


arr = []
batch_size = 20  # 每批处理10个代码


def find_bottom_line():
    # 分批次调用接口
    for i in range(0, len(large_cap_stocks), batch_size):
        batch = large_cap_stocks[i:i + batch_size]
        # 关键点：直接传入当前批次的 ts_code 数组
        quotes = IndexAnalysis.realtime_quote(','.join(f'{x}' for x in batch))
        for quote in quotes:
            value = RedStar.valid(quote)
            if value:
                v = {
                    "ts_code": quote.ts_code,
                    "bottom_line":value
                }
                arr.append(v)
    sorted_data = sorted(arr, key=lambda x: x['bottom_line'], reverse=True)
    ts_codes = [d['ts_code'] for d in sorted_data]
    data_str = '\n'.join(ts_codes)
    # 将列表转换为字符串

    # 将字符串写入文件
    with open('codes.txt', 'w') as f:
        f.write(data_str)

    # 返回文件
    return send_file('codes.txt', as_attachment=True)


def find_new_high():
    codes = ''
    for i in range(0, len(new_high_codes), batch_size):
        batch = new_high_codes[i:i + batch_size]
        # 关键点：直接传入当前批次的 ts_code 数组
        quotes = IndexAnalysis.realtime_quote(','.join(f'{x}' for x in batch))
        for quote in quotes:
            value = NewHigh.valid(quote)
            if value:
                codes= codes+(quote.ts_code+'<br>')
    return codes