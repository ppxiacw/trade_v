import json

import pandas as pd
from flask import send_file

from  pattern.RedStar import RedStar
from config.tushare_utils import IndexAnalysis
from trade_schedule import AppendMarketData
from pattern.FindShrinkage import FindShrinkage
from pattern.ShirnkageAfter import ShirnkageAfter
import os
from datetime import datetime
from pattern.ShrinkageByDate import ShrinkageByDate
from filter.OneFilter import OneFilter


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
# 获取当前日期和时间
current_datetime = datetime.now()

# 格式化日期和时间为 'yy-mm' 格式
today = current_datetime.strftime('%Y-%m-%d')


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
                    "score":value
                }
                arr.append(v)
    sorted_data = sorted(arr, key=lambda x: x['score'], reverse=True)
    ts_codes = [d['ts_code'] for d in sorted_data]
    data_str = '\n'.join(ts_codes)
    # 将列表转换为字符串
    fileName = f'{today}下影线.txt'
    # 将字符串写入文件
    with open(fileName, 'w') as f:
        f.write(data_str)

    # 返回文件
    return send_file(fileName, as_attachment=True)



def find_shrinkage():
    # 存储股票代码与 valid 返回值的映射关系
    code_value_map = {}

    for i in range(0, len(large_cap_stocks), batch_size):
        batch = large_cap_stocks[i:i + batch_size]
        quotes = IndexAnalysis.realtime_quote(','.join(str(x) for x in batch))

        for quote in quotes:
            value = FindShrinkage.valid(quote)  # 调用 valid 方法获取返回值
            if value is not None:  # 仅处理有效值
                code_value_map[quote.ts_code] = value

    # 按 valid 返回值降序排序，返回股票代码列表
    sorted_codes = sorted(
        code_value_map.items(),
        key=lambda x: x[1],
        reverse=True
    )
    fileName = f'{today}缩量.txt'

    arr =  [ts_code+'\n' for ts_code, _ in sorted_codes]
    with open(fileName,'w',encoding='utf-8')as f:
        f.writelines(arr)
    return send_file(fileName, as_attachment=True)


def find_shirnkage_after():
    # 存储股票代码与 valid 返回值的映射关系
    code_value_map = {}

    for i in range(0, len(large_cap_stocks), batch_size):
        batch = large_cap_stocks[i:i + batch_size]
        quotes = IndexAnalysis.realtime_quote(','.join(str(x) for x in batch))

        for quote in quotes:
            value = ShirnkageAfter.valid(quote)  # 调用 valid 方法获取返回值
            if value is not None:  # 仅处理有效值
                code_value_map[quote.ts_code] = value


    # 按 valid 返回值降序排序，返回股票代码列表
    sorted_codes = sorted(
        code_value_map.items(),
        key=lambda x: x[1],
        reverse=True
    )
    fileName = f'{today}缩量盘后.txt'

    arr =  [ts_code+'\n' for ts_code, _ in sorted_codes]
    with open(fileName,'w',encoding='utf-8')as f:
        f.writelines(arr)
    return send_file(fileName, as_attachment=True)


def find_shirnkage_by_date_after():
    # 存储股票代码与 valid 返回值的映射关系
    code_value_map = {}

    for i in range(0, len(large_cap_stocks), batch_size):
        batch = large_cap_stocks[i:i + batch_size]
        quotes = IndexAnalysis.realtime_quote(','.join(str(x) for x in batch))

        for quote in quotes:
            if not OneFilter.valid(quote):
                continue
            value = ShrinkageByDate.find_distance(quote)  # 调用 valid 方法获取返回值
            if value is not None:  # 仅处理有效值
                code_value_map[quote.ts_code] = value
            print(f'{quote.ts_code}+{value}')


    # 按 valid 返回值降序排序，返回股票代码列表
    sorted_codes = sorted(
        code_value_map.items(),
        key=lambda x: x[1],
        reverse=True
    )
    fileName = f'{today}缩量盘后根据日期.txt'

    arr =  [ts_code+str(code_value_map[ts_code])+'\n' for ts_code, _ in sorted_codes]
    with open(fileName,'w',encoding='utf-8')as f:
        f.writelines(arr)
    # return send_file(fileName, as_attachment=True)

# find_bottom_line()
if __name__ == "__main__":
    find_shirnkage_by_date_after()