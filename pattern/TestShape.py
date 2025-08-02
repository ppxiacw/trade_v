import pandas as pd

from  pattern.RedStar import RedStar
from utils.tushare_utils import IndexAnalysis
from pattern.FindShrinkage import FindShrinkage
from pattern.ShirnkageAfter import ShirnkageAfter
import os
from datetime import datetime
from pattern.ShrinkageByDate import ShrinkageByDate
from filter.OneFilter import OneFilter
from utils.send_dingding import *
import time

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



batch_size = 20  # 每批处理10个代码
# 获取当前日期和时间
current_datetime = datetime.now()

# 格式化日期和时间为 'yy-mm' 格式
today = current_datetime.strftime('%Y%m%d')


def find_bottom_line(datestr=None):
    arr = []
    if datestr == None:
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
    else:
        for i, ts_code in enumerate(large_cap_stocks):
            v= IndexAnalysis.get_stock_daily(ts_code,datestr)
            if v:
                value = RedStar.valid(IndexAnalysis.get_stock_daily(ts_code,datestr)[0])
                if value:
                    v = {
                        "ts_code":ts_code,
                        "score": value
                    }
                    arr.append(v)
    sorted_data = sorted(arr, key=lambda x: x['score'], reverse=True)
    ts_codes = [d['ts_code'] for d in sorted_data]
    data_str = '\n'.join(ts_codes)
    # 将列表转换为字符串
    fileName = f'{relative_path}/my_files/bottom_line_files/{today}下影线.txt'
    # 将字符串写入文件
    with open(fileName, 'w') as f:
        f.write(data_str)

    for sorted_code in sorted_data[:40]:
        send_dingtalk_message("下影线提醒",sorted_code['ts_code'],bottom_line_webhook_url)
    # 返回文件



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
    fileName = f'{relative_path}/my_files/shrinkage_files/{today}缩量.txt'

    arr =  [ts_code+'\n' for ts_code, _ in sorted_codes]
    with open(fileName,'w',encoding='utf-8')as f:
        f.writelines(arr)

    for sorted_code in sorted_codes[:40]:
        send_dingtalk_message("缩量提醒",sorted_code[0],shrink_webhook_url)




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
    fileName = f'my_files/shrinkage_files/{today}缩量盘后.txt'

    arr =  [ts_code+'\n' for ts_code, _ in sorted_codes]
    with open(fileName,'w',encoding='utf-8')as f:
        f.writelines(arr)


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
    fileName = f'{relative_path}/my_files/{today}缩量盘后根据日期.txt'

    arr =  [ts_code+str(code_value_map[ts_code])+'\n' for ts_code, _ in sorted_codes]
    with open(fileName,'w',encoding='utf-8')as f:
        f.writelines(arr)
    # return send_file(fileName, as_attachment=True)




def find_rsi():
    call_count = 0  # 记录当前时间窗口内的调用次数
    window_start = time.time()  # 时间窗口的起始时间

    for ts_code in large_cap_stocks:
        current_time = time.time()
        elapsed_time = current_time - window_start

        # 如果当前时间窗口已超过60秒，重置窗口和计数器
        if elapsed_time >= 60:
            call_count = 0
            window_start = current_time
        else:
            # 如果当前窗口内调用次数已达上限，等待至下一个窗口
            if call_count >= 70:
                sleep_time = 60 - elapsed_time
                time.sleep(sleep_time)
                # 重置窗口和计数器
                call_count = 0
                window_start = time.time()

        # 调用 API 并增加计数器
        quotes = IndexAnalysis.stk_factor(ts_code, today)
        call_count += 1
        try:
            # 处理 RSI 逻辑
            rsi_6 = quotes['rsi_6'][0]
            if rsi_6 < 30:
                print(f'{ts_code},{rsi_6}')
        except:
            print(quotes)


# find_bottom_line()
if __name__ == "__main__":
    # find_shirnkage_by_date_after()
    find_rsi()
    pass