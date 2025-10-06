import pandas as pd
from datetime import datetime
from utils.tushare_utils import IndexAnalysis, pro
import time
import threading
from functools import lru_cache
from collections import OrderedDict
# 假设您有Date_utils模块，包含get_date_by_step和get_today方法
from utils.date_utils import Date_utils
from utils.GetStockData import get_stock_name

# 缓存历史成交量数据，避免重复请求
class VolumeCache:
    def __init__(self, max_size=1000):
        self.cache = OrderedDict()
        self.max_size = max_size

    def get(self, key):
        if key in self.cache:
            # 将访问的项目移到末尾表示最近使用
            value = self.cache.pop(key)
            self.cache[key] = value
            return value
        return None

    def set(self, key, value):
        if key in self.cache:
            # 如果已存在，先删除
            self.cache.pop(key)
        elif len(self.cache) >= self.max_size:
            # 如果缓存已满，删除最久未使用的项目
            self.cache.popitem(last=False)
        self.cache[key] = value


# 创建全局缓存实例
volume_cache = VolumeCache()


# 使用LRU缓存装饰器缓存获取历史数据的函数
@lru_cache(maxsize=100)
def get_cached_historical_volume(stock_code, date_str, time_str):
    """
    获取缓存的历史成交量数据
    """
    cache_key = f"{stock_code}_{date_str}_{time_str}"
    cached_data = volume_cache.get(cache_key)

    if cached_data is not None:
        return cached_data

    # 如果没有缓存，从API获取数据
    try:
        data = pro.stk_mins(
            ts_code=stock_code,
            freq='1min',
            start_date=f"{date_str} 09:00:00",
            end_date=f"{date_str} {time_str}"
        )

        # 计算成交量总和
        if not data.empty:
            volume = data['vol'].sum() / 100  # 转换为手
        else:
            volume = 0

        # 将结果存入缓存
        volume_cache.set(cache_key, volume)
        return volume

    except Exception as e:
        print(f"获取历史数据失败 {stock_code} {date_str}: {e}")
        return 0


def get_volume_ratio_batch(stock_list, current_time=None):
    """
    批量获取股票当日成交量与前一交易日同期比值

    Parameters:
    -----------
    stock_list : list
        股票代码列表，如 ['600410.SH', '000001.SZ']
    current_time : str, optional
        当前时间，格式 'YYYY-MM-DD HH:MM:SS'，默认为当前系统时间

    Returns:
    --------
    dict
        股票代码为键，包含成交量比值的字典
    """

    # 如果没有指定当前时间，使用系统当前时间
    if current_time is None:
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # 解析当前时间
    current_dt = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')

    # 使用Date_utils获取前一交易日
    today = Date_utils.get_today()  # 格式应为 'YYYYMMDD'
    previous_trade_date = Date_utils.get_date_by_step(today, -1)  # 格式应为 'YYYYMMDD'

    # 获取当前时间的小时和分钟
    current_hour_min = current_dt.strftime('%H:%M:%S')

    volume_ratios = {}

    for stock in stock_list:
        # 获取今日实时成交量（从realtime_quote获取）
        today_real_data = IndexAnalysis.realtime_quote(stock)
        if today_real_data and len(today_real_data) > 0:
            today_volume = today_real_data[0].vol  # 单位：手
        else:
            today_volume = 0

        # 使用缓存获取前一交易日同期成交量
        previous_volume_hand = get_cached_historical_volume(
            stock, previous_trade_date, current_hour_min
        )

        # 计算成交量比值（避免除零）
        if previous_volume_hand > 0:
            ratio = today_volume / previous_volume_hand
        else:
            ratio = float('inf')  # 如果前一交易日成交量为0，返回无穷大

        volume_ratios[stock] = {
            'today_volume': today_volume,  # 今日成交量（手）
            'previous_volume': previous_volume_hand,  # 前一交易日同期成交量（手）
            'volume_ratio': ratio,
            'current_time': current_time,
            'previous_trade_date': previous_trade_date
        }

    return volume_ratios


def format_volume_data_table(data_dict):
    """
    将成交量数据字典格式化为表格字符串
    """
    if not data_dict:
        return "暂无数据"

    # 定义表格列
    headers = ["股票代码", "今日成交量", "前日成交量", "量比", "当前时间", "前交易日"]

    # 计算每列的最大宽度
    col_widths = [len(header) for header in headers]

    # 更新列宽基于数据内容
    for stock, info in data_dict.items():
        col_widths[0] = max(col_widths[0], len(stock))
        col_widths[1] = max(col_widths[1], len(f"{info['today_volume']:,.2f}"))
        col_widths[2] = max(col_widths[2], len(f"{info['previous_volume']:,.2f}"))
        col_widths[3] = max(col_widths[3], len(f"{info['volume_ratio']:.4f}"))
        col_widths[4] = max(col_widths[4], len(info['current_time']))
        col_widths[5] = max(col_widths[5], len(info['previous_trade_date']))

    # 每列增加2个字符的边距
    col_widths = [w + 2 for w in col_widths]

    # 构建表格
    table_lines = []

    # 表头分隔线
    separator = "+" + "+".join("-" * w for w in col_widths) + "+"
    table_lines.append(separator)

    # 表头
    header_line = "|"
    for i, header in enumerate(headers):
        header_line += f" {header:<{col_widths[i] - 1}}|"
    table_lines.append(header_line)
    table_lines.append(separator)

    # 数据行
    for stock, info in data_dict.items():
        row_line = "|"
        row_line += f" {stock:<{col_widths[0] - 1}}|"
        row_line += f" {get_stock_name(stock):>{col_widths[1] - 2}} |"
        row_line += f" {info['today_volume']:>{col_widths[1] - 2},.2f} |"
        row_line += f" {info['previous_volume']:>{col_widths[2] - 2},.2f} |"
        row_line += f" {info['volume_ratio']:>{col_widths[3] - 2}.4f} |"
        row_line += f" {info['current_time']:<{col_widths[4] - 1}}|"
        row_line += f" {info['previous_trade_date']:<{col_widths[5] - 1}}|"
        table_lines.append(row_line)

    table_lines.append(separator)
    print("\n".join(table_lines))


# 修改原函数以返回表格
def get_volume_ratio_simple(stock_list, current_time=None):
    """
    简化版本，以表格形式返回所有成交量数据
    """
    full_data = get_volume_ratio_batch(stock_list, current_time)


    # 返回表格形式的字符串
    return format_volume_data_table(full_data)