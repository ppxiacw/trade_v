import pandas as pd
from datetime import datetime
from utils.tushare_utils import IndexAnalysis
from functools import lru_cache
from collections import OrderedDict
from utils.date_utils import Date_utils

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
    获取缓存的历史成交量数据（使用腾讯接口）
    """
    if not str(datetime.now().date()) == Date_utils.get_today():
        time_str = "15:01:00"
    cache_key = f"{stock_code}_{date_str}_{time_str}"
    cached_data = volume_cache.get(cache_key)

    if cached_data is not None:
        return cached_data

    # 如果没有缓存，使用腾讯接口获取分钟数据
    try:
        # 使用腾讯接口获取分钟数据
        data = IndexAnalysis.rt_min(stock_code, k_type=1, num=320)
        
        if data is not None and not data.empty:
            # 转换日期格式用于筛选
            date_fmt = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            
            # 筛选指定日期和时间范围的数据
            mask = (data['candle_end_time'].dt.date == pd.to_datetime(date_fmt).date()) & \
                   (data['candle_end_time'].dt.strftime('%H:%M:%S') <= time_str)
            filtered_data = data[mask]
            
            if not filtered_data.empty:
                volume = filtered_data['amount'].sum()  # 已经是手为单位
            else:
                volume = 0
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




def get_volume_ratio_simple(stock_list, current_time=None):
    return get_volume_ratio_batch(stock_list, current_time)