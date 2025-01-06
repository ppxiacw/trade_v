from datetime import datetime

import pandas as pd
import bisect
import pandas_market_calendars as mcal


def get_previous_trading_days(start_date, n_days):
    # 获取中国上海证券交易所的交易日历
    nyse = mcal.get_calendar('SSE')  # 注意：SSE可能不是正确的标识符，请检查pandas_market_calendars的文档

    # 将起始日期转换为pandas的datetime格式
    start_dt = pd.to_datetime(start_date)

    # 使用交易日历找到一定范围内的所有交易日
    # 注意：这里需要确定一个合理的范围来包含足够的交易日
    # 这里我们假设过去一年足够长，但可能需要根据实际情况调整
    start_range = start_dt - pd.Timedelta(days=365)
    end_range = start_dt
    schedule = nyse.schedule(start_date=start_range, end_date=end_range)
    trading_days = schedule.index.date.tolist()  # 提取日期部分并转换为列表
    trading_days.sort()  # 确保列表是有序的（通常从交易日历获取的日期已经是有序的）

    # 使用bisect_right找到起始日期在交易日列表中的位置（或应该插入的位置）
    start_index = bisect.bisect_right(trading_days, start_dt.date())

    # 如果起始日期不在交易日列表中，则start_index会指向下一个应该插入的位置
    # 我们需要找到最近的交易日，即start_index-1（如果它不小于0）
    if start_index > 0:
        start_index -= 1
    else:
        # 如果起始日期之前的交易日不足n_days个，则处理这种情况
        # 这里可以选择抛出异常、返回空列表或返回实际可用的交易日
        raise ValueError("起始日期之前没有足够的交易日数据")

    # 确保我们有足够的交易日来计算前N个交易日
    if start_index < n_days - 1:
        raise ValueError("起始日期之前没有足够的交易日来满足请求的数量")

    # 计算前N个交易日的列表
    previous_trading_days = trading_days[start_index -  + 1:start_index + 1][::-1][:n_days]
    # 注意：上面的切片可能不是最优的，特别是当start_index接近列表开头时
    # 一个更简洁的方法是直接切片到所需长度，但这里为了保持与原始逻辑相似而保留

    # 将结果转换为字符串格式
    previous_trading_days_str = [date.strftime('%Y-%m-%d') for date in previous_trading_days]

    return previous_trading_days_str


# 示例使用
start_date = '2025-01-08'
n_days = 7
try:
    previous_trading_days = get_previous_trading_days(start_date, n_days)
    print(previous_trading_days)
except ValueError as e:
    print(e)

def get_today():
    now = datetime.now()
    # 格式化日期为 yyyy-mm-dd
    today = now.strftime('%Y-%m-%d')
    return today


