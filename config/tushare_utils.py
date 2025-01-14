
import tushare as ts
import pandas as pd
token = '410070664c78124d98ca5e81c3921530bd27534856b174c702d698a5'
from datetime import datetime, timedelta


# 初始化pro接口，并直接传递Token
pro = ts.pro_api(token)

# 获取沪深300日K线数据


def calculate_pct(start_date,end_date):
    df = pro.index_daily(ts_code='000300.SH', start_date=start_date, end_date=end_date)
    change_pct = (df.iloc[len(df) - 3]['close']-df.iloc[len(df)-1]['open'])/df.iloc[len(df)-1]['open']*100
    return change_pct

# 获取沪深300指数的日线行情数据
index_data = pro.index_daily(ts_code='000300.SH', start_date='20150101', end_date=datetime.now().strftime('%Y%m%d'))

# 将日期列转换为 datetime 类型，并设置为索引
index_data['trade_date'] = pd.to_datetime(index_data['trade_date'])
index_data.set_index('trade_date', inplace=True)

# 计算涨跌幅
index_data['pct_change'] = index_data['close'].pct_change() * 100
# 筛选出涨幅大于2%的日子
up_2_percent_days = index_data[index_data['pct_change'] > 2].index

# 初始化结果字典
results = {
    'next_day_up': 0,
    'next_day_down': 0,
    'high_open_low_close': 0,
    'low_open_high_close': 0,
    'next_day_equals': 0,
    'total_days': len(up_2_percent_days)
}

for day in up_2_percent_days:
    next_day = day + timedelta(days=1)

    if next_day in index_data.index:
        next_day_data = index_data.loc[next_day]

        # 统计第二天涨跌情况
        if next_day_data['pct_change'] > 0:
            results['next_day_up'] += 1
        elif next_day_data['pct_change'] < 0:
            results['next_day_down'] += 1
        else:
            results['next_day_equals'] += 1
        # 统计高开低走或低开高走情况
        if next_day_data['open'] > next_day_data['close']:
            results['high_open_low_close'] += 1
        elif next_day_data['open'] < next_day_data['close']:
            results['low_open_high_close'] += 1

# 计算百分比
for key in ['next_day_up', 'next_day_down', 'high_open_low_close', 'low_open_high_close']:
    results[key] = (results[key] / results['total_days']) * 100 if results['total_days'] != 0 else 0

print(results)

# 按时间排序
index_data.sort_index(inplace=True)