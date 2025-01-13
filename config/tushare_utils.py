
import tushare as ts
import pandas as pd
token = '410070664c78124d98ca5e81c3921530bd27534856b174c702d698a5'

# 初始化pro接口，并直接传递Token
pro = ts.pro_api(token)

# 获取沪深300日K线数据


def calculate_pct(start_date,end_date):
    df = pro.index_daily(ts_code='000300.SH', start_date=start_date, end_date=end_date)
    change_pct = (df.iloc[len(df) - 3]['close']-df.iloc[len(df)-1]['open'])/df.iloc[len(df)-1]['open']*100
    return change_pct





# data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date,market')
#
# data.to_csv('stock_info.csv')
# print(data)