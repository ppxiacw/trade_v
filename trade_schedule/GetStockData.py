import tushare as ts
import pandas as pd
from utils.date_utils import Date_utils
# 初始化
TOKEN = '410070664c78124d98ca5e81c3921530bd27534856b174c702d698a5'  # ← 替换这里
pro = ts.pro_api(TOKEN)

# 获取最近交易日
last_trade_date = pro.trade_cal(exchange='SSE', end_date='20250730', is_open=1)['cal_date'].iloc[-1]

# 获取基础信息
df_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name')

# 获取市值数据
df_daily = pro.daily_basic(
    trade_date=Date_utils.get_today(replace=True),
    fields='ts_code,close,total_mv',
    limit=6000  # 关键：指定获取数据量
)

# 合并结果
result = pd.merge(df_basic, df_daily, on='ts_code')

# 转换市值单位：万元 → 亿元
result['total_mv'] = (result['total_mv'] / 10000).round().astype(int) # 除以10000转换为亿元


print("数据已保存")