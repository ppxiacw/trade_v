from utils import stockAnalysis
import tushare as ts

token  = '410070664c78124d98ca5e81c3921530bd27534856b174c702d698a5'
ts.set_token(token)
pro = ts.pro_api(token)
# 获取所有板块列表
print(pro.rt_min(ts_code='000001.SH', freq='1MIN'))



today = stockAnalysis.get_today()
testDate = "'2025-04-09'"
testDate = f'"{today}"'
print(1)