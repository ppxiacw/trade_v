import tushare as ts
import pandas as pd
from dto.StockDataDay import StockDataDay
from dto.RealTimeStockData import RealTimeStockData
from datetime import datetime  # 正确导入 datetime 类
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import os
from utils.GetStockData import result

# 获取当前脚本的完整路径
current_path = os.path.abspath(__file__)

# 获取当前脚本的目录
dir_path = os.path.dirname(current_path)

# 获取当前脚本的上级目录
parent_dir_path = os.path.dirname(dir_path)

# 构造相对路径
relative_path = os.path.join(parent_dir_path, 'files')



token  = '410070664c78124d98ca5e81c3921530bd27534856b174c702d698a5'
ts.set_token(token)
pro = ts.pro_api(token)

stock_list  = result
class IndexAnalysis:
    def __init__(self):
        pass


    @staticmethod
    def get_stock_daily(ts_code, start_date, end_date=None):
        if end_date is None:
            end_date = start_date
        if len(ts_code)==6:
            ts_code = stock_list[stock_list['symbol'] == ts_code]['ts_code'].tolist()[0]
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        v= ts.pro_bar(ts_code=ts_code, adj='qfq', start_date=start_date, end_date=end_date)
        if v is None or  v.empty:
            return None
        # 将日期列转换为 datetime 类型，并设置为索引
        return StockDataDay.from_daily_dataframe(v)

    @staticmethod
    def calculate_pct(ts_code, start_date, end_date):
        df = ts.pro_bar(ts_code=ts_code, adj='qfq', start_date=start_date, end_date=end_date)
        change_pct =df['pct_chg'].sum()
        return change_pct

    @staticmethod
    def realtime_quote(ts_code):
        v:pd = ts.realtime_quote(ts_code=ts_code)
        arr = []
        for item in v.iterrows():
            arr.append(RealTimeStockData.from_dataframe(item[1].to_frame().T))
        return arr

    @staticmethod
    def stk_limit(date):
        df = pro.stk_limit(date)
        return df
    @staticmethod
    def stk_factor(ts_code,date):
        df = pro.stk_factor(ts_code=ts_code, start_date=date, end_date=date,
                            fields='ts_code,trade_date,rsi_6,rsi_12,rsi_24,kdj_j')
        return df



# 使用类进行分析
if __name__ == "__main__":
    # df = ts.pro_bar(ts_code='000001.SZ', adj='qfq', ma=[5,10,20,60],start_date='20250509', end_date='20250410')
    # 获取浦发银行60000.SH的历史分钟数据
    df = pro.stk_mins(ts_code='000001.SH', freq='1min', start_date='2025-06-27 09:30:00',
                      end_date='2025-07-27 09:40:00')
    print(str(df))

    print(ts.realtime_quote(ts_code='399006.SZ', src='sina'))
    # 软件上显示的9.35带上了集合竞价 也就是9.30那一个值
    # df = pro.stk_auction(ts_code='600919.SH',trade_date='20250627',
    #                      fields='ts_code, trade_date,vol,price,amount,turnover_rate,volume_ratio')
    print(df)




