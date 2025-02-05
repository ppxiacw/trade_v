import tushare as ts
import pandas as pd
from dto.StockDataDay import StockDataDay
from dto.RealTimeStockData import RealTimeStockData
from datetime import datetime  # 正确导入 datetime 类
token  = '410070664c78124d98ca5e81c3921530bd27534856b174c702d698a5'
ts.set_token(token)
pro = ts.pro_api(token)
df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
df.to_csv('../files/stock_list.csv',index=False)
stock_list = pd.read_csv('../files/stock_list.csv',dtype={'symbol':str})
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





# 使用类进行分析
if __name__ == "__main__":

    # 读取原始文件
    df = pd.read_csv('../files/stock_list.csv', dtype={'symbol': str})
    # 提取前两位字符（针对ts_code列）
    df['code_prefix'] = df['ts_code'].str[:2]

    # 筛选条件
    filtered_df = df[(df['code_prefix'] == '60') | (df['code_prefix'] == '00')]

    # 删除临时列
    filtered_df = filtered_df.drop(columns=['code_prefix'])

    # 保存到新文件
    filtered_df.to_csv('stock_list_filter.csv', index=False, encoding='utf-8-sig')
    print("筛选完成！文件已保存为 stock_list_filter.csv")
