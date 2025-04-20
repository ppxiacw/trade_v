from config.tushare_utils import IndexAnalysis
from dto.StockDataDay import StockDataDay
from utils import StockAnalysis
from config.dbconfig import  db_pool
from filter.ContinuousRedFilter import ContinuousRedFilter
from filter.FluctuationRangeFilter import FluctuationRangeFilter
from filter.IntervalRangeFilter import IntervalRangeFilter
from filter.TurnoverRateFilter import TurnoverRateFilter
conn = db_pool.get_connection()
cursor = conn.cursor()

analysis = StockAnalysis()

yesterday = analysis.get_date_by_step(analysis.get_today(),-1)

# 执行SQL查询
query = f'select vol,ts_code from market where trade_date = "{yesterday}" and close-market.open<0;'
cursor.execute(query)

shrinkage_dict = dict()

# 获取查询结果
results = cursor.fetchall()

for result in results:
    shrinkage_dict[result[1]] = result[0]


class FindShrinkage:

    def __init__(self):
        pass

    @staticmethod
    def valid(df: StockDataDay):
        if not ContinuousRedFilter.valid(df):
            print(f'{df.ts_code}被三红过滤')
            return None
        if not FluctuationRangeFilter.valid(df):
            print(f'{df.ts_code}被涨跌幅度2以上过滤')
            return None
        if not IntervalRangeFilter.valid(df):
            print(f'{df.ts_code}被十天波动波动小于5过滤或者和昨日差距小于0.5过滤')
            return None
        if not TurnoverRateFilter.valid(df):
            print(f'{df.ts_code}被换手率要求过滤')
            return None
        vol = df.vol
        if vol == 0:
           return None
        if vol<shrinkage_dict.get(df.ts_code,0) and df.close > df.open:
            return shrinkage_dict[df.ts_code]/vol


# print(FindShrinkage.valid(IndexAnalysis.get_stock_daily('000151.SZ','20250326')[0]))