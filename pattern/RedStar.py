from config.tushare_utils import IndexAnalysis
from dto.StockDataDay import StockDataDay
from utils import StockAnalysis
from dbconfig import db_pool
from filter.ContinuousRedFilter import ContinuousRedFilter
from filter.FluctuationRangeFilter import FluctuationRangeFilter
from filter.IntervalRangeFilter import IntervalRangeFilter

analysis = StockAnalysis()

yesterday = analysis.get_date_by_step(analysis.get_today(), -1)

conn = db_pool.get_connection()
cursor = conn.cursor()

# 执行SQL查询
query = f'SELECT DISTINCT max(high), ts_code FROM market WHERE trade_date >= DATE_SUB(CURDATE(), INTERVAL 10 DAY) GROUP BY ts_code;'
cursor.execute(query)

new_high = dict()

# 获取查询结果
results = cursor.fetchall()

for result in results:
    new_high[result[1]] = result[0]


class RedStar:

    def __init__(self):
        pass

    @staticmethod
    def valid(df: StockDataDay):
        try:
            if not ContinuousRedFilter.valid(df):
                print(f'{df.ts_code}被三红过滤')
                return False
            if not FluctuationRangeFilter.valid(df):
                print(f'{df.ts_code}被涨跌幅度2以上过滤')
                return False
            if not IntervalRangeFilter.valid(df):
                print(f'{df.ts_code}被十天波动波动小于5过滤或者和昨日差距小于0.5过滤')
                return False
            open = df.open
            close = df.close
            low = df.low
            pre_close = df.pre_close
            # 今天新高的不要
            if df.high >= new_high[df.ts_code]:
                print("==" + df.ts_code)
                return False
            # 涨幅
            magnification = 1
            change_pct = (close - pre_close) / pre_close * 100
            # 实体大小-
            physical_size = abs(open - close) / open
            # 下影线下探大于百分之二
            if 9 < change_pct or change_pct < -9.8:
                return False
            if close < open:
                bottom_line = ((close - low) / open)
            else:
                bottom_line = ((open - low) / open)
            if abs((open - pre_close) / pre_close) > 0.01:
                magnification = 10000000 * abs((open - pre_close) / pre_close)
            if physical_size == 0:
                if bottom_line > 0.02:
                    return bottom_line / physical_size * magnification
                else:
                    return False
            if (bottom_line / physical_size > 2):
                return bottom_line / physical_size * magnification
            else:
                return False
        except:
            return False


# print(RedStar.valid(IndexAnalysis.get_stock_daily('605339', '20250307')[0]))
