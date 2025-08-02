from utils.tushare_utils import IndexAnalysis
from dto.StockDataDay import StockDataDay
from utils.date_utils import Date_utils
from filter.ContinuousRedFilter import ContinuousRedFilter
from filter.FluctuationRangeFilter import FluctuationRangeFilter
from filter.IntervalRangeFilter import IntervalRangeFilter
from filter.NewHighFilter import NewHighFilter
from filter.OneFilter import OneFilter
analysis = Date_utils()




class RedStar:

    def __init__(self):
        pass

    @staticmethod
    def valid(df: StockDataDay):
        try:
            filters = [
                (OneFilter.valid, '一字板过滤'),
                (ContinuousRedFilter.valid, '被三红过滤'),
                (FluctuationRangeFilter.valid, '被涨跌幅度2以上过滤'),
                (IntervalRangeFilter.valid, '被十天波动波动小于5过滤或者和昨日差距小于0.5过滤'),
                (NewHighFilter.valid, '离最高点太近过滤')
            ]

            for condition, message in filters:
                if not condition(df):
                    print(f'{df.ts_code}{message}')
                    return False

            open = df.open
            close = df.close
            low = df.low
            pre_close = df.pre_close

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
            # if abs((open - pre_close) / pre_close) > 0.01:
            #     magnification = 10000000 * abs((open - pre_close) / pre_close)
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

if __name__ == "__main__":
    print(RedStar.valid(IndexAnalysis.get_stock_daily('605117.SH','20250409')[0]))