from config.tushare_utils import IndexAnalysis
from dto.StockDataDay import StockDataDay
from interface import Shape


class RedStar:

    def __init__(self):
        pass

    @staticmethod
    def valid(df: StockDataDay):
        try:
            open = df.open
            close = df.close
            low = df.low
            pre_close = df.pre_close
            #涨幅
            change_pct = (close-pre_close)/pre_close*100
            # 实体大小-
            physical_size = abs(open - close) / open
            # 下影线下探大于百分之二
            if close < open:
                top_line = abs(open - df.high) / open
                bottom_line = ((close - low) / open)
            else:
                top_line = abs(close - df.high) / close
                bottom_line = ((open - low) / open)
            if physical_size == 0:
                if  bottom_line>0.02:
                    return True
                else:
                    return False
            # 实体不超过百分之一
            if (bottom_line / physical_size > 3) and 9 > change_pct > -9.8:
                return True
            else:
                return False
        except:
            return False


print(RedStar.valid(IndexAnalysis.get_stock_daily('603878','20241016')))