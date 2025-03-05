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
            magnification=1
            change_pct = (close-pre_close)/pre_close*100
            # 实体大小-
            physical_size = abs(open - close) / open
            # 下影线下探大于百分之二
            if 9 < change_pct  or change_pct< -9.8:
                return False
            if close < open:
                top_line = abs(open - df.high) / open
                bottom_line = ((close - low) / open)
            else:
                top_line = abs(close - df.high) / close
                bottom_line = ((open - low) / open)
            if abs((open-pre_close)/pre_close)>0.01:
                magnification=10000000*abs((open-pre_close)/pre_close)
            if physical_size == 0 :
                if  bottom_line>0.02 :
                    return bottom_line / physical_size*magnification
                elif top_line > 0.02:
                    return top_line / physical_size*magnification
                else:
                    return False
            # 实体不超过百分之一
            if (bottom_line / physical_size > 2):
                return bottom_line / physical_size*magnification
            elif (top_line / physical_size > 2):
                return top_line / physical_size*magnification
            else:
                return False
        except:
            return False


print(RedStar.valid(IndexAnalysis.get_stock_daily('601138','20250228')[0]))