from dto.StockDataDay import StockDataDay


class OneFilter:

    def __init__(self):
        pass

    @staticmethod
    def valid(df: StockDataDay):
        change_pct = (df.close - df.pre_close) / df.pre_close * 100
        # 实体大小-
        if 9 < change_pct or change_pct < -9.8:
            return False
        if df.close == df.open == df.high == df.low:
            return False
        return True
