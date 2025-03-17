import pandas as pd

from dto.StockDataDay import StockDataDay


class RealTimeStockData:
    def __init__(self, name, ts_code, date, time, open_price, pre_close, price, high, low, bid, ask, volume, amount):
        self.NAME = name
        self.TS_CODE = ts_code
        self.DATE = date
        self.TIME = time
        self.OPEN = open_price
        self.PRE_CLOSE = pre_close
        self.PRICE = price
        self.HIGH = high
        self.LOW = low
        self.BID = bid
        self.ASK = ask
        self.VOLUME = volume
        self.AMOUNT = amount

    def __repr__(self):
        return (f"RealTimeStockData(NAME={self.NAME}, TS_CODE={self.TS_CODE}, DATE={self.DATE}, TIME={self.TIME}, "
                f"OPEN={self.OPEN}, PRE_CLOSE={self.PRE_CLOSE}, PRICE={self.PRICE}, HIGH={self.HIGH}, "
                f"LOW={self.LOW}, BID={self.BID}, ASK={self.ASK}, VOLUME={self.VOLUME}, AMOUNT={self.AMOUNT})")

    @staticmethod
    def from_dataframe(df):
        """
        从DataFrame中创建StockData对象。
        假设DataFrame的列名与类的属性名一致。
        """
        if not isinstance(df, pd.DataFrame):
            raise ValueError("输入必须是一个pandas.DataFrame对象")

        # 确保DataFrame只有一行数据
        if len(df) != 1:
            raise ValueError("DataFrame必须只有一行数据")

        # 提取数据
        row = df.iloc[0]
        return StockDataDay(
            ts_code = row['TS_CODE'],
            open = row['OPEN'],
            pre_close = row['PRE_CLOSE'],
            close = row['PRICE'],
            high = row['HIGH'],
            low = row['LOW'],
            amount = row['AMOUNT'],
            vol = row['VOLUME']/100
        )