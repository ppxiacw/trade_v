import pandas as pd


class StockDataDay:
    def __init__(self, ts_code, open, high, low, close, pre_close, amount, trade_date=None, change=None, pct_chg=None, vol=None):
        self.ts_code = ts_code  # 股票代码
        self.trade_date = trade_date  # 交易日期
        self.open = open  # 开盘价
        self.high = high  # 最高价
        self.low = low  # 最低价
        self.close = close  # 收盘价
        self.pre_close = pre_close  # 昨收价【除权价，前复权】
        self.change = change  # 涨跌额
        self.pct_chg = pct_chg  # 涨跌幅 【基于除权后的昨收计算的涨跌幅：（今收-除权昨收）/除权昨收 】
        self.vol = vol  # 成交量 （手）
        self.amount = amount  # 成交额 （千元）

    def __repr__(self):
        return (f"StockData(ts_code={self.ts_code}, trade_date={self.trade_date}, open={self.open}, high={self.high}, "
                f"low={self.low}, close={self.close}, pre_close={self.pre_close}, change={self.change}, "
                f"pct_chg={self.pct_chg}, vol={self.vol}, amount={self.amount})")

    @staticmethod
    def from_daily_dataframe(df):
        """
        从 DataFrame 创建 StockData 实例。
        DataFrame 的列名需要与类的属性名一致。
        """
        # 确保 DataFrame 的列名与类的属性名一致
        required_columns = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close',
                            'pre_close', 'change', 'pct_chg', 'vol', 'amount']
        if not all(column in df.columns for column in required_columns):
            raise ValueError(f"DataFrame 必须包含以下列: {required_columns}")

        # 从 DataFrame 中提取数据并创建实例
        return StockDataDay(
            ts_code=df['ts_code'].values[0],
            trade_date=df['trade_date'].values[0],
            open=df['open'].values[0],
            high=df['high'].values[0],
            low=df['low'].values[0],
            close=df['close'].values[0],
            pre_close=df['pre_close'].values[0],
            change=df['change'].values[0],
            pct_chg=df['pct_chg'].values[0],
            vol=df['vol'].values[0],
            amount=df['amount'].values[0]
        )

    @staticmethod
    def from_json(df):
        """
        从 DataFrame 创建 StockData 实例。
        DataFrame 的列名需要与类的属性名一致。
        """

        # 从 DataFrame 中提取数据并创建实例
        return StockDataDay(
            ts_code=df['stock_code'],
            trade_date=df['trade_date'],
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close'],
            pre_close=df['pre_close'],
            change=df['change'],
            pct_chg=df['change_pct'],
            vol=df['volume'],
            amount=df['amount']
        )

    @staticmethod
    def from_real_time_dataframe(df):
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
            ts_code=row['TS_CODE'],
            open=row['OPEN'],
            pre_close=row['PRE_CLOSE'],
            close=row['PRICE'],
            high=row['HIGH'],
            low=row['LOW'],
            amount=row['AMOUNT']
        )

