from datetime import datetime


class StockBasic:
    def __init__(self, ts_code, symbol, name, area, industry,):
        """
        初始化股票基本信息对象
        :param ts_code: 股票代码
        :param symbol: 股票简称
        :param name: 股票名称
        :param area: 所在地域
        :param industry: 所属行业
        """
        self.ts_code = ts_code
        self.symbol = symbol
        self.name = name
        self.area = area
        self.industry = industry

    def get_info(self):
        """
        获取股票的基本信息
        :return: 返回包含所有股票信息的字典
        """
        return {
            "ts_code": self.ts_code,
            "symbol": self.symbol,
            "name": self.name,
            "area": self.area,
            "industry": self.industry
        }

    def from_dataframe(df):
        """
        从 DataFrame 创建 StockBasic 实例。
        DataFrame 的列名需要与类的属性名一致。
        :param df: 包含股票基本信息的 DataFrame
        :return: StockBasic 实例
        """
        # 确保 DataFrame 的列名与类的属性名一致
        required_columns = ['ts_code', 'symbol', 'name', 'area', 'industry']
        if not all(column in df.columns for column in required_columns):
            raise ValueError(f"DataFrame 必须包含以下列: {required_columns}")

        # 从 DataFrame 中提取数据并创建实例
        return StockBasic(
            ts_code=df['ts_code'].values[0],
            symbol=df['symbol'].values[0],
            name=df['name'].values[0],
            area=df['area'].values[0],
            industry=df['industry'].values[0]
        )

    def __str__(self):
        """
        定义打印该对象时输出的内容
        """
        info = self.get_info()
        info_str = ', '.join([f"{key}: {value}" for key, value in info.items()])
        return f"StockBasic({info_str})"


# 示例使用
stock_example = StockBasic(ts_code="000001.SZ", symbol="平安银行", name="PingAn Bank", area="深圳", industry="金融")
print(stock_example)
