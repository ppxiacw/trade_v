import pandas as pd
import bisect
from datetime import datetime
from utils.date_utils import Date_utils


class TradingDayCalculator:
    def __init__(self):
        """
        初始化交易日历计算器

        参数:
            df_calendar (DataFrame): 必须包含'trade_date'和'trade_status'列
        """
        stockAnalysis = Date_utils()
        data1 = stockAnalysis.get_trade_calendar('2024')
        data2 = stockAnalysis.get_trade_calendar('2025')
        data = pd.concat([data1,data2])
        # 数据预处理
        self.df = data.copy()
        self.df['trade_date'] = pd.to_datetime(self.df['trade_date'])

        # 筛选有效交易日
        self.trading_days = self.df[self.df['trade_status'] == "1"]['trade_date']
        self.sorted_days = sorted(self.trading_days.dt.to_pydatetime())

        # 缓存最小最大日期
        self.min_date = self.trading_days.min()
        self.max_date = self.trading_days.max()

    def calculate(self, start_date_str, end_date_str, verbose=True):
        """
        主计算方法

        参数:
            start_date_str: 起始日期字符串
            end_date_str: 结束日期字符串
            verbose: 是否显示警告信息

        返回:
            int: 交易日数量
        """
        # 转换日期
        try:
            if len(start_date_str) ==8:
                start_date_str = f"{start_date_str[:4]}-{start_date_str[4:6]}-{start_date_str[6:8]}"
            if len(end_date_str) ==8:
                end_date_str = f"{end_date_str[:4]}-{end_date_str[4:6]}-{end_date_str[6:8]}"
            start = datetime.strptime(start_date_str, '%Y-%m-%d')
            end = datetime.strptime(end_date_str, '%Y-%m-%d')
        except ValueError:
            if verbose:
                print("错误：日期格式必须为YYYY-MM-DD")
            return 0

        # 日期有效性检查
        if start > end:
            start, end = end, start

        if start < self.min_date or end > self.max_date:
            if verbose:
                print(f"警告：日期需在{self.min_date}到{self.max_date}之间")
            return 0

        # 二分查找计算
        left = bisect.bisect_left(self.sorted_days, start)
        right = bisect.bisect_right(self.sorted_days, end)

        # 边界日期验证
        if verbose:
            if self.sorted_days[left] != start and left < len(self.sorted_days):
                print(f"提示：{start_date_str}不是交易日，下一个交易日是{self.sorted_days[left].strftime('%Y-%m-%d')}")
            if (right == 0 or self.sorted_days[right - 1] != end) and right > 0:
                print(f"提示：{end_date_str}不是交易日，前一个交易日是{self.sorted_days[right - 1].strftime('%Y-%m-%d')}")

        return right - left


calculator = TradingDayCalculator()

# print(calculator.calculate('2025-02-28', '2025-03-14'))  # 输出：3
