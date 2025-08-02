from config.dbconfig import db_pool
from dto.StockDataDay import StockDataDay
from utils.tushare_utils import IndexAnalysis
from collections import defaultdict
from utils.TradingDayCalculator import calculator

from utils.date_utils import stockAnalysis
class ShrinkageByDate:
    # 类属性存储所有股票的历史数据
    _stock_data = None

    @classmethod
    def _load_data(cls):
        """静态方法加载数据库数据到内存"""
        if cls._stock_data is not None:
            return

        conn = db_pool.get_connection()
        try:
            with conn.cursor(dictionary=True) as cursor:
                # 执行SQL查询
                sql = """
                SELECT * 
                FROM trade.market
                ORDER BY ts_code, trade_date desc 
                """
                cursor.execute(sql)
                results = cursor.fetchall()

                # 构建数据字典
                cls._stock_data = defaultdict(list)
                for row in results:
                    code = row['ts_code']
                    cls._stock_data[code].append({
                        'trade_date': row['trade_date'].strftime('%Y-%m-%d'),
                        'open': row['open'],
                        'close': row['close'],
                        'high': row['high'],
                        'low': row['low'],
                        'vol': row['vol'],
                        'amount': row['amount'],
                        'pct_chg': row['pct_chg'],
                        'change': row['change'],
                        'turnover_ratio': row['turnover_rate'],
                        'pre_close': row['pre_close']
                    })
        finally:
            conn.close()

    @staticmethod
    def find_distance(df: StockDataDay):
        """静态方法实现查找逻辑"""
        ShrinkageByDate._load_data()

        # 获取对应股票的历史数据
        sorted_data = ShrinkageByDate._stock_data.get(df.ts_code, [])
        if df.trade_date is None:
            df.trade_date = stockAnalysis.get_today()
        # 格式化目标日期（原日期格式如 20230201）
        target_date = f"{df.trade_date[:4]}-{df.trade_date[4:6]}-{df.trade_date[6:8]}"

        # 遍历历史数据查找符合条件的日期
        for item in sorted_data:
            # 跳过目标日期当天
            if item['trade_date'] == target_date:
                continue

            # 找到第一个成交量小于目标值的日期
            if item['vol'] < df.vol and item['trade_date'] != df.trade_date:
                print(f"找到符合条件的日期：{item['trade_date']}")
                return calculator.calculate(df.trade_date,item['trade_date'])
        return 10000


# 使用示例
if __name__ == "__main__":
    stock_data = IndexAnalysis.get_stock_daily('605133.SH', '2025-03-14')[0]
    print(ShrinkageByDate.find_distance(stock_data))