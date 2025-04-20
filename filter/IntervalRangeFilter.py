from dto.StockDataDay import StockDataDay
from config.tushare_utils import IndexAnalysis
from config.dbconfig import db_pool

conn = db_pool.get_connection()

cursor = conn.cursor()

# 执行SQL查询
query = """
WITH recent_dates AS (
    -- 获取全市场最近20个交易日
    SELECT DISTINCT trade_date
    FROM trade.market
    ORDER BY trade_date DESC
    LIMIT 10
),
stock_range AS (
    -- 计算每个股票的波动百分比
    SELECT
        m.ts_code,
        MAX(m.high) AS period_high,
        MIN(m.low) AS period_low,
        -- 波动百分比计算：(最高价-最低价)/最低价*100
        (MAX(m.high) - MIN(m.low)) / MIN(m.low) * 100 AS pct_range
    FROM trade.market m
    INNER JOIN recent_dates rd
        ON m.trade_date = rd.trade_date
    GROUP BY m.ts_code
    HAVING
        -- 确保20天数据完整
        COUNT(DISTINCT m.trade_date) = 10
        -- 防止除零错误（最低价>0）
        AND MIN(m.low) > 0
)
SELECT
    ts_code AS 股票代码,
    period_high AS 期间最高价,
    period_low AS 期间最低价,
    ROUND(pct_range, 2) AS 波动百分比
FROM stock_range
WHERE pct_range <= 5; 
"""
cursor.execute(query)

new_high = dict()

# 获取查询结果
results = cursor.fetchall()
redSet = {item[0] for item in results}


class IntervalRangeFilter:

    def __init__(self):
        pass

    @staticmethod
    def valid(df: StockDataDay):
        # if abs((df.close-df.pre_close)/df.pre_close)<0.005:
        #     return False
        if df.ts_code in redSet:
            return False
        else:
            return True


if __name__ == "__main__":

    print(IntervalRangeFilter.valid(IndexAnalysis.get_stock_daily('000001.SZ','20250307')[0]))
