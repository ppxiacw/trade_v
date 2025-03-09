from dto.StockDataDay import StockDataDay
from dbconfig import db_pool

conn = db_pool.get_connection()
cursor = conn.cursor()

# 执行SQL查询
query = """
WITH recent_trades AS (
    SELECT 
        ts_code,
        open,
        close,
        ROW_NUMBER() OVER (
            PARTITION BY ts_code 
            ORDER BY trade_date DESC
        ) AS rn
    FROM trade.market
)
SELECT ts_code
FROM recent_trades
WHERE rn <= 3
GROUP BY ts_code
HAVING COUNT(*) = 3 
    AND SUM(close > open) = 3;
"""
cursor.execute(query)

new_high = dict()

# 获取查询结果
results = cursor.fetchall()
redSet = {item[0] for item in results}


class ContinuousRedFilter:

    def __init__(self):
        pass

    @staticmethod
    def valid(df: StockDataDay):
        if df.ts_code in redSet:
            return False
        else:
            return True

# print(Filter.valid(IndexAnalysis.get_stock_daily('000045','20250307')[0]))
