
from dto.StockDataDay import StockDataDay
from config.dbconfig import db_pool
from config.tushare_utils import IndexAnalysis

conn = db_pool.get_connection()
cursor = conn.cursor(dictionary=True)



# 执行SQL查询
query = """
WITH ranked_dates AS (
    SELECT
        trade_date,
        DENSE_RANK() OVER (ORDER BY trade_date DESC) AS date_rank
    FROM market
    WHERE trade_date <= CURRENT_DATE
    GROUP BY trade_date
)
SELECT DISTINCT m.ts_code
FROM market m
JOIN ranked_dates r ON m.trade_date = r.trade_date
WHERE r.date_rank <= 3
AND m.turnover_rate > 2;
"""
cursor.execute(query)

new_high = dict()

# 获取查询结果
results = cursor.fetchall()
redSet = {item['ts_code'] for item in results}



class TurnoverRateFilter:

    def __init__(self):
        pass

    @staticmethod
    def valid(df: StockDataDay):
        if df.ts_code in redSet:
            return True
        else:
            return False

if __name__ == "__main__":

    print(TurnoverRateFilter.valid(IndexAnalysis.get_stock_daily('000004.SZ','20250326')[0]))