from dto.StockDataDay import StockDataDay
from config.dbconfig import  exeQuery
from config.Value import testDate
from config.tushare_utils import IndexAnalysis

# 执行SQL查询
query = f"""
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
    where trade_date <= {testDate}
)
SELECT ts_code
FROM recent_trades
WHERE rn <= 3
GROUP BY ts_code
HAVING COUNT(*) = 3 
    AND SUM(close > open) = 3;
"""
results = exeQuery(query)

new_high = dict()

# 获取查询结果
redSet = {item['ts_code'] for item in results}


class ContinuousRedFilter:

    def __init__(self):
        pass

    @staticmethod
    def valid(df: StockDataDay):
        if df.ts_code in redSet:
            return False
        else:
            return True


if __name__ == "__main__":

    print(ContinuousRedFilter.valid(IndexAnalysis.get_stock_daily('000045','20250307')[0]))
