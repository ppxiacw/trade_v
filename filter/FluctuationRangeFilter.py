
from dto.StockDataDay import StockDataDay
from dbconfig import  cursor
from config.tushare_utils import  IndexAnalysis


# 执行SQL查询
query = """
WITH latest_dates AS (
    -- 获取全市场最近20个交易日
    SELECT DISTINCT trade_date
    FROM trade.market
    ORDER BY trade_date DESC
    LIMIT 20
),
stock_trades AS (
    -- 获取各股票在这10天的数据
    SELECT
        m.ts_code,
        m.trade_date,
        m.pct_chg
    FROM trade.market m
    INNER JOIN latest_dates ld
        ON m.trade_date = ld.trade_date
),
qualified_stocks AS (
    SELECT
        ts_code,
        -- 检查每日涨跌幅是否合规
        SUM(CASE WHEN ABS(pct_chg) > 2 THEN 1 ELSE 0 END) AS violation_days,
        -- 检查是否有20天完整数据
        COUNT(*) AS total_days
    FROM stock_trades
    GROUP BY ts_code
)
SELECT ts_code
FROM qualified_stocks
WHERE
    violation_days = 0  -- 没有违规天数
    AND total_days = 20; --
"""
cursor.execute(query)

new_high = dict()

# 获取查询结果
results = cursor.fetchall()
redSet = {item[0] for item in results}



class FluctuationRangeFilter:

    def __init__(self):
        pass

    @staticmethod
    def valid(df: StockDataDay):
        if df.ts_code in redSet:
            return False
        else:
            return True


# print(FluctuationRangeFilter.valid(IndexAnalysis.get_stock_daily('000001.SZ','20250307')[0]))