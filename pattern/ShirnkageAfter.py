from dto.StockDataDay import StockDataDay
from filter.ContinuousRedFilter import ContinuousRedFilter
from filter.FluctuationRangeFilter import FluctuationRangeFilter
from filter.IntervalRangeFilter import IntervalRangeFilter
from filter.TurnoverRateFilter import TurnoverRateFilter

from utils import StockAnalysis
from config.dbconfig import db_pool
from typing import Tuple, Optional
from pattern.ShrinkageByDate import ShrinkageByDate
conn = db_pool.get_connection()
cursor = conn.cursor()

analysis = StockAnalysis()

yesterday = analysis.get_date_by_step(analysis.get_today(), -1)

# 执行SQL查询
query = """
WITH ranked_trades AS (
    SELECT
        ts_code,
        trade_date,
        vol,
        close,
        open,
        ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
    FROM trade.market
),
latest_10_days AS (
    SELECT
        ts_code,
        trade_date,
        vol,
       close,
        open
    FROM ranked_trades
    WHERE rn <= 30
),
min_vol_per_stock AS (
    SELECT
        ts_code,
        MIN(vol) AS min_vol
    FROM latest_10_days
    GROUP BY ts_code
),
today_trades AS (
    SELECT
        ts_code,
        vol,
        trade_date,
        close,
        open
    FROM ranked_trades
    WHERE rn = 1
)
SELECT t.ts_code,t.vol 
FROM today_trades t
JOIN min_vol_per_stock m ON t.ts_code = m.ts_code
WHERE t.vol = m.min_vol
  and t.close>t.open
AND t.trade_date = (SELECT MAX(trade_date) FROM trade.market);
"""
cursor.execute(query)

shrinkage_dict = dict()

# 获取查询结果
results = cursor.fetchall()

for result in results:
    shrinkage_dict[result[0]] = result[1]

redSet = {item[0] for item in results}


class ShirnkageAfter:

    def __init__(self):
        pass

    @staticmethod
    def valid(df: StockDataDay) -> Tuple[Optional[float], Optional[int]]:
        # 过滤条件检查
        if not ContinuousRedFilter.valid(df):
            print(f'{df.ts_code}被三红过滤')
            return None, None  # 保持原有逻辑

        if not FluctuationRangeFilter.valid(df):
            print(f'{df.ts_code}被涨跌幅度2以上过滤')
            return None, None

        if not IntervalRangeFilter.valid(df):
            print(f'{df.ts_code}被十天波动过滤')
            return None, None
        if not TurnoverRateFilter.valid(df):
            print(f'{df.ts_code}被换手率要求过滤')
            return None, None

        # 计算逻辑
        vol = df.vol / 100
        if vol == 0:
            return None, None

        if df.ts_code in redSet:
            shrinkage_value = shrinkage_dict.get(df.ts_code, 0)
            if vol < shrinkage_value and df.close > df.open:
                day = ShrinkageByDate.find_distance(df)
                return shrinkage_value / vol, day

        # 默认返回（必须添加）
        return None, None



# print(ShirnkageAfter.valid(IndexAnalysis.get_stock_daily('000002.SZ', '20250313')[0]))
