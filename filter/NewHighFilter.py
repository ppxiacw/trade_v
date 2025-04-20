from dto.StockDataDay import StockDataDay
from config.tushare_utils import IndexAnalysis
from config.dbconfig import exeQuery
from config.Value import testDate


# 执行SQL查询
# query = f'SELECT DISTINCT max(high) as high, ts_code FROM market WHERE trade_date >= DATE_SUB("2025-04-09", INTERVAL 10 DAY) GROUP BY ts_code;'
query = f'SELECT DISTINCT max(high) as high, ts_code FROM market WHERE trade_date >= DATE_SUB({testDate}, INTERVAL 10 DAY) GROUP BY ts_code;'

results=exeQuery(query)

new_high = dict()
# 获取查询结果
for item in results:
    new_high[item['ts_code']] = item["high"]


class NewHighFilter:


        @staticmethod
        def valid(df: StockDataDay):
            # 获取当前股票的 ts_code
            ts_code = df.ts_code  # 假设 StockDataDay 有 ts_code 属性

            # 检查该股票是否在 new_high 字典中存在记录
            if ts_code not in new_high:
                return False  # 若无历史最高价数据，直接返回 False

            # 获取该股票过去10日的最高价
            historical_high = new_high[ts_code]

            # 计算阈值：最高价下跌7%后的价格
            threshold = historical_high * 0.93

            # 判断当前收盘价是否低于或等于阈值
            return df.close <= threshold

if __name__ == "__main__":

    print(NewHighFilter.valid(IndexAnalysis.get_stock_daily('000001.SZ','20250408')[0]))
