from dto.StockDataDay import StockDataDay
from utils import StockAnalysis
from dbconfig import  cursor

analysis = StockAnalysis()

yesterday = analysis.get_date_by_step(analysis.get_today(),-1)

# 执行SQL查询
query = f'select max(high),ts_code from market where trade_date <= "{yesterday}" group by ts_code ;'
cursor.execute(query)

new_high = dict()

# 获取查询结果
results = cursor.fetchall()

for result in results:
    new_high[result[1]] = result[0]


class NewHigh:




    def __init__(self):
        pass

    @staticmethod
    def valid(df: StockDataDay):
        print(f'{df.ts_code},{df.close},{new_high[df.ts_code]}')
        if df.close>=new_high[df.ts_code]:
            return True