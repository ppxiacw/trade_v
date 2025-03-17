from dto.StockDataDay import StockDataDay
from utils import StockAnalysis
from dbconfig import  db_pool

conn = db_pool.get_connection()
cursor = conn.cursor()

analysis = StockAnalysis()

yesterday = analysis.get_date_by_step(analysis.get_today(),-1)

# 执行SQL查询
query = f'select vol,ts_code from market where trade_date = "{yesterday}" and close-market.open<0;'
cursor.execute(query)

shrinkage_dict = dict()

# 获取查询结果
results = cursor.fetchall()

for result in results:
    shrinkage_dict[result[1]] = result[0]


class FindShrinkage:

    def __init__(self):
        pass

    @staticmethod
    def valid(df: StockDataDay):
        print(df.ts_code)
        vol = df.vol
        if vol == 0:
           return None
        if vol<shrinkage_dict.get(df.ts_code,0) and df.close > df.open:
            return shrinkage_dict[df.ts_code]/vol