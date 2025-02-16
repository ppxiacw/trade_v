from dto.StockDataDay import StockDataDay

from dbconfig import  cursor


# 执行SQL查询
query = "select max(high),ts_code from market group by ts_code;"
cursor.execute(query)

new_high = dict()

# 获取查询结果
results = cursor.fetchall()

for result in results:
    new_high[result[1]] = result[0]
print(1)


class NewHigh:




    def __init__(self):
        pass

    @staticmethod
    def valid(df: StockDataDay):
        if df.close>=new_high[df.ts_code]:
            return True