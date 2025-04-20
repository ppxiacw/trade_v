from config.dbconfig import connection
# 创建连接


def select():
    # 创建游标对象
    cursor = connection.cursor()

    # 执行查询
    query = ("select adddate(max(trade_date),1) from market")
    cursor.execute(query)
    result = cursor.fetchone()
    latest_trade_date = result[0]
    print(f"最新的交易日期是: {latest_trade_date}")
    return latest_trade_date

