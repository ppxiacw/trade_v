import mysql.connector

# 创建连接
connection = mysql.connector.connect(
    user='root',
    password='123456',
    host='127.0.0.1',  # 或者你的服务器IP地址
    database='trade'
)

def select():
    # 创建游标对象
    cursor = connection.cursor()

    # 执行查询
    query = ("select adddate(max(trade_date),1) from market_2025")
    cursor.execute(query)
    result = cursor.fetchone()
    latest_trade_date = result[0]
    print(f"最新的交易日期是: {latest_trade_date}")
    return latest_trade_date

