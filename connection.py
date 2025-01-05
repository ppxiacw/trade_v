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
    query = ("select adddate(max(trade_date),1) from market_2024")
    cursor.execute(query)
    result = cursor.fetchone()
    latest_trade_date = result[0]
    print(f"最新的交易日期是: {latest_trade_date}")
    return latest_trade_date


def insert():
    try:
        if connection.is_connected():
            cursor = connection.cursor()

            # SQL 插入语句
            sql_insert_query = """INSERT INTO employees (first_name, last_name, position, hire_date) 
                                  VALUES (%s, %s, %s, %s)"""

            # 要插入的数据
            insert_tuple = ('John', 'Doe', 'Developer', '2023-10-01')

            # 使用游标执行SQL语句
            result = cursor.execute(sql_insert_query, insert_tuple)
            connection.commit()  # 提交更改
            print("Record inserted successfully into employees table")

    except Exception as e:
        print("Failed to insert record into MySQL table {}".format(e))

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")