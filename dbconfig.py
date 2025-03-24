from sqlalchemy import create_engine
import mysql.connector  # MySQL连接信息

db_config = {
    "user": "root",
    "password": "123456",
    "host": "127.0.0.1",  # 或者你的服务器IP地址
    "database": "trade",

}

# 创建MySQL连接引擎
engine = create_engine('mysql+mysqlconnector://{user}:{password}@{host}/{database}'.format(
    **db_config))

connection = mysql.connector.connect(
    user='root',
    password='123456',
    host='127.0.0.1',  # 或者你的服务器IP地址
    database='trade'
)

# 创建游标对象
cursor = connection.cursor(dictionary=True)  # 添加 dictionary=True 参数



from mysql.connector import pooling

# 全局连接池配置
db_pool = pooling.MySQLConnectionPool(
    pool_name="flask_pool",
    pool_size=10,
    host='127.0.0.1',
    user='root',
    password='123456',
    database='trade'
)


