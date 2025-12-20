import mysql
from mysql.connector import pooling
from sqlalchemy import create_engine

# 全局连接池配置（添加 autoreconnect 和超时参数）
db_pool = pooling.MySQLConnectionPool(
    pool_name="flask_pool",
    pool_size=10,
    host='212.64.32.213',
    user='trade',
    password='trade007576!',
    database='trade',
    autocommit=True,  # 自动提交事务
    pool_reset_session=True,  # 允许重置会话
    connect_timeout=30  # 连接超时时间（秒）
)

db_config = {
    "user": "trade",
    "password": "trade007576!",
    "host":'212.64.32.213',  # 或者你的服务器IP地址
    "database": "trade",

}

# 创建MySQL连接引擎
engine = create_engine('mysql+mysqlconnector://{user}:{password}@{host}/{database}'.format(
    **db_config))


def exeQuery(query):
    conn = None
    cursor = None
    try:
        # 从连接池获取连接（设置获取连接重试）
        conn = db_pool.get_connection()
        if not conn.is_connected():
            conn.reconnect(attempts=3, delay=1)

        cursor = conn.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Exception as e:
        print(f"Query Error: {e}")
        return None
    finally:
        # 安全释放资源
        try:
            if cursor:
                cursor.close()
        except Exception as e:
            print(f"Cursor close error: {e}")

        try:
            if conn:
                conn.close()  # 归还连接，忽略内部 reset_session 错误
        except Exception as e:
            print(f"Connection close error: {e} (但连接已归还池)")