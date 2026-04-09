import logging
from mysql.connector import pooling
from sqlalchemy import create_engine
from config.runtime_config import get_db_runtime_settings, get_db_connection_uri

logger = logging.getLogger(__name__)

_DB_SETTINGS = get_db_runtime_settings()
_POOL_NAME = f"{_DB_SETTINGS['pool_name']}_legacy"

# 全局连接池配置（添加 autoreconnect 和超时参数）
db_pool = pooling.MySQLConnectionPool(
    pool_name=_POOL_NAME,
    pool_size=_DB_SETTINGS['pool_size'],
    host=_DB_SETTINGS['host'],
    user=_DB_SETTINGS['user'],
    password=_DB_SETTINGS['password'],
    database=_DB_SETTINGS['database'],
    autocommit=True,  # 自动提交事务
    pool_reset_session=True,  # 允许重置会话
    connect_timeout=_DB_SETTINGS['connect_timeout'],  # 连接超时时间（秒）
)

db_config = {
    "user": _DB_SETTINGS['user'],
    "password": _DB_SETTINGS['password'],
    "host": _DB_SETTINGS['host'],
    "database": _DB_SETTINGS['database'],
}

# 创建MySQL连接引擎
engine = create_engine(get_db_connection_uri(db_config), pool_pre_ping=True)


def exeQuery(query, params=None):
    conn = None
    cursor = None
    try:
        # 从连接池获取连接（设置获取连接重试）
        conn = db_pool.get_connection()
        if not conn.is_connected():
            conn.reconnect(attempts=3, delay=1)

        cursor = conn.cursor(dictionary=True)
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        result = cursor.fetchall() or []
        return result
    except Exception:
        logger.exception("Query Error: %s", query)
        return []
    finally:
        # 安全释放资源
        try:
            if cursor:
                cursor.close()
        except Exception as e:
            logger.warning("Cursor close error: %s", e)

        try:
            if conn:
                conn.close()  # 归还连接，忽略内部 reset_session 错误
        except Exception as e:
            logger.warning("Connection close error: %s (连接可能已归还池)", e)