import mysql.connector
from mysql.connector import pooling, Error
from sqlalchemy import create_engine, text
from contextlib import contextmanager
import logging
from typing import List, Dict, Any, Optional, Union

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseManager:
    """数据库管理类"""

    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self._init_pool()
        self._init_engine()

    def _init_pool(self):
        """初始化连接池"""
        try:
            self.db_pool = pooling.MySQLConnectionPool(
                pool_name="flask_pool",
                pool_size=10,
                host=self.db_config["host"],
                user=self.db_config["user"],
                password=self.db_config["password"],
                database=self.db_config["database"],
                autocommit=True,
                pool_reset_session=True,
                connect_timeout=30,
                charset='utf8mb4',
                collation='utf8mb4_unicode_ci'
            )
            logger.info("数据库连接池初始化成功")
        except Error as e:
            logger.error(f"数据库连接池初始化失败: {e}")
            raise

    def _init_engine(self):
        """初始化SQLAlchemy引擎"""
        try:
            connection_string = f"mysql+mysqlconnector://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}/{self.db_config['database']}"
            self.engine = create_engine(
                connection_string,
                pool_pre_ping=True,  # 执行前ping检测连接有效性
                echo=False  # 设为True可查看SQL日志
            )
            logger.info("SQLAlchemy引擎初始化成功")
        except Exception as e:
            logger.error(f"SQLAlchemy引擎初始化失败: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """获取数据库连接的上下文管理器"""
        conn = None
        try:
            conn = self.db_pool.get_connection()
            if not conn.is_connected():
                conn.reconnect(attempts=3, delay=1)
            yield conn
        except Error as e:
            logger.error(f"获取数据库连接失败: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """执行查询语句

        Args:
            query: SQL查询语句
            params: 参数元组

        Returns:
            查询结果列表
        """
        conn = None
        cursor = None
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                result = cursor.fetchall()
                logger.debug(f"查询执行成功，返回 {len(result)} 条记录")
                return result
        except Error as e:
            logger.error(f"查询执行失败: {e}, SQL: {query}")
            return []
        finally:
            if cursor:
                cursor.close()

    def execute_insert(self, table: str, data: Dict[str, Any]) -> Optional[int]:
        """执行插入操作

        Args:
            table: 表名
            data: 插入的数据字典

        Returns:
            插入的主键ID
        """
        if not data:
            logger.warning("插入数据为空")
            return None

        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        params = tuple(data.values())

        conn = None
        cursor = None
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                inserted_id = cursor.lastrowid
                logger.debug(f"插入成功，表: {table}, ID: {inserted_id}")
                return inserted_id
        except Error as e:
            logger.error(f"插入失败: {e}, SQL: {query}")
            return None
        finally:
            if cursor:
                cursor.close()

    def execute_update(self, table: str, data: Dict[str, Any], where: str, where_params: Optional[tuple] = None) -> int:
        """执行更新操作

        Args:
            table: 表名
            data: 更新的数据字典
            where: WHERE条件
            where_params: WHERE条件参数

        Returns:
            受影响的行数
        """
        if not data:
            logger.warning("更新数据为空")
            return 0

        set_clause = ', '.join([f"{key} = %s" for key in data.keys()])
        query = f"UPDATE {table} SET {set_clause} WHERE {where}"
        params = tuple(data.values())
        if where_params:
            params += where_params

        conn = None
        cursor = None
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                affected_rows = cursor.rowcount
                logger.debug(f"更新成功，表: {table}, 影响行数: {affected_rows}")
                return affected_rows
        except Error as e:
            logger.error(f"更新失败: {e}, SQL: {query}")
            return 0
        finally:
            if cursor:
                cursor.close()

    def execute_delete(self, table: str, where: str, params: Optional[tuple] = None) -> int:
        """执行删除操作

        Args:
            table: 表名
            where: WHERE条件
            params: 参数元组

        Returns:
            受影响的行数
        """
        query = f"DELETE FROM {table} WHERE {where}"

        conn = None
        cursor = None
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                affected_rows = cursor.rowcount
                logger.debug(f"删除成功，表: {table}, 影响行数: {affected_rows}")
                return affected_rows
        except Error as e:
            logger.error(f"删除失败: {e}, SQL: {query}")
            return 0
        finally:
            if cursor:
                cursor.close()

    def execute_many(self, query: str, data: List[tuple]) -> int:
        """批量执行操作

        Args:
            query: SQL语句
            data: 参数列表

        Returns:
            受影响的总行数
        """
        conn = None
        cursor = None
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.executemany(query, data)
                affected_rows = cursor.rowcount
                logger.debug(f"批量操作成功，影响行数: {affected_rows}")
                return affected_rows
        except Error as e:
            logger.error(f"批量操作失败: {e}, SQL: {query}")
            return 0
        finally:
            if cursor:
                cursor.close()

    def execute_transaction(self, operations: List[tuple]) -> bool:
        """执行事务操作

        Args:
            operations: 操作列表，每个元素为 (query, params) 或 (query,)

        Returns:
            是否成功
        """
        conn = None
        cursor = None
        try:
            with self.get_connection() as conn:
                conn.autocommit = False  # 关闭自动提交
                cursor = conn.cursor()

                for operation in operations:
                    if len(operation) == 1:
                        cursor.execute(operation[0])
                    else:
                        cursor.execute(operation[0], operation[1])

                conn.commit()
                logger.info("事务执行成功")
                return True

        except Error as e:
            if conn:
                conn.rollback()
            logger.error(f"事务执行失败: {e}")
            return False
        finally:
            if conn:
                conn.autocommit = True  # 恢复自动提交
            if cursor:
                cursor.close()


# 数据库配置
db_config = {
    "user": "trade",
    "password": "trade007576!",
    "host": "212.64.32.213",
    "database": "trade",
}

# 创建全局数据库管理器实例
db_manager = DatabaseManager(db_config)


# 兼容旧代码的快捷函数
def exe_query(query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """执行查询（兼容旧代码）"""
    return db_manager.execute_query(query, params)


# 股票告警日志相关的专用方法
class StockAlertDAO:
    """股票告警数据访问对象"""

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    def insert_alert(self, alert_data: Dict[str, Any]) -> Optional[int]:
        """插入股票告警记录"""
        return self.db.execute_insert('stock_alert_log', alert_data)

    def get_alerts_by_stock(self, stock_code: str, limit: int = 100) -> List[Dict[str, Any]]:
        """根据股票代码查询告警记录"""
        query = """
            SELECT * FROM stock_alert_log 
            WHERE stock_code = %s 
            ORDER BY trigger_time DESC 
            LIMIT %s
        """
        return self.db.execute_query(query, (stock_code, limit))

    def get_unprocessed_alerts(self, alert_level: Optional[int] = None) -> List[Dict[str, Any]]:
        """查询未处理的告警记录"""
        if alert_level:
            query = "SELECT * FROM stock_alert_log WHERE alert_level = %s ORDER BY trigger_time DESC"
            return self.db.execute_query(query, (alert_level,))
        else:
            query = "SELECT * FROM stock_alert_log  ORDER BY trigger_time DESC"
            return self.db.execute_query(query)



    def get_alerts_by_time_range(self, start_time: str, end_time: str) -> List[Dict[str, Any]]:
        """根据时间范围查询告警记录"""
        query = """
            SELECT * FROM stock_alert_log 
            WHERE trigger_time BETWEEN %s AND %s 
            ORDER BY trigger_time DESC
        """
        return self.db.execute_query(query, (start_time, end_time))


# 创建股票告警DAO实例
stock_alert_dao = StockAlertDAO(db_manager)

# 使用示例
if __name__ == "__main__":
    # 插入示例
    alert_data = {
        'stock_code': '000001',
        'stock_name': '平安银行',
        'alert_type': '价格突破',
        'alert_level': 2,
        'alert_message': '股价突破阻力位',
        'trigger_time': '2024-01-01 10:00:00'
    }

    # 插入记录
    alert_id = stock_alert_dao.insert_alert(alert_data)
    print(f"插入的告警ID: {alert_id}")

    # 查询记录
    alerts = stock_alert_dao.get_alerts_by_stock('000001')
    print(f"查询到 {len(alerts)} 条记录")

