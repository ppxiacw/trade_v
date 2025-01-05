from dbconfig import connection
import adata
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from contextlib import contextmanager
from mysql.connector import pooling, Error

# 设置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 创建一个连接池
dbconfig = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "123456",
    "database": "trade"
}
connection_pool = pooling.MySQLConnectionPool(pool_name="mypool", pool_size=10, **dbconfig)


@contextmanager
def get_db_cursor():
    conn = None
    try:
        conn = connection_pool.get_connection()
        cursor = conn.cursor(dictionary=True)
        yield cursor
        conn.commit()
    except Error as e:
        logging.error(f"Database error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()


def fetch_monthly_change(stock_code):
    yearly_changes = {}

    try:
        with get_db_cursor() as cursor:
            for year in range(2015, 2025):  # 注意年份范围是 [2015, 2024]
                query = """
                    SELECT DATE_FORMAT(trade_date, '%Y-%m') AS month,
                           SUM(change_pct) AS total_change_pct
                    FROM market_{year}
                    WHERE stock_code = %s
                    GROUP BY month
                    ORDER BY month
                """.format(year=year)
                params = (stock_code,)
                cursor.execute(query, params)
                results = cursor.fetchall()

                yearly_changes[str(year)] = {row['month']: row['total_change_pct'] for row in
                                             results} if results else {}
                logging.info(
                    f'stock_code:{stock_code}, year:{year}, fetched months: {[row["month"] for row in results]}')

    except Exception as e:
        logging.error(f"Error fetching data for stock_code {stock_code}: {e}")
        return None

    return stock_code, yearly_changes


# 获取所有股票代码并转换为字符串类型
df = adata.stock.info.all_code()
df['stock_code'] = df['stock_code'].astype(str)

# 初始化字典来存储结果
change_dict = {}

# 使用线程池并发执行
with ThreadPoolExecutor(max_workers=10) as executor:  # 减少线程数以降低对数据库的压力
    futures = {executor.submit(fetch_monthly_change, v['stock_code']): v['stock_code'] for i, v in df.iterrows()}
    for future in as_completed(futures):
        try:
            result = future.result()
            if result:
                stock_code, yearly_changes = result
                change_dict[stock_code] = yearly_changes
        except Exception as e:
            logging.error(f"Error processing future for stock_code: {e}")

file_path = 'stock_change.json'

# 使用 with 语句来管理文件对象，这样可以在不需要的时候自动关闭文件
try:
    with open(file_path, 'w', encoding='utf-8') as json_file:
        # 将字典序列化为 JSON 并写入文件
        json.dump(change_dict, json_file, ensure_ascii=False, indent=4)
    logging.info(f"Data has been written to {file_path}")
except Exception as e:
    logging.error(f"Failed to write to file {file_path}: {e}")