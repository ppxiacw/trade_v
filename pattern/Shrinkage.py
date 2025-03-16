from dbconfig import db_pool
from dto.StockDataDay import StockDataDay
from config.tushare_utils import IndexAnalysis

conn = db_pool.get_connection()
from collections import defaultdict


try:
    with conn.cursor(dictionary=True) as cursor:
        # 2. 执行SQL查询
        sql = """
        SELECT * 
        FROM trade.market
        ORDER BY ts_code, trade_date desc 
        """
        cursor.execute(sql)

        # 3. 获取所有结果
        results = cursor.fetchall()

        # 4. 创建字典结构
        stock_dict = defaultdict(list)

        # 5. 填充字典
        for row in results:
            code = row['ts_code']
            stock_dict[code].append({
                'trade_time': row['trade_time'],
                'trade_date': row['trade_date'].strftime('%Y-%m-%d'),  # 转换日期格式
                'open': row['open'],
                'close': row['close'],
                'high': row['high'],
                'low': row['low'],
                'vol': row['vol'],
                'amount': row['amount'],
                'pct_chg': row['pct_chg'],
                'change': row['change'],
                'turnover_ratio': row['turnover_ratio'],
                'pre_close': row['pre_close']
            })

finally:
    conn.close()



def findDistance(df:StockDataDay):
    sorted_data = stock_dict[df.ts_code]
    for idx, item in enumerate(sorted_data):
        if df.trade_date == item['trade_date'].replace('-',''):
            continue
        if item['vol'] < df.vol:
            print(item['trade_date'])
            break
    print(-1)


findDistance(IndexAnalysis.get_stock_daily('605133.SH', '20250205')[0])