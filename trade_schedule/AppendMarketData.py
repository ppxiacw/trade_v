import os

import pandas as pd
from config.tushare_utils import ts
from dbconfig import engine
from utils import StockAnalysis

analysis = StockAnalysis()
today = analysis.get_today(replace=True)
# 获取当前脚本的完整路径
current_path = os.path.abspath(__file__)

# 获取当前脚本的目录
dir_path = os.path.dirname(current_path)

# 获取当前脚本的上级目录
parent_dir_path = os.path.dirname(dir_path)

# 构造相对路径
relative_path = os.path.join(parent_dir_path, 'files')

# 初始化一个空的DataFrame用于累积数据

# 读取股票列表
df = pd.read_csv(f'{relative_path}/stock_list_filter.csv', dtype={'symbol': str})

def append_market_data():
    # 遍历所有股票代码
    all_data = pd.DataFrame()
    for _, row in df.iterrows():
        ts_code = row['ts_code']
        print(f":processing {ts_code}")

        # 获取数据
        data = ts.pro_bar(ts_code=ts_code, adj='qfq', start_date=today, end_date=today)

        # 累积非空数据
        if not data.empty:
            all_data = pd.concat([all_data, data], ignore_index=True)

    # 一次性批量插入数据库（如果非空）
    if not all_data.empty:
        all_data.to_sql(
            name='market',
            con=engine,
            if_exists='append',  # 保持追加模式
            index=False,
            chunksize=1000  # 分块写入，避免内存溢出
        )
        print(f"successful {len(all_data)} records")
    else:
        print("insert nothing")

if __name__ == "__main__":
    append_market_data()