import os

import pandas as pd
from utils.tushare_utils import ts
from config.dbconfig import engine


from utils.date_utils import Date_utils
token  = '410070664c78124d98ca5e81c3921530bd27534856b174c702d698a5'
ts.set_token(token)
pro = ts.pro_api(token)
analysis = Date_utils()
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
        data = ts.pro_bar(ts_code=ts_code, adj='qfq', start_date='20250501', end_date=today, factors=['tor'])

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


def append_market_mins_data(ts_code):
    df = pro.stk_mins(ts_code=ts_code, freq='1min', start_date='2025-06-25 09:00:00',
                      end_date='2025-07-28 19:00:00')
    print(df)
    df = pro.stk_mins(ts_code=ts_code, freq='5min', start_date='2025-06-25 09:00:00',
                      end_date='2025-07-28 19:00:00')
    print(df)
    # if not df.empty:
    #     df.to_sql(
    #         name='market_mins',
    #         con=engine,
    #         if_exists='append',  # 保持追加模式
    #         index=False,
    #         chunksize=1000  # 分块写入，避免内存溢出
    #     )
    #     print(f"successful {len(df)} records")
    # else:
    #     print("insert nothing")
if __name__ == "__main__":
    append_market_mins_data('000001.SH')