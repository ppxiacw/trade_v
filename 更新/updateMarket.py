import datetime

import pandas as pd
import numpy as np
import adata
import connection
# import update
now = datetime.datetime.now()
# 格式化日期为 yyyy-mm-dd
today = now.strftime('%Y-%m-%d')
df = adata.stock.info.all_code()
df['stock_code'] = df['stock_code'].astype(str)

# 假设 df 是你的股票代码列表 DataFrame
all_data = []  # 创建一个空列表用于存储所有结果
start_date = connection.select()
for i, v in df.iterrows():
    print(i)
    # 获取特定股票的历史市场数据
    value = adata.stock.market.get_market(
        stock_code=v['stock_code'],
        # start_date= str(start_date),
        start_date= str(start_date),
        end_date= today
    )

    if not value.empty:  # 检查返回的 DataFrame 是否为空
        all_data.append(value)
    else:
        print('===')

# 合并所有数据到一个 DataFrame 中
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
else:
    final_df = pd.DataFrame()


from sqlalchemy import create_engine

# MySQL连接信息
db_config = {
        "user":"root",
        "password":"123456",
        "host":"127.0.0.1",  # 或者你的服务器IP地址
        "database":"trade"
}

# 创建MySQL连接引擎
engine = create_engine('mysql+mysqlconnector://{user}:{password}@{host}/{database}'.format(
    **db_config))



# 将DataFrame中的数据写入MySQL表
table_name = 'market_2025'  # 替换为你的表名
# final_df = final_df.drop(columns=['Unnamed: 0'])

final_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

print(f"Data successfully imported into {table_name}")