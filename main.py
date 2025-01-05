import pandas as pd
import adata  # 假设这是你用来获取股票数据的库
from sqlalchemy import create_engine

# MySQL连接信息
db_config = {
    "user": "root",
    "password": "123456",
    "host": "127.0.0.1",  # 或者你的服务器IP地址
    "database": "trade"
}

# 创建MySQL连接引擎
engine = create_engine('mysql+mysqlconnector://{user}:{password}@{host}/{database}'.format(
    **db_config))

# 获取所有股票代码
df = adata.stock.info.all_code()
df['stock_code'] = df['stock_code'].astype(str)

# 定义表名和年份范围
table_name = 'market_data'  # 替换为你的表名
start_year = 2015
end_year = 2023

def insert_in_batches(df, table_name, engine, batch_size=1000):
    """将DataFrame分批插入数据库"""
    total_rows = len(df)
    for start in range(0, total_rows, batch_size):
        end = min(start + batch_size, total_rows)
        print(f"Inserting rows {start} to {end - 1}...")
        df[start:end].to_sql(name=table_name, con=engine, if_exists='append', index=False)
    print("All data inserted successfully.")


# 循环遍历年份，获取并保存每年的数据
for year in range(start_year, end_year + 1):
    print(f"Processing data for the year {year}...")
    table_name = f'market_{year}'
    all_data = []  # 每次循环时重置 all_data 列表

    for i, v in df.iterrows():
        print(f"{i},{year}")

        # 获取特定股票的历史市场数据
        value = adata.stock.market.get_market(
            stock_code=v['stock_code'],
            start_date=f'{year}-01-01',
            end_date=f'{year}-12-31'
        )

        if not value.empty:  # 检查返回的 DataFrame 是否为空
            all_data.append(value)

    # 合并所有数据到一个 DataFrame 中
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        # 将合并后的数据分批写入MySQL表
        insert_in_batches(final_df, table_name, engine, batch_size=1000)
        # 将合并后的数据写入MySQL表
        print(f"Data for year {year} successfully imported into {table_name}")
    else:
        print(f"No data available for year {year}")

print("All data processing completed.")