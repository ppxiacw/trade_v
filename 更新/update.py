import adata
import pandas as pd
import datetime
import utils
import connection
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
table_name = 'stock'  # 替换为你的表名
# final_df = final_df.drop(columns=['Unnamed: 0'])


# 获取当前日期和时间
now = datetime.datetime.now()
# 格式化日期为 yyyy-mm-dd
today = now.strftime('%Y-%m-%d')


df = adata.stock.info.all_code()
for index, row in df.iterrows():
    value = adata.stock.info.get_plate_east(row['stock_code'])
    print(index)
    # amount = adata.stock.market.get_market(row["stock_code"], start_date='2024-12-06')['close']*value['total_shares']
    # print(amount[0]/100000000)
    df.at[index, 'concept'] = ','.join(value[value['plate_type']=='概念']['plate_name'].tolist())
    df.at[index, 'plate'] =','.join(value[value['plate_type']=='板块']['plate_name'].tolist())
    df.at[index, 'profession'] =','.join(value[value['plate_type']=='行业']['plate_name'].tolist())

    value2 = adata.stock.info.get_stock_shares(row['stock_code'], False)
    if len(adata.stock.market.get_market(row["stock_code"], start_date=today))<1:
        continue
    amount = adata.stock.market.get_market(row["stock_code"], start_date=today)['close']*value2['total_shares']
    print(amount[0]/100000000)
    df.at[index, 'market_cap_billion'] = amount[0]/100000000

df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

