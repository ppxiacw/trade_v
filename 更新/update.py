import adata
import pandas as pd
import datetime
import utils
import connection
from sqlalchemy import create_engine
from config.tushare_utils import IndexAnalysis,pro
# MySQL连接信息
db_config = {
        "user":"root",
        "password":"123456",
        "host":"47.103.135.146",  # 或者你的服务器IP地址
        "database":"trade"
}

# 创建MySQL连接引擎
engine = create_engine('mysql+mysqlconnector://{user}:{password}@{host}/{database}'.format(
    **db_config))



# 将DataFrame中的数据写入MySQL表
table_name = 'stock_info_tushare'  # 替换为你的表名
# final_df = final_df.drop(columns=['Unnamed: 0'])


# 获取当前日期和时间
now = datetime.datetime.now()
# 格式化日期为 yyyy-mm-dd
today = now.strftime('%Y-%m-%d')

ana = IndexAnalysis()

df = pd.read_csv('../files/stock_list.csv')
for index, row in df.iterrows():
    print(index)
    # 定义股票代码和日期范围
    # 获取每日基本面信息
    v = pro.daily_basic(ts_code=row['ts_code'], trade_date='20250124', fields='ts_code,trade_date,total_mv')
    try:
        amount = v['total_mv']/10000
        df.at[index, 'market_cap_billion'] = amount[0]
    except:
        pass
df.to_csv('../files/stock_list.csv',index=False)
# df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

