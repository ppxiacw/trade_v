import adata
import pandas as pd
from sqlalchemy import create_engine
import datetime

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

# 读取数据



def find_pattern(stock_code):
    # 提取特定股票的数据
    query = f'SELECT * FROM trade.market where trade_date<="2024-12-19" and trade_date>"2024-10-21"  and stock_code={stock_code}'
    stock_df = pd.read_sql(query, engine)

    # 按日期排序
    stock_df['trade_date'] = pd.to_datetime(stock_df['trade_date'])
    stock_df = stock_df.sort_values('trade_date')

    # 计算移动平均线（可选，用于辅助识别趋势）
    stock_df['MA50'] = stock_df['close'].rolling(window=50).mean()
    if  stock_df[stock_df['trade_date']=='2024-12-16'].empty:
        return None,None
    # 初始化变量
    first_peak = None
    second_peak = None
    consolidation_start = None
    consolidation_end = None
    # 遍历数据，识别形态
    v2 = pd.read_sql(f'select high from market where trade_date = "2024-10-8" and stock_code= {stock_code}', engine)
    if v2.empty:
        return None,None
    oct_8_high = v2.iloc[0]['high']

    v = pd.read_sql(f'select high from market where trade_date = "2024-10-8" and stock_code= {stock_code}', engine)
    if v.empty:
        return None,None
    # oct_8_high = stock_df[stock_df['trade_date'] ==
    for i in range(1, len(stock_df) - 1):
        if stock_df['high'].iloc[i] > stock_df['high'].iloc[i - 1] and stock_df['high'].iloc[i] > stock_df['high'].iloc[
            i + 1]:
            if first_peak is None:
                first_peak = stock_df['high'].iloc[i]
            elif stock_df['high'].iloc[i] > first_peak:
                second_peak = stock_df['high'].iloc[i]
                # 检查两个高点的幅度是否不超过20%
                if (second_peak - first_peak) / first_peak * 100 > 20:
                    # print(f"The amplitude between the two peaks for {stock_code} exceeds 20%. Skipping this pattern.")
                    second_peak = None
                    consolidation_start = None
                    break
                # 确保第二波高点大于10月8号的最高点
                elif second_peak <= oct_8_high:
                    # print(
                    #     f"The second peak for {stock_code} is not higher than the high on October 8th. Skipping this pattern.")
                    second_peak = None
                    consolidation_start = None
                    break
                else:
                    consolidation_start = i + 1
                    break

    if second_peak is not None:
        # 检查两个高点的幅度是否不超过20%
        if (second_peak - first_peak) / first_peak * 100 > 20:
            # print("The amplitude between the two peaks exceeds 20%. Skipping this pattern.")
            second_peak = None
            consolidation_start = None
        else:
            for i in range(consolidation_start, len(stock_df)):
                high_price = stock_df['high'].iloc[i]
                # 检查是否接近第二阶段高点，幅度小于4%
                if abs((high_price - second_peak) / second_peak) * 100 < 4:
                    consolidation_end = i
                    break

    if consolidation_end is not None:
        # 检查是否在第二阶段高点附近横盘震荡
        consolidation_high = stock_df['high'].iloc[consolidation_start:consolidation_end].max()
        consolidation_low = stock_df['low'].iloc[consolidation_start:consolidation_end].min()
        if (consolidation_high - consolidation_low) / consolidation_high < 0.20:  # 5%波动范围
            print(f'stock_code{stock_code},{consolidation_high},{consolidation_low}')

            return True, stock_df.iloc[consolidation_start:consolidation_end]

    return False, None


# 示例：识别特定股票的形态
df = adata.stock.info.all_code()
for i,v in df.iterrows():
    print(i)
    stock_code = v['stock_code']
    found, pattern_df = find_pattern(stock_code)

    if found:
        print(f"Pattern found for {stock_code}")
        # print(pattern_df)

    # 示例：保存结果到新的表中
    if found:
        pattern_df.to_sql(name='pattern_results', con=engine, if_exists='append', index=False)