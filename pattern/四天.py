import pandas as pd

# 确保所有 market_info_dict 中的数据类型正确
dtype_dict = {'stock_code': str}

# 初始化市场信息字典并一次性读取所有年份的数据
start_year = 2024
end_year = 2024 + 1  # 根据你的需求调整结束年份
# end_year = 2014 + 1  # 根据你的需求调整结束年份

market_info_dict = {}

for year in range(start_year, end_year):
    # 读取每年的数据
    df = pd.read_csv(f'C:\\Users\\曹威\\Desktop\\market\\market_{year}.csv', dtype=dtype_dict)

    # 确保 trade_date 是 datetime 类型，并设置为索引
    df['trade_date'] = pd.to_datetime(df['trade_date'])

    # 创建一个空字典来存储每个 stock_code 的 DataFrame
    market_info_dict[year] = {}

    # 按 stock_code 分组并分别处理
    for stock_code, group in df.groupby('stock_code'):
        # 设置复合索引 (stock_code, trade_date)，并确保数据按 trade_date 排序
        group.set_index('trade_date', inplace=True)
        group.sort_index(inplace=True)

        # 将处理后的 DataFrame 存储到 market_info_dict 中
        market_info_dict[year][stock_code] = group




# 读取主数据集并应用指定的数据类型
data = pd.read_csv('../config/stock_info.csv', dtype=dtype_dict)

sale_day =3




# 第二天和第三天量差不多大,但是第三天的涨幅比第二天的高不少
def limit(df_stock_year,index):
   flag1 = df_stock_year.loc[index, 'change_pct'] <= 5
   flag = df_stock_year.loc[index, 'change_pct']-df_stock_year.loc[index-1, 'change_pct'] > 0
   flag2 = (df_stock_year.loc[index]['volume']/df_stock_year.loc[index-1]['volume'])<1.1
   flag3 = (df_stock_year.loc[index]['change_pct']/df_stock_year.loc[index-1]['change_pct'])>1.5
   return flag1 and flag2 and flag3 and flag



def findStock():
    all_change_pct = 0
    count = 0
    results =[]
    for index, item in data.iterrows():
        year = int(item['trade_date'][:4])
        stock_code = item['symbol']
        #
        # # 提前筛选出特定股票代码和年的数据，并重置索引
        df_stock_year = market_info_dict[year][stock_code].reset_index(
            drop=True)

        i_index = item["index"]

        if not limit(df_stock_year,i_index):
            continue


        count = count+1
        change_pct = (df_stock_year.loc[i_index + sale_day, 'close'] - df_stock_year.loc[i_index + 1, 'open']) / \
                     df_stock_year.loc[i_index + 1, 'open']*100
        # 添加结果到列表中
        results.append({
            'stock_code': stock_code,
            'trade_date': item['trade_date'],
            'change_pct': change_pct
        })
        all_change_pct += change_pct
    sorted_results = sorted(results, key=lambda x: (-x['change_pct'], x['stock_code']))
    avg_change_pct = all_change_pct / count

    for sorted_result in sorted_results:
        print(sorted_result)
    print(f"{avg_change_pct},{all_change_pct},{count}")
    return avg_change_pct


findStock()

