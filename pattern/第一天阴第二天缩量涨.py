import pandas as pd
import json
market_info_dict = {}

start_year = 2024
for i in range(start_year, 2024 + 1):
    print(i)
    file_path = f'C:\\Users\\曹威\\Desktop\\market\\market_{i}.csv'
    df = pd.read_csv(file_path, dtype={'stock_code': str})

    # 遍历数据框中的每一行
    for index, row in df.iterrows():
        stock_code = row['stock_code']
        # 如果 stock_code 不在字典中，则初始化一个空列表
        if stock_code not in market_info_dict:
            market_info_dict[stock_code] = {}

        # 向列表中添加当前年份的数据，可以自行决定如何保存具体信息
        # 这里我们简单地保存整行数据作为一个记录
        market_info_dict[stock_code][row['trade_date']]= row.to_dict()

# 将数据保存为JSON文件
file_path = 'C:\\Users\\曹威\\PycharmProjects\\pythonProject\\files\\data_his_2024.json'  # 文件路径，请根据实际情况修改

with open(file_path, 'w', encoding='utf-8') as f:
    json.dump(market_info_dict, f, ensure_ascii=False, indent=4)

# with open(file_path, 'r', encoding='utf-8') as f:
#     market_info_dict = json.load(f)
#
# for i in market_info_dict:
#     print(i)