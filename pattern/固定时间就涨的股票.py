from dbconfig import connection
import adata
import json

df = adata.stock.info.all_code()
df['stock_code'] = df['stock_code'].astype(str)

change_dict = {}
for i, v in df.iterrows():
    # 确保每个股票代码作为键存在于 change_dict 中
    if v["stock_code"] not in change_dict:
        change_dict[v["stock_code"]] = {}
    cursor = connection.cursor()
    for year in range(2015,2024+1):
        # if i>5:
        #     break
        # 执行查询
        # query = (f'select sum(change_pct) from market_{year} where stock_code = "{v["stock_code"]}" and trade_date between "{year}-01-01" and "{year}-01-31"')
        query = (f'SELECT DATE_FORMAT(trade_date, "%Y-%m") AS month,SUM(change_pct) AS total_change_pct FROM market_{year} WHERE stock_code = "{v["stock_code"]}" GROUP BY month ORDER BY month')
        cursor.execute(query)
        result = cursor.fetchall()
        change_dict[v["stock_code"]][str(year)] = result
        print(f'stock_code:{v["stock_code"]},year:{year},change_pct{result}')

file_path = 'stock_change.json'

# 使用 with 语句来管理文件对象，这样可以在不需要的时候自动关闭文件
with open(file_path, 'w', encoding='utf-8') as json_file:
    # 将字典序列化为 JSON 并写入文件
    json.dump(change_dict, json_file, ensure_ascii=False, indent=4)