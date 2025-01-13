import csv
import json
from datetime import datetime

data = None
# 数据样本
with open('data.json',encoding='utf-8')as f:
    data = json.load(f)






# 定义CSV文件路径和名称
csv_file_path = 'stock_data.csv'

# 打开CSV文件进行写入操作
with open(csv_file_path, mode='w', newline='') as file:
    # 创建一个DictWriter对象并指定列名
    writer = csv.DictWriter(file, fieldnames=["index","stock_code", "trade_date", "change_pct"])

    # 写入CSV文件的表头
    writer.writeheader()

    # 写入数据行
    for record in data:
        # 如果需要，可以在这里对数据进行任何格式化操作，比如日期格式转换等
        formatted_record = {
            "index": record["index"],
            "stock_code": record["stock_code"],
            "trade_date": record["trade_date"],  # 假设记录中的日期已经是字符串形式
            "change_pct": record["change_pct"]
        }
        writer.writerow(formatted_record)

print(f"Data has been written to {csv_file_path}")