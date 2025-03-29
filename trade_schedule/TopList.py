from config.tushare_utils import pro
import pandas as pd
from pathlib import Path
import os

from datetime import datetime  # 正确导入 datetime 类
current_datetime = datetime.now()

# 格式化日期和时间为 'yy-mm' 格式
today = current_datetime.strftime('%Y%m%d')
try:
    # 获取数据
    df = pro.top_list(trade_date=today)
    df = df[df['ts_code'].astype(str).str.startswith(('60', '00'))]

    # 方案2：更兼容的写法（适用于所有Pandas版本）

    output_dir = 'top_list_files'  # 当前目录下的文件夹
    os.makedirs(output_dir, exist_ok=True)

    file_path = os.path.join(output_dir, f'{today}.txt')

    with open(file_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(df['ts_code'].astype(str)))

    print("股票代码已成功导出到output目录")

except Exception as e:
    print(f"发生错误: {str(e)}")