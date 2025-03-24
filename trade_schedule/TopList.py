from config.tushare_utils import pro
import pandas as pd
from pathlib import Path
try:
    # 获取数据
    df = pro.top_list(trade_date='20250324')




    # 方案2：更兼容的写法（适用于所有Pandas版本）
    with open('stock_codes_alt.txt', 'w', encoding='utf-8') as f:
        f.write('\n'.join(df['ts_code'].astype(str)) + '\n')  # 确保最后也有换行

    print("股票代码已成功导出到output目录")

except Exception as e:
    print(f"发生错误: {str(e)}")