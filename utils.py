from datetime import datetime
import pandas as pd
import adata

import dbconfig

date = '2025-01-07'
n = 6
calendar = adata.stock.info.trade_calendar('2025')

# 找到 trade_date 等于指定日期的第一行的索引
# 找到 trade_date 等于指定日期的第一行的索引
start_index = calendar[calendar['trade_date'] == date].index[0] if not calendar[
    calendar['trade_date'] == date].empty else None

if start_index is not None:
    selected_rows = []

    if n > 0:
        # 正数：向后 n 个数据中选择，且 trade_status 为 1
        for i in range(start_index, len(calendar)):
            row = calendar.iloc[i]
            if row['trade_status'] == '1':
                selected_rows.append(row)
                if len(selected_rows) >= n:
                    break

        result_df = pd.DataFrame(selected_rows)  # 保持原始顺序（向后）
    elif n < 0:
        # 负数：向前 n 个数据中选择，且 trade_status 为 1
        for i in range(start_index, -1, -1):
            row = calendar.iloc[i]
            if row['trade_status'] == '1':
                selected_rows.append(row)
                if len(selected_rows) >= abs(n):
                    break

        result_df = pd.DataFrame(selected_rows).iloc[::-1]  # 恢复原始顺序（向前）
    else:
        result_df = pd.DataFrame()  # 如果 n 是 0，则返回空 DataFrame

    print(result_df)
else:
    print(f"没有找到 trade_date 为 {date} 的记录")


def get_today():
    now = datetime.now()
    # 格式化日期为 yyyy-mm-dd
    today = now.strftime('%Y-%m-%d')
    return today


query = (f'select * from stock')
df = pd.read_sql(query, dbconfig.engine)


def find_stock_info(stock_code):
    return df[df['stock_code'] == stock_code]


