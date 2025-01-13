from datetime import datetime
import pandas as pd
import adata

import dbconfig


def get_date_by_step(date, n):
    calendar = adata.stock.info.trade_calendar(date[:4])
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
                    if len(selected_rows) >= n + 1:
                        break

            result_df = pd.DataFrame(selected_rows)  # 保持原始顺序（向后）
        elif n < 0:
            # 负数：向前 n 个数据中选择，且 trade_status 为 1
            for i in range(start_index, -1, -1):
                row = calendar.iloc[i]
                if row['trade_status'] == '1':
                    selected_rows.append(row)
                    if len(selected_rows) >= abs(n - 1):
                        break

            result_df = pd.DataFrame(selected_rows).iloc[::-1]  # 恢复原始顺序（向前）
        else:
            result_df = pd.DataFrame()  # 如果 n 是 0，则返回空 DataFrame

        return result_df.iloc[0]['trade_date']
    else:
        print(f"没有找到 trade_date 为 {date} 的记录")


def get_today():
    now = datetime.now()
    # 格式化日期为 yyyy-mm-dd
    today = now.strftime('%Y-%m-%d')
    calendar = adata.stock.info.trade_calendar(today[:4])
    index = calendar[calendar['trade_date'] == today].index[0]
    if calendar.loc[index]['trade_status'] == '0':
        for i in reversed(range(0, index)):
            if calendar.loc[i]['trade_status'] == '1':
                return calendar.loc[i]['trade_date']
    else:
        return today





def find_stock_info(stock_code):
    # 如果 df 尚未加载，则加载它并保存为函数的属性
    if not hasattr(find_stock_info, "df"):
        query = 'select * from stock'
        find_stock_info.df = pd.read_sql(query, dbconfig.engine)

    # 使用已经加载的数据框进行查询
    return find_stock_info.df[find_stock_info.df['stock_code'] == stock_code]


