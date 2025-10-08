from datetime import datetime
import pandas as pd
import adata


class Date_utils:
    calendar_cache = {}  # 类级别的交易日历缓存
    stock_info_df = None  # 类级别的股票信息缓存

    @staticmethod
    def get_trade_calendar(year=None):
        if not year:
            year = datetime.now().year
        year_str = str(year)

        if year_str not in Date_utils.calendar_cache:
            Date_utils.calendar_cache[year_str] = adata.stock.info.trade_calendar(year_str)
        return Date_utils.calendar_cache[year_str]

    @staticmethod
    def get_date_by_step(date_str, n, replace=False):
        # 新增逻辑：自动将 'yyyyMMdd' 格式的输入转换为 'yyyy-mm-dd'
        if '-' not in date_str:
            # 假设是 8 位数字的日期格式（如：20250101）
            year = date_str[:4]
            month = date_str[4:6]
            day = date_str[6:8]
            # 替换为标准的带连字符格式
            date_str = f"{year}-{month}-{day}"
        calendar = Date_utils.get_trade_calendar(date_str[:4])
        start_index = calendar[calendar['trade_date'] == date_str].index[0] if not calendar[
            calendar['trade_date'] == date_str].empty else None

        if start_index is not None:
            selected_rows = []

            if n > 0:
                for i in range(start_index, len(calendar)):
                    row = calendar.iloc[i]
                    if row['trade_status'] == '1':
                        selected_rows.append(row)
                        if len(selected_rows) >= n + 1:
                            break
                result_df = pd.DataFrame(selected_rows)['trade_date']
                result = result_df.iloc[-1]
                if replace:
                    return result.replace('-', '')
                else:
                    return result
            elif n < 0:
                for i in range(start_index, -1, -1):
                    row = calendar.iloc[i]
                    if str(row['trade_status']) == '1':
                        selected_rows.append(row)
                        if len(selected_rows) >= abs(n - 1):
                            break
                result_df = pd.DataFrame(selected_rows)['trade_date']
                result = result_df.iloc[-1]
                if replace:
                    return result.replace('-', '')
                else:
                    return result
            else:
                return pd.DataFrame()  # 如果 n 是 0，则返回空 DataFrame
        else:
            print(f"没有找到 trade_date 为 {date_str} 的记录")
            return None

    @staticmethod
    def get_today(replace=False):
        now = datetime.now()
        today_str = now.strftime('%Y-%m-%d')
        calendar = Date_utils.get_trade_calendar(today_str[:4])
        index = calendar[calendar['trade_date'] == today_str].index[0] if not calendar[
            calendar['trade_date'] == today_str].empty else None

        if index is not None and str(calendar.loc[index]['trade_status']) == '0':
            for i in reversed(range(0, index)):
                if calendar.loc[i]['trade_status'] == '1':
                    today_str = calendar.loc[i]['trade_date']
                    break
        elif index is not None:
            if replace:
                return today_str.replace('-', '')
            else:
                return today_str
        else:
            return None

        if replace:
            return today_str.replace('-', '')
        else:
            return today_str


# 使用类进行分析
if __name__ == "__main__":
    # 直接调用静态方法，无需实例化
    a = Date_utils.get_trade_calendar()
    print(a)