from datetime import datetime
import pandas as pd
import adata


class StockAnalysis:
    def __init__(self):
        self.calendar_cache = None  # 缓存交易日历
        self.stock_info_df = None  # 缓存股票信息数据框

    def get_trade_calendar(self, year=None):
        if not year:
            year = datetime.now().year
        if not self.calendar_cache or str(year) not in self.calendar_cache:
            self.calendar_cache = {str(year): adata.stock.info.trade_calendar(str(year))}
        return self.calendar_cache[str(year)]

    def get_date_by_step(self, date_str, n):
        calendar = self.get_trade_calendar(date_str[:4])
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
                return result_df.iloc[-1]
            elif n < 0:
                for i in range(start_index, -1, -1):
                    row = calendar.iloc[i]
                    if row['trade_status'] == '1':
                        selected_rows.append(row)
                        if len(selected_rows) >= abs(n-1):
                            break
                result_df = pd.DataFrame(selected_rows)['trade_date']
                return result_df.iloc[-1]
                # 恢复原始顺序（向前）
            else:
                result_df = pd.DataFrame()  # 如果 n 是 0，则返回空 DataFrame
        else:
            print(f"没有找到 trade_date 为 {date_str} 的记录")
            return None

    def get_today(self,replace=False):
        now = datetime.now()
        today_str = now.strftime('%Y-%m-%d')
        calendar = self.get_trade_calendar(today_str[:4])
        index = calendar[calendar['trade_date'] == today_str].index[0] if not calendar[
            calendar['trade_date'] == today_str].empty else None

        if index is not None and calendar.loc[index]['trade_status'] == '0':
            for i in reversed(range(0, index)):
                if calendar.loc[i]['trade_status'] == '1':
                    today_str = calendar.loc[i]['trade_date']
                    break
        elif index is not None:
            if replace:
                return today_str.replace('-','')
            else:
                return today_str
        else:
            return None

        if replace:
            return today_str.replace('-', '')
        else:
            return today_str

    def find_stock_info(self, stock_code):
        # 如果 df 尚未加载，则加载它并保存为类属性
        if self.stock_info_df is None:
            self.stock_info_df = pd.read_csv("C:/Users/曹威/PycharmProjects/pythonProject/files/stock_list.csv",dtype={'symbol':str})

        # 使用已经加载的数据框进行查询
        return self.stock_info_df[self.stock_info_df['symbol'] == stock_code]

# 使用类进行分析
if __name__ == "__main__":
    an = StockAnalysis()
    an.get_today()