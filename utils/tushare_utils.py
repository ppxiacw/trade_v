import numpy as np
import tushare as ts
import pandas as pd
from dto.StockDataDay import StockDataDay
from dto.RealTimeStockData import RealTimeStockData
from datetime import datetime  # 正确导入 datetime 类
import warnings
import ssl

warnings.simplefilter(action='ignore', category=FutureWarning)
import os
from utils.GetStockData import result
from urllib.request import urlopen  # python自带爬虫库
import json  # python自带的json数据库
from random import randint  # python自带的随机数库
import pandas as pd
from utils.date_utils import Date_utils
from datetime import datetime, timedelta

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行

# 获取当前脚本的完整路径
current_path = os.path.abspath(__file__)

# 获取当前脚本的目录
dir_path = os.path.dirname(current_path)

# 获取当前脚本的上级目录
parent_dir_path = os.path.dirname(dir_path)

# 构造相对路径
relative_path = os.path.join(parent_dir_path, 'files')

token = '410070664c78124d98ca5e81c3921530bd27534856b174c702d698a5'
ts.set_token(token)
pro = ts.pro_api(token)

stock_list = result
ma_cache = {}


class IndexAnalysis:
    def __init__(self):
        pass

    @staticmethod
    def get_stock_daily(ts_code, start_date, end_date=None):
        if end_date is None:
            end_date = start_date
        if len(ts_code) == 6:
            ts_code = stock_list[stock_list['symbol'] == ts_code]['ts_code'].tolist()[0]
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        v = ts.pro_bar(ts_code=ts_code, adj='qfq', start_date=start_date, end_date=end_date)
        if v is None or v.empty:
            return None
        # 将日期列转换为 datetime 类型，并设置为索引
        return StockDataDay.from_daily_dataframe(v)

    @staticmethod
    def realtime_quote(ts_code):
        v: pd = ts.realtime_quote(ts_code=ts_code)
        arr = []
        for item in v.iterrows():
            arr.append(RealTimeStockData.from_dataframe(item[1].to_frame().T))
        return arr

    @staticmethod
    def stk_factor(ts_code, date):
        df = pro.stk_factor(ts_code=ts_code, start_date=date, end_date=date,
                            fields='ts_code,trade_date,rsi_6,rsi_12,rsi_24,kdj_j')
        return df

    @staticmethod
    def rt_min(stock_code, k_type=1, num=320):
        # num最多不能超过320
        # =====获取分钟级别的K线
        # 获取K线数据：http://ifzq.gtimg.cn/appstock/app/kline/mkline?param=sz000001,m5,,640&_var=m5_today&r=0.6508601564534552
        # 正常网址：http://stockhtm.finance.qq.com/sstock/ggcx/000001.shtml
        # 分割代码和交易所
        code_part, exchange_part = stock_code.upper().split('.')

        # 转换为新浪需要的格式（sh/sz + 代码）
        sina_code = f"{exchange_part.lower()}{code_part}"
        # ===构建网址
        stock_code = sina_code  # # 正常股票sz000001，指数sh000001, ETF sh510500
        k_type = k_type  # 1, 5, 15, 30, 60
        start = 10 ** (16 - 1)
        end = (10 ** 16) - 1
        random_num = str(randint(start, end))
        # 构建url
        url = 'http://ifzq.gtimg.cn/appstock/app/kline/mkline?param=%s,m%s,,%s&_var=m%s_today&r=0.%s'
        url = url % (stock_code, k_type, num, k_type, random_num)

        # ===获取数据
        import ssl
        context = ssl._create_unverified_context()
        content = urlopen(url=url, context=context, timeout=15).read().decode()  # 使用python自带的库，从网络上获取信息

        # ===将数据转换成dict格式
        content = content.split('=', maxsplit=1)[-1]
        content = json.loads(content)

        # ===将数据转换成DataFrame格式
        k_data = content['data'][stock_code]['m' + str(k_type)]
        df = pd.DataFrame(k_data)

        # ===对数据进行整理
        rename_dict = {0: 'candle_end_time', 1: 'open', 2: 'close', 3: 'high', 4: 'low', 5: 'amount'}
        # 其中amount单位是手
        df.rename(columns=rename_dict, inplace=True)
        df['candle_end_time'] = df['candle_end_time'].apply(
            lambda x: '%s-%s-%s %s:%s' % (x[0:4], x[4:6], x[6:8], x[8:10], x[10:12]))
        df['candle_end_time'] = pd.to_datetime(df['candle_end_time'])
        df = df[['candle_end_time', 'open', 'high', 'low', 'close', 'amount']]
        cols_to_convert = df.columns.drop('candle_end_time')
        df[cols_to_convert] = df[cols_to_convert].apply(pd.to_numeric, errors='coerce')

        return df


    @staticmethod
    def get_volume_ratio(stock_code, k_type=1):
        """
        计算今日开盘到当前时刻成交量 / 上一个交易日相同时段成交量
        :param stock_code: 股票代码（如 '000001.SZ'）
        :param k_type: K线类型（1分钟、5分钟等）
        :return: 成交量比值（float），若无昨日数据则返回None
        """
        # 获取当前时间（用于筛选今日数据）
        now = datetime.now()
        current_time = now.strftime('%H:%M:%S')
        today = Date_utils.get_today()
        his_day = Date_utils.get_date_by_step(Date_utils.get_today(),-1)
        #获取历史分钟代码
        his_df =  pro.stk_mins(ts_code=stock_code, freq=f'{k_type}min', start_date=f'{his_day} 09:00:00', end_date=f'{his_day} {current_time}')
        # 获取今日分钟级数据
        today_df = IndexAnalysis.rt_min(stock_code, k_type=k_type, num=320)
        if today_df.empty:
            return None



        # 筛选今日开盘到当前时刻的数据
        today_df = today_df[today_df['candle_end_time'].dt.date == now.date()]
        today_df = today_df[today_df['candle_end_time'].dt.strftime('%H:%M') <= current_time]
        today_volume = today_df['amount'].sum()  # 今日累计成交量（单位：手）

        # 获取上一个交易日的日期
        # 从 today_df 中找到离今天最近的日期
        unique_dates = today_df['candle_end_time'].dt.date.unique()
        if len(unique_dates) < 2:  # 如果没有足够的历史数据
            return None
        last_trading_date = unique_dates[-2]  # 取倒数第二个日期（上一个交易日）

        # 获取上一个交易日的分钟级数据
        last_trading_df = IndexAnalysis.rt_min(stock_code, k_type=k_type, num=320)
        if last_trading_df.empty:
            return None

        # 筛选上一个交易日相同时段的数据
        last_trading_df = last_trading_df[last_trading_df['candle_end_time'].dt.date == last_trading_date]
        last_trading_df = last_trading_df[last_trading_df['candle_end_time'].dt.strftime('%H:%M') <= current_time]
        last_trading_volume = last_trading_df['amount'].sum()  # 上一个交易日累计成交量（单位：手）

        # 计算比值（避免除以0）
        if last_trading_volume == 0:
            return None
        return today_volume / last_trading_volume

    @staticmethod
    def my_pro_bar(stock_code, start_date=None, end_date=None):
        # 创建包含所有三个参数的缓存键
        cache_key = f"ma_{stock_code}_{start_date}_{end_date}"

        if cache_key in ma_cache:
            return ma_cache[cache_key]

        # 如果用户没有传入开始/结束日期，使用默认逻辑计算
        if start_date is None:
            start_date = Date_utils.get_date_by_step(Date_utils.get_today(replace=False), -130, True)
        if end_date is None:
            end_date = Date_utils.get_today(replace=True)

        # 判断资产类型
        if stock_code.endswith('.SH'):
            if stock_code[:3] in ['000', '999']:
                asset_type = 'I'
            elif stock_code[:1] in ['5', '51']:
                asset_type = 'FD'
            elif stock_code[:3] in ['110', '113']:
                asset_type = 'CB'
            else:
                asset_type = 'E'
        elif stock_code.endswith('.SZ'):
            if stock_code[:3] == '399':
                asset_type = 'I'
            elif stock_code[:2] in ['15', '16', '18']:
                asset_type = 'FD'
            elif stock_code[:3] in ['123', '127', '128']:
                asset_type = 'CB'
            else:
                asset_type = 'E'
        else:
            asset_type = 'E'

        # 缓存中没有或已过期，重新获取数据
        data = ts.pro_bar(
            ts_code=stock_code,
            asset=asset_type,  # 使用判断出的资产类型
            start_date=start_date,
            end_date=end_date,
            ma=[5, 10, 20, 30, 60, 120]
        )

        # 存入缓存
        ma_cache[cache_key] = data
        return data

    def calculate_realtime_ma(df, current_price_dict=None, window_sizes=[5, 10, 20, 30, 60], date_col='trade_date'):

        # 确保日期列是datetime类型
        if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
            df[date_col] = pd.to_datetime(df[date_col])

        # 获取最新日期（降序排列的首行）
        latest_date_in_data = df[date_col].iloc[0]

        # 初始化变量
        use_current_price = False
        current_price = None

        # 处理当前价格
        if current_price_dict:
            try:
                current_price = float(current_price_dict['close'])
                current_timestamp = pd.to_datetime(current_price_dict['timestamp'])

                # 判断是否需要使用当前价格（日期不同）
                if current_timestamp.date() != latest_date_in_data.date():
                    use_current_price = True
            except KeyError:
                raise ValueError("current_price_dict must contain 'price' and 'timestamp' keys")

        ma_values = {}
        for window in window_sizes:
            if use_current_price:
                required_history = window - 1
                # 获取最近的历史数据（降序排列的前N条）
                if len(df) >= required_history:
                    history_closes = df['close'].iloc[:required_history].values
                    all_closes = np.append(history_closes, current_price)
                    ma_value = np.mean(all_closes)
                else:
                    ma_value = None  # 数据不足
            else:
                # 直接使用历史数据（降序排列的前window条）
                if len(df) >= window:
                    ma_value = df['close'].iloc[:window].mean()
                else:
                    ma_value = None  # 数据不足

            ma_values[f'ma{window}'] = ma_value

        ma_values['used_current_price'] = use_current_price
        return ma_values

# 使用类进行分析
if __name__ == "__main__":
    # df = ts.pro_bar(ts_code='000001.SZ', adj='qfq', ma=[5,10,20,60],start_date='20250509', end_date='20250410')
    # 获取浦发银行60000.SH的历史分钟数据
    # 示例：计算平安银行（000001.SZ）今日成交量与昨日的比值
    # ratio = IndexAnalysis.get_volume_ratio('000831.SZ', k_type=1)
    v =  IndexAnalysis.my_pro_bar('560860.SH')
    print(v)


