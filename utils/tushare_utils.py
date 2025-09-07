import tushare as ts
import pandas as pd
from dto.StockDataDay import StockDataDay
from dto.RealTimeStockData import RealTimeStockData
from datetime import datetime  # 正确导入 datetime 类
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import os
from utils.GetStockData import result
from urllib.request import urlopen  # python自带爬虫库
import json  # python自带的json数据库
from random import randint  # python自带的随机数库
import pandas as pd
from utils.date_utils import Date_utils
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行

# 获取当前脚本的完整路径
current_path = os.path.abspath(__file__)

# 获取当前脚本的目录
dir_path = os.path.dirname(current_path)

# 获取当前脚本的上级目录
parent_dir_path = os.path.dirname(dir_path)

# 构造相对路径
relative_path = os.path.join(parent_dir_path, 'files')



token  = '410070664c78124d98ca5e81c3921530bd27534856b174c702d698a5'
ts.set_token(token)
pro = ts.pro_api(token)

stock_list  = result
ma_cache = {}


class IndexAnalysis:
    def __init__(self):
        pass


    @staticmethod
    def get_stock_daily(ts_code, start_date, end_date=None):
        if end_date is None:
            end_date = start_date
        if len(ts_code)==6:
            ts_code = stock_list[stock_list['symbol'] == ts_code]['ts_code'].tolist()[0]
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        v= ts.pro_bar(ts_code=ts_code, adj='qfq', start_date=start_date, end_date=end_date)
        if v is None or  v.empty:
            return None
        # 将日期列转换为 datetime 类型，并设置为索引
        return StockDataDay.from_daily_dataframe(v)

    @staticmethod
    def calculate_pct(ts_code, start_date, end_date):
        df = ts.pro_bar(ts_code=ts_code, adj='qfq', start_date=start_date, end_date=end_date)
        change_pct =df['pct_chg'].sum()
        return change_pct

    @staticmethod
    def realtime_quote(ts_code):
        v:pd = ts.realtime_quote(ts_code=ts_code)
        arr = []
        for item in v.iterrows():
            arr.append(RealTimeStockData.from_dataframe(item[1].to_frame().T))
        return arr

    @staticmethod
    def stk_limit(date):
        df = pro.stk_limit(date)
        return df
    @staticmethod
    def stk_factor(ts_code,date):
        df = pro.stk_factor(ts_code=ts_code, start_date=date, end_date=date,
                            fields='ts_code,trade_date,rsi_6,rsi_12,rsi_24,kdj_j')
        return df

    @staticmethod
    def rt_min(stock_code,k_type=1,num=320):
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
        random_num =str(randint(start, end))
        # 构建url
        url = 'http://ifzq.gtimg.cn/appstock/app/kline/mkline?param=%s,m%s,,%s&_var=m%s_today&r=0.%s'
        url = url % (stock_code, k_type, num, k_type,random_num)

        # ===获取数据
        content = urlopen(url=url, timeout=15).read().decode()  # 使用python自带的库，从网络上获取信息

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

        # ===考察其他周期、指数、ETF

        # ===考察特殊情况
        # 正常股票：sz000001 sz000002，退市股票：sh600002 sz000003、停牌股票：sz300124，上市新股：sz002952，除权股票：sh600276，

    @staticmethod
    def get_ma(stock_code):
        ma_cache = {}
        # 检查缓存中是否存在且未过期（假设缓存1小时）
        cache_key = f"ma_{stock_code}"
        if cache_key in ma_cache:
            return ma_cache[cache_key]

        if stock_code.endswith('.SH'):
            if stock_code[:3] in ['000', '999']:
                asset_type = 'I'
            elif stock_code[:2] in ['50', '51']:
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
            start_date=Date_utils.get_date_by_step(Date_utils.get_today(replace=False), -130, True),
            end_date=Date_utils.get_today(replace=True),
            ma=[5, 10, 20, 30, 60, 120])

        # 存入缓存
        ma_cache[cache_key] = data
        return data
# 使用类进行分析
if __name__ == "__main__":
    # df = ts.pro_bar(ts_code='000001.SZ', adj='qfq', ma=[5,10,20,60],start_date='20250509', end_date='20250410')
    # 获取浦发银行60000.SH的历史分钟数据
    df =IndexAnalysis.rt_min('600000.SH',k_type=1,num=320)
    print(df)






