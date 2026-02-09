"""
股票数据工具模块 - 使用 akshare 获取数据
"""
from pathlib import Path
import numpy as np
import akshare as ak
import pandas as pd
from dto.StockDataDay import StockDataDay
from dto.RealTimeStockData import RealTimeStockData
import warnings
import ssl
import logging

warnings.simplefilter(action='ignore', category=FutureWarning)
import os

# 禁用代理，避免代理连接问题
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'
if 'HTTP_PROXY' in os.environ:
    del os.environ['HTTP_PROXY']
if 'HTTPS_PROXY' in os.environ:
    del os.environ['HTTPS_PROXY']
if 'http_proxy' in os.environ:
    del os.environ['http_proxy']
if 'https_proxy' in os.environ:
    del os.environ['https_proxy']

from utils.GetStockData import result
from urllib.request import urlopen
import json
from random import randint
from utils.date_utils import Date_utils
from datetime import datetime, timedelta
from utils.common import format_stock_code

pd.set_option('expand_frame_repr', False)

# 获取当前脚本的完整路径
current_path = os.path.abspath(__file__)
dir_path = os.path.dirname(current_path)
parent_dir_path = os.path.dirname(dir_path)
relative_path = os.path.join(parent_dir_path, 'files')

# 保留 token 用于兼容（虽然不再使用 tushare）
token = '410070664c78124d98ca5e81c3921530bd27534856b174c702d698a5'

stock_list = result
ma_cache = {}


def _convert_code_to_akshare(ts_code):
    """
    将 tushare 格式代码转换为 akshare 格式
    000001.SZ -> 000001
    """
    return ts_code.split('.')[0]


def _convert_code_to_tushare(code, exchange=None):
    """
    将纯代码转换为 tushare 格式
    000001 -> 000001.SZ
    """
    if '.' in code:
        return code
    if exchange:
        return f"{code}.{exchange}"
    # 根据代码判断交易所
    if code.startswith('6'):
        return f"{code}.SH"
    else:
        return f"{code}.SZ"


class IndexAnalysis:
    def __init__(self):
        pass

    @staticmethod
    def get_stock_daily(ts_code, start_date, end_date=None):
        """
        使用 akshare 获取股票日线数据
        """
        if end_date is None:
            end_date = start_date
        
        # 转换代码格式
        if len(ts_code) == 6:
            ts_code = stock_list[stock_list['symbol'] == ts_code]['ts_code'].tolist()[0]
        
        pure_code = _convert_code_to_akshare(ts_code)
        
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        
        try:
            # 转换日期格式 YYYYMMDD -> YYYY-MM-DD
            start_date_fmt = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}" if len(start_date) == 8 else start_date
            end_date_fmt = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}" if len(end_date) == 8 else end_date
            
            # 使用 akshare 获取数据
            df = ak.stock_zh_a_hist(symbol=pure_code, period="daily", 
                                     start_date=start_date_fmt, end_date=end_date_fmt, 
                                     adjust="qfq")
            
            if df is None or df.empty:
                return None
            
            # 转换列名以兼容原有格式
            df = df.rename(columns={
                '日期': 'trade_date',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'vol',
                '成交额': 'amount',
            })
            df['ts_code'] = ts_code
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y%m%d')
            
            # 按日期降序排列
            df = df.sort_values('trade_date', ascending=False).reset_index(drop=True)
            
            return StockDataDay.from_daily_dataframe(df)
        except Exception as e:
            logging.error(f"获取股票日线数据失败: {e}")
            return None

    @staticmethod
    def realtime_quote(ts_codes):
        """
        使用腾讯接口获取实时行情数据
        接口地址: https://ifzq.gtimg.cn/appstock/app/kline/mkline
        """
        code_list = [code.strip() for code in ts_codes.split(',') if code.strip()]
        
        arr = []
        current_time = datetime.now().strftime('%H:%M:%S')
        
        for code in code_list:
            try:
                # 转换代码格式，支持多种输入格式
                # 000001.SZ -> sz000001
                # sh000001 -> sh000001
                # 000001 -> sz000001 (默认深圳)
                if '.' in code:
                    code_part, exchange_part = code.upper().split('.')
                    qq_code = f"{exchange_part.lower()}{code_part}"
                    ts_code = f"{code_part}.{exchange_part.upper()}"
                elif code.lower().startswith(('sh', 'sz')):
                    qq_code = code.lower()
                    exchange_part = code[:2].upper()
                    code_part = code[2:]
                    ts_code = f"{code_part}.{exchange_part}"
                else:
                    # 默认根据代码前缀判断交易所
                    if code.startswith('6'):
                        qq_code = f"sh{code}"
                        ts_code = f"{code}.SH"
                    else:
                        qq_code = f"sz{code}"
                        ts_code = f"{code}.SZ"
                    code_part = code
                
                # 构建腾讯接口 URL
                random_num = str(randint(10**15, 10**16 - 1))
                url = f'http://ifzq.gtimg.cn/appstock/app/kline/mkline?param={qq_code},m1,,1&_var=m1_today&r=0.{random_num}'
                
                # 请求数据
                context = ssl._create_unverified_context()
                content = urlopen(url=url, context=context, timeout=10).read().decode()
                
                # 解析数据
                content = content.split('=', maxsplit=1)[-1]
                data = json.loads(content)
                
                # 获取实时行情数据 (qt 字段)
                qt_data = data.get('data', {}).get(qq_code, {}).get('qt', {}).get(qq_code, [])
                
                if not qt_data or len(qt_data) < 10:
                    logging.warning(f"获取 {code} 实时数据失败：数据格式不正确")
                    continue
                
                # 解析 qt 数组
                # 索引: 1=名称, 2=代码, 3=最新价, 4=昨收, 5=今开, 6=成交量, 14=最高, 15=最低
                name = qt_data[1] if len(qt_data) > 1 else ''
                price = float(qt_data[3]) if len(qt_data) > 3 and qt_data[3] else 0
                pre_close = float(qt_data[4]) if len(qt_data) > 4 and qt_data[4] else 0
                open_price = float(qt_data[5]) if len(qt_data) > 5 and qt_data[5] else 0
                volume = float(qt_data[6]) if len(qt_data) > 6 and qt_data[6] else 0
                high = float(qt_data[33]) if len(qt_data) > 33 and qt_data[33] else price
                low = float(qt_data[34]) if len(qt_data) > 34 and qt_data[34] else price
                amount = float(qt_data[37].split('/')[2]) if len(qt_data) > 37 and qt_data[37] and '/' in str(qt_data[37]) else 0
                
                # 构建与 RealTimeStockData 兼容的数据格式
                df_data = pd.DataFrame([{
                    'TS_CODE': ts_code,
                    'NAME': name,
                    'TIME': current_time,
                    'OPEN': open_price,
                    'HIGH': high,
                    'LOW': low,
                    'PRICE': price,
                    'PRE_CLOSE': pre_close,
                    'VOLUME': volume / 100,  # 转换为手
                    'AMOUNT': amount,
                }])
                arr.append(RealTimeStockData.from_dataframe(df_data))
                
            except Exception as e:
                logging.error(f"获取 {code} 实时数据失败: {e}")
                continue
        
        return arr

    @staticmethod
    def stk_factor(ts_code, date):
        """
        使用 akshare 获取技术指标（RSI、KDJ）
        注意：akshare 没有直接的指标接口，需要自己计算
        """
        try:
            pure_code = _convert_code_to_akshare(ts_code)
            
            # 获取足够的历史数据来计算指标
            end_date = f"{date[:4]}-{date[4:6]}-{date[6:8]}" if len(date) == 8 else date
            start_date_dt = datetime.strptime(date, '%Y%m%d') - timedelta(days=60)
            start_date = start_date_dt.strftime('%Y-%m-%d')
            
            df = ak.stock_zh_a_hist(symbol=pure_code, period="daily",
                                     start_date=start_date, end_date=end_date,
                                     adjust="qfq")
            
            if df is None or df.empty:
                return pd.DataFrame()
            
            # 计算 RSI
            df['close'] = df['收盘']
            df['rsi_6'] = IndexAnalysis._calculate_rsi(df['close'], 6)
            df['rsi_12'] = IndexAnalysis._calculate_rsi(df['close'], 12)
            df['rsi_24'] = IndexAnalysis._calculate_rsi(df['close'], 24)
            
            # 计算 KDJ
            kdj = IndexAnalysis._calculate_kdj(df['最高'], df['最低'], df['close'])
            df['kdj_j'] = kdj['J']
            
            df['ts_code'] = ts_code
            df['trade_date'] = pd.to_datetime(df['日期']).dt.strftime('%Y%m%d')
            
            # 返回指定日期的数据
            result = df[df['trade_date'] == date][['ts_code', 'trade_date', 'rsi_6', 'rsi_12', 'rsi_24', 'kdj_j']]
            return result
        except Exception as e:
            logging.error(f"获取技术指标失败: {e}")
            return pd.DataFrame()
    
    @staticmethod
    def _calculate_rsi(prices, period=14):
        """计算 RSI 指标"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    @staticmethod
    def _calculate_kdj(high, low, close, n=9, m1=3, m2=3):
        """计算 KDJ 指标"""
        lowest_low = low.rolling(window=n).min()
        highest_high = high.rolling(window=n).max()
        rsv = (close - lowest_low) / (highest_high - lowest_low) * 100
        
        k = rsv.ewm(com=m1-1, adjust=False).mean()
        d = k.ewm(com=m2-1, adjust=False).mean()
        j = 3 * k - 2 * d
        
        return pd.DataFrame({'K': k, 'D': d, 'J': j})

    @staticmethod
    def rt_min(stock_code, k_type=1, num=320):
        """
        获取分钟级别的K线数据
        使用腾讯接口（保持不变，因为这个接口很稳定）
        """
        code_part, exchange_part = stock_code.upper().split('.')
        sina_code = f"{exchange_part.lower()}{code_part}"
        stock_code = sina_code
        
        start = 10 ** (16 - 1)
        end = (10 ** 16) - 1
        random_num = str(randint(start, end))
        
        url = 'http://ifzq.gtimg.cn/appstock/app/kline/mkline?param=%s,m%s,,%s&_var=m%s_today&r=0.%s'
        url = url % (stock_code, k_type, num, k_type, random_num)

        context = ssl._create_unverified_context()
        content = urlopen(url=url, context=context, timeout=15).read().decode()

        content = content.split('=', maxsplit=1)[-1]
        content = json.loads(content)

        k_data = content['data'][stock_code]['m' + str(k_type)]
        df = pd.DataFrame(k_data)

        rename_dict = {0: 'candle_end_time', 1: 'open', 2: 'close', 3: 'high', 4: 'low', 5: 'amount'}
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
        使用 akshare 获取历史分钟数据
        """
        now = datetime.now()
        current_time = now.strftime('%H:%M:%S')
        
        # 获取今日分钟级数据
        today_df = IndexAnalysis.rt_min(stock_code, k_type=k_type, num=320)
        if today_df.empty:
            return None

        # 筛选今日开盘到当前时刻的数据
        today_df = today_df[today_df['candle_end_time'].dt.date == now.date()]
        today_df = today_df[today_df['candle_end_time'].dt.strftime('%H:%M') <= current_time]
        today_volume = today_df['amount'].sum()

        # 获取数据中的所有日期
        all_df = IndexAnalysis.rt_min(stock_code, k_type=k_type, num=320)
        unique_dates = all_df['candle_end_time'].dt.date.unique()
        
        if len(unique_dates) < 2:
            return None
        
        # 取上一个交易日
        last_trading_date = sorted(unique_dates)[-2]

        # 筛选上一个交易日相同时段的数据
        last_trading_df = all_df[all_df['candle_end_time'].dt.date == last_trading_date]
        last_trading_df = last_trading_df[last_trading_df['candle_end_time'].dt.strftime('%H:%M') <= current_time]
        last_trading_volume = last_trading_df['amount'].sum()

        if last_trading_volume == 0:
            return None
        return today_volume / last_trading_volume

    @staticmethod
    def my_pro_bar(stock_code, start_date=None, end_date=None):
        """
        使用 akshare 获取带均线的历史数据
        """
        cache_key = f"ma_{stock_code}_{start_date}_{end_date}"

        if cache_key in ma_cache:
            return ma_cache[cache_key]

        if start_date is None:
            start_date = Date_utils.get_date_by_step(Date_utils.get_today(replace=False), -130, True)
        if end_date is None:
            end_date = Date_utils.get_today(replace=True)

        try:
            pure_code = _convert_code_to_akshare(stock_code)
            
            # 转换日期格式
            start_date_fmt = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}" if len(start_date) == 8 else start_date
            end_date_fmt = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}" if len(end_date) == 8 else end_date
            
            # 根据代码判断是否为指数
            if stock_code[:3] in ['000', '399', '999']:
                # 指数数据
                df = ak.stock_zh_index_daily(symbol=f"sh{pure_code}" if stock_code.endswith('.SH') else f"sz{pure_code}")
            else:
                # 个股数据
                df = ak.stock_zh_a_hist(symbol=pure_code, period="daily",
                                         start_date=start_date_fmt, end_date=end_date_fmt,
                                         adjust="qfq")
            
            if df is None or df.empty:
                return None
            
            # 统一列名
            if '日期' in df.columns:
                df = df.rename(columns={
                    '日期': 'trade_date',
                    '开盘': 'open',
                    '收盘': 'close',
                    '最高': 'high',
                    '最低': 'low',
                    '成交量': 'vol',
                    '成交额': 'amount',
                })
            elif 'date' in df.columns:
                df = df.rename(columns={
                    'date': 'trade_date',
                    'volume': 'vol',
                })
            
            df['ts_code'] = stock_code
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y%m%d')
            
            # 计算均线
            df = df.sort_values('trade_date', ascending=True).reset_index(drop=True)
            for ma in [5, 10, 20, 30, 60, 120]:
                df[f'ma{ma}'] = df['close'].rolling(window=ma).mean()
            
            # 按日期降序排列
            df = df.sort_values('trade_date', ascending=False).reset_index(drop=True)
            
            ma_cache[cache_key] = df
            return df
        except Exception as e:
            logging.error(f"获取带均线的历史数据失败: {e}")
            return None

    @staticmethod
    def calculate_realtime_ma(df, current_price_dict=None, window_sizes=[5, 10, 20, 30, 60], date_col='trade_date'):
        """计算实时均线"""
        if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
            df[date_col] = pd.to_datetime(df[date_col])

        latest_date_in_data = df[date_col].iloc[0]

        use_current_price = False
        current_price = None

        if current_price_dict:
            try:
                current_price = float(current_price_dict['close'])
                current_timestamp = pd.to_datetime(current_price_dict['timestamp'])

                if current_timestamp.date() != latest_date_in_data.date():
                    use_current_price = True
            except KeyError:
                raise ValueError("current_price_dict must contain 'price' and 'timestamp' keys")

        ma_values = {}
        for window in window_sizes:
            if use_current_price:
                required_history = window - 1
                if len(df) >= required_history:
                    history_closes = df['close'].iloc[:required_history].values
                    all_closes = np.append(history_closes, current_price)
                    ma_value = np.mean(all_closes)
                else:
                    ma_value = None
            else:
                if len(df) >= window:
                    ma_value = df['close'].iloc[:window].mean()
                else:
                    ma_value = None

            ma_values[f'ma{window}'] = ma_value

        ma_values['used_current_price'] = use_current_price
        return ma_values


# 使用类进行分析
if __name__ == "__main__":
    # 测试实时行情
    v = IndexAnalysis.realtime_quote('000001.SZ')
    print(v)
