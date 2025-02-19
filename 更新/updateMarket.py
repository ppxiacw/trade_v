from config.tushare_utils import  IndexAnalysis
from pattern.NewHigh import new_high
import pandas as pd
import os
from  utils import StockAnalysis
current_path = os.path.abspath(__file__)

# 获取当前脚本的目录
dir_path = os.path.dirname(current_path)

# 获取当前脚本的上级目录
parent_dir_path = os.path.dirname(dir_path)

# 构造相对路径
relative_path = os.path.join(parent_dir_path, 'files')

def new_high_():
    analysis = StockAnalysis()
    df = IndexAnalysis.stk_limit(analysis.get_today().replace('-',''))

    # 1. 将字典转换为Series便于映射
    historical_high_series = pd.Series(new_high, name='historical_high')

    # 2. 将历史最高价合并到DataFrame
    df['historical_high'] = df['TS_CODE'].map(historical_high_series)

    # 3. 过滤出涨停价突破历史高点的股票
    result_df = df[
        (df['UP_LIMIT'] > df['historical_high']) &  # 涨停价突破历史高点
        df['historical_high'].notna()  # 排除没有历史数据的股票
        ]

    result_df.to_csv(f'{relative_path}/new_high.csv')

new_high_()