import adata

import dbconfig
import pandas as pd

import utils
ut = utils.StockAnalysis()
today = ut.get_today()
yesterday = ut.get_date_by_step(ut.get_today(),-1)
query = (f'select * from market_2025 where  trade_date>="2025-01-01"   and trade_date<="{today}"')

vdf = pd.read_sql(query, dbconfig.engine)
# result = df.to_dict('records')
def findStock(stock_code,step=0):
  # 将 DataFrame 转换为字典列表WWeeeeeeee
    arr = []
    i = 0
    volume = 0
    result =vdf[vdf['stock_code']==stock_code].to_dict('records')
    for index,v in enumerate(result):

        if v['close'] > v['open'] and  v["change_pct"]<5:
            if i == 0 or ( i != 0 and v['volume'] > volume):
                volume = v['volume']
                i = i + 1
            else:
                i = 1
                volume = v['volume']
                # volume = 0
            if i == 3 and (index+step)<len(result) and v["change_pct"]<5  :
                value = {
                    "stock_code":stock_code,
                    "trade_date":v["trade_date"],
                    "change_pct":(result[index+step]['close']-result[index+step]['open'])/result[index+step]['open']*100
                }
                if step ==0 and str(v["trade_date"])==today:
                    arr.append(value)
                if step ==1 and str(v["trade_date"])==yesterday:
                    arr.append(value)
        else:
            i = 0
            volume = 0
    return arr

df = adata.stock.info.all_code()


def fallback():
    all_sum_pct = 0
    all_count = 0
    all_arr = []
    for i, v in df.iterrows():
        print(i)
        if v['stock_code'].startswith("60") or v['stock_code'].startswith("00") :
            sum_pct = 0
            count = 0
            for year in range(2025,2025+1):
                b = findStock(v['stock_code'],1)
                if len(b)>0:
                    for item in b:
                        count = count+1
                        sum_pct = sum_pct+item["change_pct"]
                        all_count = all_count+1
                        all_sum_pct = all_sum_pct+item["change_pct"]
                    all_arr =all_arr+b
            if count>0:
                pass
                # print(f'{v['stock_code']},{sum_pct/count}')
    for i, e in enumerate(all_arr):
        all_arr[i] = e | utils.StockAnalysis().find_stock_info(e['stock_code']).to_dict()
    sorted_arr = sorted(all_arr, key=lambda x: float(x['change_pct']),reverse=True)
    for s_v in  sorted_arr:
        print(s_v)
    print(f'sss{all_sum_pct/all_count},{all_sum_pct},{all_count}')



def find():
    all_sum_pct = 0
    all_count = 0
    for i, v in df.iterrows():
        if not v['stock_code'].startswith("68") and not v['stock_code'].startswith("30") and not v['stock_code'].startswith("8"):
            b = findStock(v['stock_code'],0)
            if len(b) > 0:
                for item in b:
                    all_count = all_count + 1
                    all_sum_pct = all_sum_pct + item["change_pct"]
                    print(f'{v['stock_code']},{item}')
    print(f'sss{all_sum_pct / all_count},{all_sum_pct},{all_count}')

# #找出今日红三兵
# find()

# 回测昨日红三兵
fallback()
