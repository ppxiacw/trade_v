import adata

import dbconfig
import pandas as pd

import utils


def findStock(stock_code, year,start_date=None,end_date=None,step=0):
    query = (f'select * from market_{year} where stock_code = "{stock_code}"  and trade_date>="{start_date}"   and trade_date<="{end_date}"' )

    df = pd.read_sql(query, dbconfig.engine)
    result = df.to_dict('records')  # 将 DataFrame 转换为字典列表WWeeeeeeee
    arr = []
    i = 0
    volume = 0
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
                if step ==0 and str(v["trade_date"])==utils.get_today():
                    arr.append(value)
                if step ==1 and str(v["trade_date"])=='2025-01-07':
                    arr.append(value)
        else:
            i = 0
            volume = 0
    return arr

df = adata.stock.info.all_code()


def fallback():
    all_sum_pct = 0
    all_count = 0
    for i, v in df.iterrows():
        if v['stock_code'].startswith("60") or v['stock_code'].startswith("00") :
            sum_pct = 0
            count = 0
            for year in range(2025,2025+1):
                b = findStock(v['stock_code'], year, f'2025-01-01', utils.get_today(),1)
                if len(b)>0:
                    for item in b:
                        count = count+1
                        sum_pct = sum_pct+item["change_pct"]
                        all_count = all_count+1
                        all_sum_pct = all_sum_pct+item["change_pct"]
                        print(f'{v['stock_code']},{item}')
            if count>0:
                print(f'{v['stock_code']},{sum_pct/count}')
    print(f'sss{all_sum_pct/all_count},{all_sum_pct},{all_count}')



def find():
    all_sum_pct = 0
    all_count = 0
    for i, v in df.iterrows():
        if not v['stock_code'].startswith("68") and not v['stock_code'].startswith("30") and not v['stock_code'].startswith("8"):
            b = findStock(v['stock_code'], 2025, '2025-01-02', utils.get_today(),0)
            if len(b) > 0:
                for item in b:
                    all_count = all_count + 1
                    all_sum_pct = all_sum_pct + item["change_pct"]
                    print(f'{v['stock_code']},{item}')
    print(f'sss{all_sum_pct / all_count},{all_sum_pct},{all_count}')

#找出今日红三兵
find()

#回测昨日红三兵
# fallback()
