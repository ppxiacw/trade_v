import adata

import dbconfig
import pandas as pd



def findStock(stock_code, year,start_date=None,end_date=None):
    query = (f'select * from market_{year} where stock_code = "{stock_code}"  and trade_date>="{start_date}"   and trade_date<="{end_date}"' )

    df = pd.read_sql(query, dbconfig.engine)
    result = df.to_dict('records')  # 将 DataFrame 转换为字典列表WWeeeeeeee
    arr = []
    prt_dict = {}
    i = 0
    volume = 0
    for index,v in enumerate(result):
        # if v["stock_code"] != '600250':
        #     continue
        if v['close'] > v['open']  :
            if i == 0 or ( i != 0 and v['volume'] > volume):
                volume = v['volume']
                i = i + 1
            else:
                i = 1
                volume = v['volume']
                # volume = 0
            # if i == 2 and (index+2)<len(result) and v["change_pct"]<9.9:
            if i == 3 and (index+2)<len(result) and v["change_pct"]<9.9 :
                value = {
                    "stock_code":stock_code,
                    "trade_date":v["trade_date"],
                    # "change_pct":result[index+1]["change_pct"]+result[index+2]["change_pct"]  ### 第三天尾盘买 拿两天
                    # "change_pct":result[index+1]["change_pct"]+result[index+2]["change_pct"]+result[index+3]["change_pct"]### 第三天尾盘买 拿三天
                    "change_pct":(result[index+2]['close']-result[index+1]['open'])/result[index+1]['open']*100

                }
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
        # if v['stock_code'] != '000029':
        #     continue
        if i>600:
            break
        if v['stock_code'].startswith("60") or v['stock_code'].startswith("00") :
            sum_pct = 0
            count = 0
            for year in range(2020,2024+1):
                b = findStock(v['stock_code'], year, f'{year}-01-01', f'{year}-12-31')
                if len(b)>0:
                    for item in b:
                        count = count+1
                        sum_pct = sum_pct+item["change_pct"]
                        all_count = all_count+1
                        all_sum_pct = all_sum_pct+item["change_pct"]
                        print(f'{v['stock_code']},{item}')
                    average_a = sum(item["change_pct"] for item in b) / len(b)
                    print(f'{year},{average_a}')
            if count>0:
                print(f'{v['stock_code']},{sum_pct/count}')
    print(f'sss{all_sum_pct/all_count},{all_sum_pct},{all_count}')



def find():
    for i, v in df.iterrows():
        b = findStock(v['stock_code'], 2024, '2024-12-31', '2025-01-03')
        if (len(b) > 0):
            print(f'{v['stock_code']},{b}')

fallback()




####  "change_pct":result[index+1]["change_pct"]+result[index+2]["change_pct"]  ### 第三天尾盘买 拿两天  0.16
####  "change_pct":result[index+1]["change_pct"]+result[index+2]["change_pct"]  ### 第三天尾盘买,第二天的成交量不要求放大 拿两天  0.18
# "change_pct": result[index + 1]["change_pct"] + result[index + 2]["change_pct"] + result[index + 3]["change_pct"]  ### 第三天尾盘买 拿三天  0.33
