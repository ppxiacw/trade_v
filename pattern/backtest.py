import adata
import pandas as pd

start_year = 2015
market_info_dict = {}
for i in range(start_year,2024+1):
    with open(f'C:\\Users\\曹威\\Desktop\\market\\market_{i}.csv')as f:
        market_info_dict[i] = pd.read_csv(f, dtype={'stock_code': str})
        print(i)

def findStock(stock_code, year):
    # 使用布尔索引进行多条件过滤
    df = market_info_dict[year][(market_info_dict[year]['stock_code'] == stock_code)]
    result = df.to_dict('records')
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
            if i == 3 and (index+3)<len(result) and v["change_pct"]<5  :
                value = {
                    "stock_code":stock_code,
                    "trade_date":v["trade_date"],
                    "change_pct":(result[index+3]['close']-result[index+1]['open'])/result[index+1]['open']*100
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
        if  v['stock_code'].startswith("68") or  v['stock_code'].startswith("30"):
            continue
        if i>600:
            break
        sum_pct = 0
        count = 0
        for year in range(start_year,2024+1):
            b = findStock(v['stock_code'], year)
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
    print(f'{all_sum_pct/all_count},{all_sum_pct},{all_count}')





fallback()


