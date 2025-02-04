import adata
import pandas as pd
import json
from config.tushare_utils import IndexAnalysis

start_year = 2015
market_info_dict = {}
# 红三兵
red_day = 3
#第n天卖出
sale_day =3
for i in range(start_year, 2024 + 1):
    with open(f'C:\\Users\\曹威\\Desktop\\market\\market_{i}.csv') as f:
        market_info_dict[i] = pd.read_csv(f, dtype={'stock_code': str})
    print(i)


def findStock(stock_code, year):
    # 使用布尔索引进行多条件过滤
    global arr

    df = market_info_dict[year][(market_info_dict[year]['stock_code'] == stock_code)]
    result = df.to_dict('records')
    arr = []
    i = 0
    volume = 0
    for index, v in enumerate(result):
        # if v['close'] > v['open'] and v["change_pct"] < 5 and result[index]["high"] > result[index - 1]["high"]:
        if v['close'] > v['open'] and result[index]["high"] > result[index - 1]["high"]:
            if i == 0 or (i != 0 and v['volume'] > volume):
                volume = v['volume']
                i = i + 1
            else:
                i = 1
                volume = v['volume']
            if i == red_day and (index + sale_day) < len(result) and result[index + 1]['open'] != 0 :
                # sz_pct = calculate_pct(result[index + 1]['trade_date'].replace('-',''),result[index + sale_day]['trade_date'].replace('-',''))
                value = {
                    "index":index,
                    "stock_code": stock_code,
                    "trade_date": v["trade_date"],
                    "change_pct": (result[index + sale_day]['close'] - result[index + 1]['open']) / result[index + 1]['open'] * 100,
                    # "sz_pct":sz_pct
                }
                arr.append(value)
        else:
            i = 0
            volume = 0

    return arr


df = IndexAnalysis().pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date,market')


def fallback():
    all_arr = []
    all_sum_pct = 0
    all_count = 0
    all_sz_pct = {}
    for i, v in df.iterrows():
        # if i<0:
        #     continue
        symbol = v['symbol']
        print(symbol)
        if symbol.startswith("60") or symbol.startswith("00"):
            sum_pct = 0
            count = 0
            for year in range(start_year, 2024 + 1):
                b = findStock(symbol, year)
                if len(b) > 0:
                    all_arr.extend(b)
                    for item in b:
                        count = count + 1
                        sum_pct = sum_pct + item["change_pct"]
                        all_count = all_count + 1
                        all_sum_pct = all_sum_pct + item["change_pct"]
                        # all_sz_pct[item['trade_date']] =  item["sz_pct"]
                        print(f'{symbol},{item}')
                    average_a = sum(item["change_pct"] for item in b) / len(b)
                    print(f'{year},{average_a}')
            if count > 0:
                print(f'{symbol},{sum_pct / count}')
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(all_arr, f, ensure_ascii=False, indent=4)
        # 0.25339832697336706,20124.388331570866,79418
    print(f'{all_sum_pct / all_count},{all_sum_pct},{all_count}')


def limit_every_day(index,result):
    v = result[index]
    return v['close'] > v['open'] and v["change_pct"] < 5 and result[index]["high"] > result[index - 1]["high"]


def limit_last_day(index,result):
    v = result[index]
    return v['close'] > v['open'] and v["change_pct"] < 5 and result[index]["high"] > result[index - 1]["high"]


fallback()
