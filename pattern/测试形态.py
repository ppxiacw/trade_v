import pandas as pd
from  pattern.RedStar import RedStar
from config.tushare_utils import IndexAnalysis

large_cap_stocks = pd.read_csv('../files/stock_list_filter.csv',dtype={'symbol':str})

# large_cap_stocks = vs[vs['market_cap_billion'] > 100]['ts_code'].tolist()
large_cap_stocks = large_cap_stocks['ts_code'].tolist()

arr = []
batch_size = 20  # 每批处理10个代码

# 分批次调用接口
for i in range(0, len(large_cap_stocks), batch_size):
    batch = large_cap_stocks[i:i + batch_size]
    # 关键点：直接传入当前批次的 ts_code 数组
    quotes = IndexAnalysis.realtime_quote(','.join(f'{x}' for x in batch))
    for quote in quotes:
        if RedStar.valid(quote):
            print(quote.ts_code)

    arr.append(quotes)
print(len(arr))