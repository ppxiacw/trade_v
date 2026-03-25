import akshare as ak
import pandas as pd

# 设置 pandas 显示全部数据，不省略
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

def fetch_concept_stocks(concept_name):
    """
    获取指定股票概念的成分股（东方财富接口）
    参数: concept_name - 概念名称，如"机器人"、"人工智能"等
    """
    try:
        stocks_df = ak.stock_board_concept_cons_em(symbol=concept_name)
        print(f"成功获取 '{concept_name}' 概念，共 {len(stocks_df)} 只股票。")
        return stocks_df
    except Exception as e:
        print(f"获取失败，错误信息：{e}")
        return None

# ========== 使用范例 ==========

# 1. 获取所有东方财富概念板块列表
print("【获取所有概念板块】")
concept_df = ak.stock_board_concept_name_em()
print(concept_df)

# 2. 通过概念名称获取成分股
print("\n【通过概念名称获取成分股】")
concept_stocks = fetch_concept_stocks("智能电网")
if concept_stocks is not None:
    print(concept_stocks[['代码', '名称']])
pd.set_option('display.max_rows', None)

# 获取指定基金/ETF的持仓股票
# symbol: 基金代码（如 "159995" 是芯片ETF）
# date: 持仓日期（格式 "2024"）
df = ak.fund_portfolio_hold_em(symbol="159326", date="2025")
print(df)