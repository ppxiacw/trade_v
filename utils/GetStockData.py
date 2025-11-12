import akshare as ak
import json
from monitor.config.db_monitor import exe_query


def convert_code_format(code):
    """
    将股票代码转换为带交易所后缀的格式
    规则：
    - 6开头 -> .SH (上交所)
    - 0或3开头 -> .SZ (深交所)
    """
    if code.startswith('6'):
        return f"{code}.SH"
    elif code.startswith('0') or code.startswith('3'):
        return f"{code}.SZ"
    else:
        # 其他情况，默认返回深交所格式
        return f"{code}.SZ"


try:
    # 使用 akshare 获取A股代码和名称
    result_df = ak.stock_info_a_code_name()

    # 创建以转换后的ts_code为键的字典，保持与之前代码兼容的结构
    records = result_df.to_dict(orient='records')
    result_dict = {}

    for record in records:
        # 获取原始代码和名称
        original_code = record['code']
        name = record['name']

        # 转换为带后缀的格式
        ts_code = convert_code_format(original_code)

        # 构建结果字典
        result_dict[ts_code] = {
            'ts_code': ts_code,
            'symbol': original_code,  # 保持原始代码格式
            'name': name
        }

    # 保持与之前代码的兼容性
    result = result_dict

except Exception as e:
    print(f"发生错误: {str(e)}")
    print(json.dumps({"error": str(e)}, indent=4))
    result_dict = {}
    result = {}


def get_stock_name(stock_code):
    try:
        value = exe_query('select * from stocks')
        stock_list = {item['stock_code']: item for item in value}
        # 优先尝试从 config 中获取股票名称
        if stock_code in stock_list:
            name = stock_list[stock_code].get('stock_name')
            if name:  # 如果有值则返回
                return name

        # 如果 config 中没有，尝试从 result_dict 中获取
        # 注意：这里我们同时支持原始代码格式和带后缀格式
        if stock_code in result_dict:
            name = result_dict[stock_code].get('name')
            if name:  # 如果有值则返回
                return name
        else:
            # 如果直接查找失败，尝试转换格式后再查找
            converted_code = convert_code_format(stock_code)
            if converted_code in result_dict:
                name = result_dict[converted_code].get('name')
                if name:  # 如果有值则返回
                    return name

        return stock_code
    except Exception as e:
        return stock_code

print(get_stock_name('000001.SZ'))