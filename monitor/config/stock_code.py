import re


_INDEX_NAME_KEYWORDS = ("指数", "中证", "上证", "深证", "创业板", "沪深")


def normalize_monitor_stock_code(stock_code, stock_name=""):
    """
    将监控股票代码统一转换为后缀格式: 000852.SH / 002050.SZ。
    兼容输入: sh000852 / 000852.SH / 000852
    """
    code = str(stock_code or "").strip()
    if not code:
        return ""

    name = str(stock_name or "")

    # 前缀格式: sh000852 / sz002050
    prefix_match = re.fullmatch(r"(?i)(sh|sz)\s*([0-9]{6})", code)
    if prefix_match:
        exchange = prefix_match.group(1).upper()
        pure = prefix_match.group(2)
        return f"{pure}.{exchange}"

    # 后缀格式: 000852.SH / 002050.SZ
    suffix_match = re.fullmatch(r"([0-9]{6})\.(sh|sz)", code, flags=re.IGNORECASE)
    if suffix_match:
        pure = suffix_match.group(1)
        exchange = suffix_match.group(2).upper()
        return f"{pure}.{exchange}"

    # 纯数字格式: 000852 / 002050 / 399006
    pure_match = re.fullmatch(r"([0-9]{1,6})", code)
    if pure_match:
        pure = pure_match.group(1).zfill(6)
        if pure.startswith("399"):
            exchange = "SZ"
        elif pure.startswith("000") and any(keyword in name for keyword in _INDEX_NAME_KEYWORDS):
            exchange = "SH"
        elif pure.startswith("6"):
            exchange = "SH"
        else:
            exchange = "SZ"
        return f"{pure}.{exchange}"

    # 不可识别时返回原值，避免阻断流程
    return code
