
def calculate_rsi_tonghuashun(data, period=14):
    """
    计算与同花顺匹配的RSI值（使用Wilder平滑方法）

    参数:
    data: DataFrame, 包含价格数据，必须有'close'列
    period: int, RSI计算周期（6或12）

    返回:
    float: 最新一条数据的RSI值
    """
    # 确保有足够的数据点
    if len(data) < period + 1:
        return None

    # 复制数据以避免修改原始DataFrame
    df = data.copy()

    # 计算价格变化
    delta = df['close'].diff()

    # 分离上涨和下跌
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    # 计算初始平均值（前period个周期的简单平均）
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()

    # 使用Wilder平滑方法计算后续平均值
    for i in range(period, len(df)):
        avg_gain.iloc[i] = (avg_gain.iloc[i - 1] * (period - 1) + gain.iloc[i]) / period
        avg_loss.iloc[i] = (avg_loss.iloc[i - 1] * (period - 1) + loss.iloc[i]) / period

    # 计算相对强度(RS)
    rs = avg_gain / avg_loss

    # 计算RSI
    rsi = 100 - (100 / (1 + rs))

    return rsi.iloc[-1]  # 返回最新值


def get_tonghuashun_rsi_values(data):
    """
    计算同花顺风格的RSI6和RSI12值

    参数:
    data: DataFrame, 包含价格数据，必须有'close'列

    返回:
    dict: 包含RSI6和RSI12值的字典
    """
    # 计算RSI6
    rsi6 = calculate_rsi_tonghuashun(data, period=6)

    # 计算RSI12
    rsi12 = calculate_rsi_tonghuashun(data, period=12)

    return {
        "RSI6": round(rsi6, 2) if rsi6 is not None else None,
        "RSI12": round(rsi12, 2) if rsi12 is not None else None
    }