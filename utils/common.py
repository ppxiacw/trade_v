def format_stock_code(code, target_format='prefix'):
    """
    格式化股票/场内ETF代码

    Args:
        code: 输入的代码，支持格式：
              - 前缀式: sz000001 (深圳股票), sh510300 (上海ETF), sz159919 (深圳ETF)
              - 纯数字: 000001, 510300, 159919
              - 后缀式: 000001.sz, 510300.sh, 159919.sz
        target_format: 目标格式，取值为：
                       - 'prefix': sz000001 或 sh510300 (默认)
                       - 'pure': 000001 或 510300
                       - 'suffix': 000001.sz 或 510300.sh

    Returns:
        格式化后的代码字符串

    Raises:
        ValueError: 当输入代码格式无法识别或不受支持时
    """
    # 交易所映射 (扩展说明，不影响逻辑)
    exchange_map = {
        'sh': '上海交易所',
        'sz': '深圳交易所'
    }

    # 核心修改：增强的代码->交易所映射规则
    # 规则优先级：ETF > 股票
    def get_exchange_by_number(number_part):
        """
        根据纯数字代码判断交易所
        规则：
        1. 先判断ETF：沪市ETF(51,56,58...), 深市ETF(15,16,159...)
        2. 再判断股票：沪股(6...), 深股(0,3...)
        """
        # 确保是字符串并补齐到至少6位以方便判断
        num_str = str(number_part).zfill(6)

        # 1. 判断沪市ETF (通常以51, 56, 58开头)
        if num_str[:2] in ['51', '56', '58']:
            return 'sh'
        # 2. 判断深市ETF (通常以15, 16, 159开头)
        elif num_str[:2] in ['15', '16'] or num_str[:3] == '159':
            return 'sz'
        # 3. 判断沪市股票 (以6开头)
        elif num_str[0] == '6':
            return 'sh'
        # 4. 判断深市股票 (以0或3开头)
        elif num_str[0] in ['0', '3']:
            return 'sz'
        # 5. 其他情况（如B股、新股等）可根据需要扩展，这里默认返回sz
        else:
            # 对于无法识别的代码，可以根据业务需求调整默认行为
            # 这里保守地返回深圳交易所，或可以选择抛出异常
            return 'sz'  # 或 raise ValueError(f"无法识别的代码格式: {number_part}")

    # 提取数字部分和交易所标识
    number_part = ''
    exchange = ''

    # 处理输入代码
    code = str(code).strip()  # 去除空白字符

    if '.' in code:  # 格式: 000001.sz 或 510300.sh
        parts = code.split('.')
        number_part = parts[0]
        exchange = parts[1].lower()
    elif code[:2].lower() in ['sz', 'sh']:  # 格式: sz000001 或 sh510300
        exchange = code[:2].lower()
        number_part = code[2:]
    else:  # 格式: 纯数字 000001 或 510300
        number_part = code
        # 使用新规则判断交易所
        exchange = get_exchange_by_number(number_part)

    # 确保number_part是6位数字（不足6位前面补0）
    number_part = str(number_part).zfill(6)

    # 验证交易所代码
    if exchange not in ['sh', 'sz']:
        raise ValueError(f"无效的交易所代码: {exchange}")

    # 根据目标格式返回结果
    if target_format == 'prefix':
        return f"{exchange}{number_part}"
    elif target_format == 'pure':
        return number_part
    elif target_format == 'suffix':
        return f"{number_part}.{exchange}"
    else:
        raise ValueError(f"不支持的格式: {target_format}，请使用 'prefix'、'pure' 或 'suffix'")


# 测试示例
if __name__ == "__main__":
    test_cases = [
        # (输入代码, 目标格式, 期望输出)
        ('510300', 'prefix', 'sh510300'),  # 沪深300 ETF (沪市)
        ('159919', 'prefix', 'sz159919'),  # 沪深300 ETF (深市)
        ('588000', 'prefix', 'sh588000'),  # 科创50 ETF
        ('159915', 'suffix', '159915.sz'),  # 创业板 ETF
        ('sh510500', 'pure', '510500'),  # 中证500 ETF
        ('sz159995', 'pure', '159995'),  # 芯片ETF
        ('000001', 'prefix', 'sz000001'),  # 平安银行 (深市股票)
        ('600000', 'prefix', 'sh600000'),  # 浦发银行 (沪市股票)
        ('300750', 'prefix', 'sz300750'),  # 宁德时代 (创业板股票)
    ]

    print("测试结果：")
    for input_code, fmt, expected in test_cases:
        result = format_stock_code(input_code, fmt)
        print(result)