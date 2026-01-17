"""
技术指标相关路由
"""
import re
import json
import requests
import pandas as pd
from flask import Blueprint, request, jsonify
from utils.IndicatorCalculation import IndicatorCalculation

indicator_bp = Blueprint('indicator', __name__)


def parse_jsonp_response(text: str) -> dict:
    """
    解析 JSONP 响应
    格式: kline_m5={"code":0,"data":...}
    """
    try:
        # 尝试直接解析为 JSON
        return json.loads(text)
    except:
        pass
    
    # 解析 JSONP 格式: varname={...}
    match = re.search(r'=\s*(\{.*\})\s*;?\s*$', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except:
            pass
    
    return None


def fetch_kline_data(stock_code: str, period: str = 'm30', count: int = 100):
    """
    获取K线数据
    
    Args:
        stock_code: 股票代码 (如 sh600519, sz000001)
        period: 周期 (m1, m5, m15, m30, day, week, month)
        count: 获取数据条数
    
    Returns:
        DataFrame with columns: time, open, close, high, low, volume
    """
    # 格式化股票代码
    if stock_code.startswith('sh') or stock_code.startswith('sz'):
        formatted_code = stock_code
    else:
        # 纯数字，判断交易所
        if stock_code.startswith('6'):
            formatted_code = f'sh{stock_code}'
        else:
            formatted_code = f'sz{stock_code}'
    
    # 根据周期类型选择接口
    if period in ['day', 'week', 'month']:
        # 日/周/月使用 fqkline 接口
        url = f'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={formatted_code},{period},,,{count},qfq&_var=kline_{period}'
        kline_key = f'qfq{period}'
    else:
        # 分钟级别使用 mkline 接口
        url = f'https://ifzq.gtimg.cn/appstock/app/kline/mkline?param={formatted_code},{period},,{count}&_var=kline_{period}'
        kline_key = period
    
    try:
        response = requests.get(url, timeout=10)
        text = response.text
        
        # 解析 JSONP 响应
        data = parse_jsonp_response(text)
        
        if not data or data.get('code') != 0:
            print(f"接口返回错误: {data}")
            return None
        
        stock_data = data.get('data', {}).get(formatted_code)
        if not stock_data:
            print(f"未找到股票数据: {formatted_code}")
            return None
        
        kline_data = stock_data.get(kline_key)
        if not kline_data:
            # 尝试其他key
            kline_data = stock_data.get(period)
        
        if not kline_data or not isinstance(kline_data, list):
            print(f"未找到K线数据，可用keys: {stock_data.keys()}")
            return None
        
        # 转换为DataFrame
        # K线数据格式可能有多种：6列、7列、8列甚至更多
        # 日K线格式: [time, open, close, high, low, volume, amount, turnover, ...]
        # 分钟线格式: [time, open, close, high, low, volume, ...]
        # 找出数据中最大的列数
        max_cols = max(len(row) for row in kline_data) if kline_data else 0
        
        # 定义所有可能的列名（增加更多以防数据列更多）
        all_columns = ['time', 'open', 'close', 'high', 'low', 'volume', 'amount', 'turnover', 'extra1', 'extra2']
        columns = all_columns[:max_cols]
        
        # 确保每行数据列数一致
        normalized_data = []
        for row in kline_data:
            if len(row) < max_cols:
                row = list(row) + [None] * (max_cols - len(row))
            normalized_data.append(row[:max_cols])
        
        df = pd.DataFrame(normalized_data, columns=columns)
        
        # 确保数值类型
        for col in ['open', 'close', 'high', 'low', 'volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
        
    except Exception as e:
        print(f"获取K线数据失败: {e}")
        import traceback
        traceback.print_exc()
        return None


@indicator_bp.route('/rsi/test/<string:stock_code>', methods=['GET'])
def test_kline(stock_code):
    """测试K线数据获取"""
    period = request.args.get('period', 'm30')
    
    # 格式化股票代码
    if stock_code.startswith('sh') or stock_code.startswith('sz'):
        formatted_code = stock_code
    else:
        if stock_code.startswith('6'):
            formatted_code = f'sh{stock_code}'
        else:
            formatted_code = f'sz{stock_code}'
    
    # 构建URL
    url = f'https://ifzq.gtimg.cn/appstock/app/kline/mkline?param={formatted_code},{period},,100&_var=kline_{period}'
    
    try:
        response = requests.get(url, timeout=10)
        text = response.text[:500]  # 只返回前500字符
        
        return jsonify({
            'url': url,
            'status_code': response.status_code,
            'response_preview': text,
            'formatted_code': formatted_code
        })
    except Exception as e:
        return jsonify({'error': str(e)})


@indicator_bp.route('/rsi', methods=['GET'])
@indicator_bp.route('/rsi/<string:stock_code>', methods=['GET'])
def get_rsi(stock_code=None):
    """
    获取股票RSI值（精简版）
    
    参数:
        stock_code: 股票代码 (路径参数或查询参数)
        period: K线周期，默认 m30 (查询参数)
    
    返回:
        {
            "rsi": 45.32  // RSI6值
        }
    """
    try:
        # 获取参数
        if not stock_code:
            stock_code = request.args.get('stock_code')
        
        if not stock_code:
            return jsonify({'error': '缺少股票代码参数'}), 400
        
        period = request.args.get('period', 'm30')
        
        # 获取K线数据
        df = fetch_kline_data(stock_code, period, count=100)
        
        if df is None or len(df) < 15:
            return jsonify({'error': f'获取K线数据失败'}), 400
        
        # 计算RSI
        rsi_values = IndicatorCalculation.get_rsi_values(df)
        
        rsi6 = rsi_values.get('RSI6')
        
        # 转换为 Python 原生类型（避免 numpy 类型序列化问题）
        rsi6 = float(rsi6) if rsi6 is not None else None
        
        return jsonify({
            'rsi': rsi6
        })
        
    except Exception as e:
        return jsonify({'error': f'计算RSI失败: {str(e)}'}), 500


@indicator_bp.route('/daily-condition/<string:stock_code>', methods=['GET'])
def check_daily_condition(stock_code):
    """
    检查日线是否良好（前20交易日内是否出现过放量阴线）
    
    参数:
        stock_code: 股票代码 (路径参数)
        days: 检查的交易日数量，默认20 (查询参数)
    
    返回:
        {
            "is_good": true/false,  // 日线是否良好（没有放量阴线为true）
            "volume_bars": [...]    // 放量阴线详情（如果有）
        }
    
    判断逻辑:
        - 阴线：收盘价 < 开盘价
        - 放量：阴线的成交量 > 前5个交易日内任意一根阳线的成交量
    """
    try:
        days = int(request.args.get('days', 20))
        lookback = int(request.args.get('lookback', 5))  # 向前查找的交易日数
        
        # 获取日K线数据（多获取一些用于找之前的阳线）
        df = fetch_kline_data(stock_code, 'day', count=days + 20)
        
        if df is None or len(df) < days:
            return jsonify({'error': '获取日K线数据失败'}), 400
        
        # 判断阴线阳线
        # 阳线：收盘价 >= 开盘价
        # 阴线：收盘价 < 开盘价
        df['is_positive'] = df['close'] >= df['open']
        df['is_negative'] = df['close'] < df['open']
        
        # 获取前lookback个交易日内阳线的最小成交量
        # 对于每根K线，找到它前lookback个交易日内阳线中的最小成交量
        min_prev_positive_volume = []
        for i in range(len(df)):
            # 向前查找lookback个交易日内的阳线最小成交量
            min_volume = None
            start_idx = max(0, i - lookback)
            for j in range(i - 1, start_idx - 1, -1):
                if df.iloc[j]['is_positive']:
                    vol = df.iloc[j]['volume']
                    if min_volume is None or vol < min_volume:
                        min_volume = vol
            min_prev_positive_volume.append(min_volume)
        
        df['min_prev_positive_volume'] = min_prev_positive_volume
        
        # 只检查最近days天的数据
        df_check = df.tail(days).copy()
        
        # 判断放量阴线：阴线 且 成交量 > 前lookback个交易日内阳线的最小成交量
        df_check['is_volume_negative'] = (
            df_check['is_negative'] & 
            df_check['min_prev_positive_volume'].notna() &
            (df_check['volume'] > df_check['min_prev_positive_volume'])
        )
        
        # 找出所有放量阴线
        volume_bars = df_check[df_check['is_volume_negative']][
            ['time', 'open', 'close', 'volume', 'min_prev_positive_volume']
        ].to_dict('records')
        
        # 转换为原生类型
        for bar in volume_bars:
            bar['open'] = float(bar['open'])
            bar['close'] = float(bar['close'])
            bar['volume'] = float(bar['volume'])
            bar['min_prev_positive_volume'] = float(bar['min_prev_positive_volume']) if bar['min_prev_positive_volume'] else None
        
        is_good = len(volume_bars) == 0
        
        return jsonify({
            'is_good': is_good,
            'volume_bars': volume_bars,
            'checked_days': days
        })
        
    except Exception as e:
        return jsonify({'error': f'检查日线条件失败: {str(e)}'}), 500


@indicator_bp.route('/rsi/batch', methods=['POST'])
def get_rsi_batch():
    """
    批量获取多个股票的RSI值
    
    请求体:
        {
            "stock_codes": ["sh600519", "sz000001"],
            "period": "m30"
        }
    """
    try:
        data = request.get_json()
        stock_codes = data.get('stock_codes', [])
        period = data.get('period', 'm30')
        
        if not stock_codes:
            return jsonify({'success': False, 'message': '缺少股票代码列表'}), 400
        
        results = {}
        for stock_code in stock_codes:
            df = fetch_kline_data(stock_code, period, count=100)
            
            if df is not None and len(df) >= 15:
                rsi_values = IndicatorCalculation.get_rsi_values(df)
                rsi6 = rsi_values.get('RSI6')
                rsi12 = rsi_values.get('RSI12')
                
                # 转换为 Python 原生类型
                rsi6 = float(rsi6) if rsi6 is not None else None
                rsi12 = float(rsi12) if rsi12 is not None else None
                condition_met = bool(rsi6 < 20 or rsi6 > 80) if rsi6 is not None else False
                
                results[stock_code] = {
                    'rsi6': rsi6,
                    'rsi12': rsi12,
                    'condition_met': condition_met
                }
            else:
                results[stock_code] = {
                    'rsi6': None,
                    'rsi12': None,
                    'condition_met': False,
                    'error': '数据获取失败'
                }
        
        return jsonify({
            'success': True,
            'data': results,
            'period': period
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': f'批量计算RSI失败: {str(e)}'}), 500

