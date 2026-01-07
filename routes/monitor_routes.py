"""
监控相关路由
"""
from flask import Blueprint, jsonify
from utils.tushare_utils import IndexAnalysis
from monitor.services.volume_radio import get_volume_ratio_simple

monitor_bp = Blueprint('monitor', __name__)


@monitor_bp.route('/rt_min')
def get_realtime_min():
    """获取实时分钟数据"""
    return jsonify({
        "000001.SH": "上证指数",
        "data": IndexAnalysis.rt_min('000001.SH', 1).to_dict(orient='records')
    })


@monitor_bp.route('/volume_ratio', methods=['GET'])
@monitor_bp.route('/volume_ratio/<string:stock_codes>', methods=['GET'])
def volume_ratio(stock_codes=None):
    """获取量比数据"""
    from app import config
    
    if stock_codes is None:
        stock_codes = list(config.CONFIG_LIST.keys())
    else:
        stock_codes = stock_codes.split(',')

    return get_volume_ratio_simple(stock_codes)


@monitor_bp.route('/ma', methods=['GET'])
@monitor_bp.route('/ma/<string:stock_codes>', methods=['GET'])
def calculate_ma_distances(stock_codes=None):
    """计算均线距离"""
    from app import config, alert_checker
    
    if stock_codes is None:
        stock_codes = list(config.CONFIG_LIST.keys())
    else:
        stock_codes = stock_codes.split(',')
    
    v = alert_checker.calculate_ma_distances(stock_codes)
    return v

