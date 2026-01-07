"""
股票相关路由
"""
from flask import Blueprint, jsonify
from config.dbconfig import exeQuery
from utils.common import format_stock_code

stock_bp = Blueprint('stock', __name__)


@stock_bp.route('/get_stock_list')
def get_stock_list():
    """获取股票列表"""
    stocks = "select * from stocks order by id desc"
    result = exeQuery(stocks)

    for stock in result:
        stock['stock_code'] = format_stock_code(stock['stock_code'], 'prefix')
    return jsonify(result)


@stock_bp.route('/reload_config')
def reload_config():
    """重新加载配置"""
    from app import config, stock_data
    config.reload_config()
    stock_data.initialize_data_storage()
    return jsonify({'success': True, 'message': '配置重载成功'})

