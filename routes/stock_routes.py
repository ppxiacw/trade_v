"""
股票相关路由
"""
import logging

from flask import Blueprint, jsonify, request
from config.dbconfig import exeQuery
from utils.common import format_stock_code
from services.stock_screen_service import screen_stocks_by_mv_and_pct

stock_bp = Blueprint('stock', __name__)
_logger = logging.getLogger(__name__)


@stock_bp.route('/get_stock_list')
def get_stock_list():
    """获取股票列表"""
    stocks = "select * from stocks order by id desc"
    result = exeQuery(stocks)

    for stock in result:
        stock['stock_code'] = format_stock_code(stock['stock_code'], 'prefix')
    return jsonify(result)


@stock_bp.route('/screen/mv_pct', methods=['GET'])
def screen_mv_pct():
    """
    实时筛选：总市值 >= 指定亿元，且涨跌幅 >= 指定 %（腾讯 qt.gtimg.cn，与 K 线腾讯源一致）。
    查询参数：
      min_mv_yi: 最小总市值（亿元），默认 50
      min_pct_chg: 最小涨跌幅（%），默认 0
      limit: 最大返回条数，默认 3000，最大 8000
    """
    try:
        min_mv_yi = request.args.get('min_mv_yi', default=50.0, type=float)
        min_pct_chg = request.args.get('min_pct_chg', default=0.0, type=float)
        limit = request.args.get('limit', default=3000, type=int)

        if min_mv_yi < 0:
            return jsonify({'success': False, 'message': 'min_mv_yi 不能为负数'}), 400
        if limit < 1 or limit > 8000:
            return jsonify({'success': False, 'message': 'limit 需在 1～8000 之间'}), 400

        data, meta = screen_stocks_by_mv_and_pct(
            min_mv_yi=min_mv_yi,
            min_pct_chg=min_pct_chg,
            limit=limit,
        )
        return jsonify({
            'success': True,
            'data': data,
            'count': len(data),
            'meta': meta,
            'params': {
                'min_mv_yi': min_mv_yi,
                'min_pct_chg': min_pct_chg,
                'limit': limit,
            },
        })
    except Exception as e:
        _logger.exception('screen_mv_pct 失败')
        hint = (
            '筛选数据拉取失败（当前为腾讯 qt.gtimg.cn，与 K 线日 K 同属腾讯源）；'
            '多为网络波动或代理问题，请稍后重试。'
        )
        return jsonify({
            'success': False,
            'message': f'{hint} 详情: {str(e)}',
        }), 503


@stock_bp.route('/reload_config')
def reload_config():
    """重新加载配置"""
    from app import config, stock_data
    config.reload_config()
    stock_data.initialize_data_storage()
    return jsonify({'success': True, 'message': '配置重载成功'})

