"""
股票分组相关路由
"""
import logging
import time

from flask import Blueprint, request, jsonify
from services import group_service

group_bp = Blueprint('group', __name__)
_logger = logging.getLogger(__name__)


@group_bp.route('/groups', methods=['GET'])
def get_groups():
    """获取所有分组"""
    try:
        # 默认不携带 stocks，避免首屏大查询阻塞；需要明细时调用 /groups/<id>
        include_stocks_raw = request.args.get('include_stocks', 'false')
        include_stocks = str(include_stocks_raw).lower() == 'true'
        groups = group_service.get_all_groups(include_stocks=include_stocks)

        # 兜底：避免服务层异常形态导致 500
        if groups is None:
            _logger.error('get_all_groups 返回 None，include_stocks=%s', include_stocks)
            groups = []
        elif not isinstance(groups, list):
            _logger.error(
                'get_all_groups 返回非 list，type=%s include_stocks=%s',
                type(groups).__name__,
                include_stocks,
            )
            groups = []

        return jsonify({
            'success': True,
            'data': groups,
            'total': len(groups)
        })

    except Exception as e:
        _logger.exception('获取分组失败: include_stocks=%s', request.args.get('include_stocks', 'true'))
        # 降级返回，避免前端接口 500
        return jsonify({
            'success': True,
            'data': [],
            'total': 0,
            'degraded': True,
            'message': f'分组服务降级返回，错误: {str(e)}',
        }), 200


@group_bp.route('/groups/<int:group_id>', methods=['GET'])
def get_group(group_id):
    """获取分组详情"""
    start_ts = time.perf_counter()
    try:
        include_stocks_raw = request.args.get('include_stocks', 'true')
        include_stocks = str(include_stocks_raw).lower() == 'true'
        group = group_service.get_group(group_id, include_stocks=include_stocks)
        
        if group:
            return jsonify({'success': True, 'data': group})
        else:
            return jsonify({'success': False, 'message': '分组不存在'}), 404
            
    except Exception as e:
        _logger.exception('获取分组详情失败: group_id=%s', group_id)
        return jsonify({'success': False, 'message': f'服务器错误: {str(e)}'}), 500
    finally:
        elapsed_ms = (time.perf_counter() - start_ts) * 1000
        if elapsed_ms >= 500:
            _logger.warning(
                '分组详情接口较慢: group_id=%s include_stocks=%s cost_ms=%.2f',
                group_id,
                request.args.get('include_stocks', 'true'),
                elapsed_ms,
            )


@group_bp.route('/groups/code/<string:group_code>', methods=['GET'])
def get_group_by_code(group_code):
    """根据分组代码获取分组"""
    start_ts = time.perf_counter()
    try:
        include_stocks_raw = request.args.get('include_stocks', 'true')
        include_stocks = str(include_stocks_raw).lower() == 'true'
        group = group_service.get_group_by_code(group_code, include_stocks=include_stocks)
        
        if group:
            return jsonify({'success': True, 'data': group})
        else:
            return jsonify({'success': False, 'message': '分组不存在'}), 404
            
    except Exception as e:
        _logger.exception('根据分组代码获取分组失败: group_code=%s', group_code)
        return jsonify({'success': False, 'message': f'服务器错误: {str(e)}'}), 500
    finally:
        elapsed_ms = (time.perf_counter() - start_ts) * 1000
        if elapsed_ms >= 500:
            _logger.warning(
                '按代码获取分组接口较慢: group_code=%s include_stocks=%s cost_ms=%.2f',
                group_code,
                request.args.get('include_stocks', 'true'),
                elapsed_ms,
            )


@group_bp.route('/groups', methods=['POST'])
def create_group():
    """创建分组"""
    try:
        group_data = request.get_json()
        if not group_data:
            return jsonify({'success': False, 'message': '请求数据为空'}), 400
        
        result = group_service.create_group(group_data)
        
        if result['success']:
            return jsonify(result), 201
        else:
            return jsonify(result), 400
            
    except Exception as e:
        _logger.exception('创建分组失败')
        return jsonify({'success': False, 'message': f'服务器错误: {str(e)}'}), 500


@group_bp.route('/groups/<int:group_id>', methods=['PUT'])
def update_group(group_id):
    """更新分组"""
    try:
        group_data = request.get_json()
        if not group_data:
            return jsonify({'success': False, 'message': '请求数据为空'}), 400
        
        result = group_service.update_group(group_id, group_data)
        
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 400
            
    except Exception as e:
        _logger.exception('更新分组失败: group_id=%s', group_id)
        return jsonify({'success': False, 'message': f'服务器错误: {str(e)}'}), 500


@group_bp.route('/groups/<int:group_id>', methods=['DELETE'])
def delete_group(group_id):
    """删除分组"""
    try:
        result = group_service.delete_group(group_id)
        
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 400
            
    except Exception as e:
        _logger.exception('删除分组失败: group_id=%s', group_id)
        return jsonify({'success': False, 'message': f'服务器错误: {str(e)}'}), 500


@group_bp.route('/groups/<int:group_id>/stocks', methods=['POST'])
def add_stock_to_group(group_id):
    """添加股票到分组"""
    try:
        data = request.get_json()
        if not data or not data.get('stockCode'):
            return jsonify({'success': False, 'message': '股票代码不能为空'}), 400
        
        result = group_service.add_stock_to_group(
            group_id,
            data.get('stockCode'),
            data.get('stockName', '')
        )
        
        if result['success']:
            return jsonify(result), 201
        else:
            return jsonify(result), 400
            
    except Exception as e:
        _logger.exception('添加股票到分组失败: group_id=%s', group_id)
        return jsonify({'success': False, 'message': f'服务器错误: {str(e)}'}), 500


@group_bp.route('/groups/<int:group_id>/stocks/batch', methods=['POST'])
def add_stocks_batch_to_group(group_id):
    """批量添加股票到分组（单次事务）"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': '请求数据为空'}), 400
        stocks = data.get('stocks')
        if not isinstance(stocks, list):
            return jsonify({'success': False, 'message': 'stocks 须为数组'}), 400
        if len(stocks) > 8000:
            return jsonify({'success': False, 'message': '单次最多 8000 只股票'}), 400

        result = group_service.add_stocks_batch_to_group(group_id, stocks)
        status = 200 if result.get('success') else 400
        return jsonify(result), status

    except Exception as e:
        _logger.exception('批量添加股票到分组失败: group_id=%s', group_id)
        return jsonify({'success': False, 'message': f'服务器错误: {str(e)}'}), 500


@group_bp.route('/groups/<int:group_id>/stocks/<string:stock_code>', methods=['DELETE'])
def remove_stock_from_group(group_id, stock_code):
    """从分组中移除股票"""
    try:
        result = group_service.remove_stock_from_group(group_id, stock_code)
        
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 400
            
    except Exception as e:
        _logger.exception('从分组移除股票失败: group_id=%s stock_code=%s', group_id, stock_code)
        return jsonify({'success': False, 'message': f'服务器错误: {str(e)}'}), 500


@group_bp.route('/groups/<int:group_id>/stocks', methods=['PUT'])
def update_group_stocks(group_id):
    """更新分组的股票列表（全量更新）"""
    try:
        data = request.get_json()
        stocks = data.get('stocks', []) if data else []
        
        result = group_service.update_group_stocks(group_id, stocks)
        
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 400
            
    except Exception as e:
        _logger.exception('全量更新分组股票失败: group_id=%s', group_id)
        return jsonify({'success': False, 'message': f'服务器错误: {str(e)}'}), 500

