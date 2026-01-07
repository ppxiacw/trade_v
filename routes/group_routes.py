"""
股票分组相关路由
"""
from flask import Blueprint, request, jsonify
from services import group_service

group_bp = Blueprint('group', __name__)


@group_bp.route('/groups', methods=['GET'])
def get_groups():
    """获取所有分组"""
    try:
        include_stocks = request.args.get('include_stocks', 'true').lower() == 'true'
        groups = group_service.get_all_groups(include_stocks=include_stocks)
        
        return jsonify({
            'success': True,
            'data': groups,
            'total': len(groups)
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': f'服务器错误: {str(e)}'}), 500


@group_bp.route('/groups/<int:group_id>', methods=['GET'])
def get_group(group_id):
    """获取分组详情"""
    try:
        group = group_service.get_group(group_id)
        
        if group:
            return jsonify({'success': True, 'data': group})
        else:
            return jsonify({'success': False, 'message': '分组不存在'}), 404
            
    except Exception as e:
        return jsonify({'success': False, 'message': f'服务器错误: {str(e)}'}), 500


@group_bp.route('/groups/code/<string:group_code>', methods=['GET'])
def get_group_by_code(group_code):
    """根据分组代码获取分组"""
    try:
        group = group_service.get_group_by_code(group_code)
        
        if group:
            return jsonify({'success': True, 'data': group})
        else:
            return jsonify({'success': False, 'message': '分组不存在'}), 404
            
    except Exception as e:
        return jsonify({'success': False, 'message': f'服务器错误: {str(e)}'}), 500


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
        return jsonify({'success': False, 'message': f'服务器错误: {str(e)}'}), 500

