"""
订单相关路由
"""
from flask import Blueprint, request, jsonify
from services import order_service

order_bp = Blueprint('order', __name__)


@order_bp.route('/orders', methods=['POST'])
def create_order():
    """创建订单"""
    try:
        order_data = request.get_json()
        if not order_data:
            return jsonify({'success': False, 'message': '请求数据为空'}), 400
        
        result = order_service.create_order(order_data)
        
        if result['success']:
            return jsonify(result), 201
        else:
            return jsonify(result), 400
            
    except Exception as e:
        return jsonify({'success': False, 'message': f'服务器错误: {str(e)}'}), 500


@order_bp.route('/orders', methods=['GET'])
def get_orders():
    """获取订单列表"""
    try:
        status = request.args.get('status')
        stock_code = request.args.get('stock_code')
        limit = int(request.args.get('limit', 50))
        
        orders = order_service.get_orders(status=status, stock_code=stock_code, limit=limit)
        
        return jsonify({
            'success': True,
            'data': orders,
            'total': len(orders)
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': f'服务器错误: {str(e)}'}), 500


@order_bp.route('/orders/<int:order_id>', methods=['GET'])
def get_order(order_id):
    """获取订单详情"""
    try:
        order = order_service.get_order(order_id)
        
        if order:
            return jsonify({'success': True, 'data': order})
        else:
            return jsonify({'success': False, 'message': '订单不存在'}), 404
            
    except Exception as e:
        return jsonify({'success': False, 'message': f'服务器错误: {str(e)}'}), 500


@order_bp.route('/orders/<int:order_id>/status', methods=['PUT'])
def update_order_status(order_id):
    """更新订单状态"""
    try:
        data = request.get_json()
        new_status = data.get('status')
        note = data.get('note')
        
        if not new_status:
            return jsonify({'success': False, 'message': '状态不能为空'}), 400
        
        result = order_service.update_order_status(order_id, new_status, note)
        
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 400
            
    except Exception as e:
        return jsonify({'success': False, 'message': f'服务器错误: {str(e)}'}), 500


@order_bp.route('/orders/<int:order_id>/cancel', methods=['POST'])
def cancel_order(order_id):
    """取消订单"""
    try:
        data = request.get_json() or {}
        reason = data.get('reason')
        
        result = order_service.cancel_order(order_id, reason)
        
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 400
            
    except Exception as e:
        return jsonify({'success': False, 'message': f'服务器错误: {str(e)}'}), 500

