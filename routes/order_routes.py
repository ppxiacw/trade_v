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
        try:
            limit = int(request.args.get('limit', 50))
        except (TypeError, ValueError):
            return jsonify({'success': False, 'message': 'limit 必须是整数'}), 400
        if limit < 1 or limit > 500:
            return jsonify({'success': False, 'message': 'limit 需在 1~500 之间'}), 400
        
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


@order_bp.route('/orders/import-delivery', methods=['POST'])
def import_delivery_orders():
    """导入交割单（CSV）"""
    try:
        upload_file = request.files.get('file')
        if upload_file is None:
            return jsonify({'success': False, 'message': '请上传交割单CSV文件（file）'}), 400

        result = order_service.import_delivery_csv(upload_file)
        if result.get('success'):
            return jsonify(result)
        return jsonify(result), 400
    except Exception as e:
        return jsonify({'success': False, 'message': f'服务器错误: {str(e)}'}), 500


@order_bp.route('/delivery-records', methods=['GET'])
def get_delivery_records():
    """获取交割单记录列表"""
    try:
        stock_code = request.args.get('stock_code')
        operation = request.args.get('operation')
        try:
            limit = int(request.args.get('limit', 200))
        except (TypeError, ValueError):
            return jsonify({'success': False, 'message': 'limit 必须是整数'}), 400
        if limit < 1 or limit > 5000:
            return jsonify({'success': False, 'message': 'limit 需在 1~5000 之间'}), 400

        rows = order_service.get_delivery_records(stock_code=stock_code, operation=operation, limit=limit)
        return jsonify({
            'success': True,
            'data': rows,
            'total': len(rows),
        })
    except Exception as e:
        return jsonify({'success': False, 'message': f'服务器错误: {str(e)}'}), 500


@order_bp.route('/delivery-records/import', methods=['POST'])
def import_delivery_records():
    """导入交割单记录（CSV）"""
    try:
        upload_file = request.files.get('file')
        if upload_file is None:
            return jsonify({'success': False, 'message': '请上传交割单CSV文件（file）'}), 400
        result = order_service.import_delivery_csv(upload_file)
        if result.get('success'):
            return jsonify(result)
        return jsonify(result), 400
    except Exception as e:
        return jsonify({'success': False, 'message': f'服务器错误: {str(e)}'}), 500

