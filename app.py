import io
import sys
import threading

import urllib3
from flask import Flask, request, jsonify
from flask_cors import CORS
from urllib3.exceptions import InsecureRequestWarning

from utils.tushare_utils import IndexAnalysis
from services import order_service

# 设置标准输出编码为 UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# 禁用 HTTPS 不安全请求警告
urllib3.disable_warnings(InsecureRequestWarning)

from monitor.config.settings import Config
from monitor.models.stock_data import StockData
from monitor.services.data_fetcher import DataFetcher
from monitor.services.alert_checker import AlertChecker
from monitor.services.alert_sender import AlertSender
from monitor.services.stock_monitor import StockMonitor
from monitor.services.volume_radio import get_volume_ratio_simple
from config.dbconfig import  exeQuery
from utils.common import format_stock_code

app = Flask(__name__)
CORS(app)

# 初始化配置和组件
config = Config()
data_fetcher = DataFetcher(config, config.DEBUG_MODE)
stock_data = StockData(config,data_fetcher)

alert_checker = AlertChecker(config, stock_data)
alert_sender = AlertSender(config)

# 创建监控器
monitor = StockMonitor(config, data_fetcher, alert_checker, alert_sender, stock_data)

@app.route('/rt_min')
def get_alerts():
    return {"000001.SH":"上证指数","data":IndexAnalysis.rt_min('000001.SH',1).to_dict(orient='records')}


@app.route('/api/volume_ratio', methods=['GET'])
@app.route('/api/volume_ratio/<string:stock_codes>', methods=['GET'])
def volume_ratio(stock_codes=None):
    if stock_codes is None:
        stock_codes = list(config.CONFIG_LIST.keys())
    else:
        stock_codes = stock_codes.split(',')

    return get_volume_ratio_simple(stock_codes)

@app.route('/api/ma', methods=['GET'])
@app.route('/api/ma/<string:stock_codes>', methods=['GET'])
def calculate_ma_distances(stock_codes=None):
    if stock_codes is None:
        stock_codes = list(config.CONFIG_LIST.keys())
    else:
        stock_codes = stock_codes.split(',')
    v = alert_checker.calculate_ma_distances(stock_codes)
    return v


@app.route('/api/get_stock_list')
def get_stock_list():
    stocks = "select * from stocks order by id desc"
    result = exeQuery(stocks)

    for stock in result:
        stock['stock_code'] = format_stock_code(stock['stock_code'],'prefix')
    return result

@app.route('/api/reload_config')
def reload_config():
    # 这里需要实现重新加载配置的逻辑
    config.reload_config()
    stock_data.initialize_data_storage()
    return "配置重载功能"


# ==================== 订单API ====================

@app.route('/api/orders', methods=['POST'])
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


@app.route('/api/orders', methods=['GET'])
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


@app.route('/api/orders/<int:order_id>', methods=['GET'])
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


@app.route('/api/orders/<int:order_id>/status', methods=['PUT'])
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


@app.route('/api/orders/<int:order_id>/cancel', methods=['POST'])
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


if __name__ == "__main__" or __name__ == 'app':
    monitor_thread = threading.Thread(target=monitor.start_monitoring, daemon=True)
    monitor_thread.start()

    app.run(host='0.0.0.0', port=5000)
