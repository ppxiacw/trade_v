"""
监控相关路由
"""
from flask import Blueprint, jsonify, request
from utils.tushare_utils import IndexAnalysis
from monitor.services.volume_radio import get_volume_ratio_simple
from monitor.config.db_monitor import db_manager, stock_alert_dao

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


# ==================== 监控股票管理接口 ====================

@monitor_bp.route('/stocks', methods=['GET'])
def get_monitor_stocks():
    """获取所有监控股票列表"""
    try:
        # 先查询表结构，获取实际存在的列
        query = "SELECT * FROM stocks ORDER BY is_monitor DESC, stock_code ASC"
        stocks = db_manager.execute_query(query)
        
        # 为每个股票添加默认的配置字段（如果不存在）
        import json
        for stock in stocks:
            # 解析JSON字段（如果存在）
            if 'price_thresholds' in stock and stock.get('price_thresholds'):
                try:
                    stock['price_thresholds'] = json.loads(stock['price_thresholds'])
                except:
                    stock['price_thresholds'] = []
            else:
                stock['price_thresholds'] = []
                
            if 'change_thresholds' in stock and stock.get('change_thresholds'):
                try:
                    stock['change_thresholds'] = json.loads(stock['change_thresholds'])
                except:
                    stock['change_thresholds'] = []
            else:
                stock['change_thresholds'] = []
                
            if 'ma_types' in stock and stock.get('ma_types'):
                try:
                    stock['ma_types'] = json.loads(stock['ma_types'])
                except:
                    stock['ma_types'] = [5, 10, 20, 30, 60, 120]
            else:
                stock['ma_types'] = [5, 10, 20, 30, 60, 120]
            
            # 设置默认值
            if 'common' not in stock:
                stock['common'] = 0
            if 'normal_movement' not in stock:
                stock['normal_movement'] = 0
            if 'break_ma' not in stock:
                stock['break_ma'] = 0
        
        return jsonify({
            'success': True,
            'data': stocks,
            'total': len(stocks)
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@monitor_bp.route('/stocks', methods=['POST'])
def add_monitor_stock():
    """添加监控股票"""
    try:
        data = request.get_json()
        stock_code = data.get('stock_code')
        stock_name = data.get('stock_name', '')
        
        if not stock_code:
            return jsonify({'success': False, 'message': '股票代码不能为空'}), 400
        
        # 检查是否已存在
        existing = db_manager.execute_query(
            "SELECT id FROM stocks WHERE stock_code = %s", 
            (stock_code,)
        )
        
        if existing:
            # 更新为监控状态
            db_manager.execute_update(
                'stocks',
                {'is_monitor': 1},
                'stock_code = %s',
                (stock_code,)
            )
            return jsonify({'success': True, 'message': '已启用监控'})
        
        # 新增股票 - 只使用基本字段
        insert_data = {
            'stock_code': stock_code,
            'stock_name': stock_name,
            'is_monitor': 1
        }
        
        stock_id = db_manager.execute_insert('stocks', insert_data)
        
        return jsonify({
            'success': True, 
            'message': '添加成功，请点击重载配置使监控生效',
            'id': stock_id
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@monitor_bp.route('/stocks/<string:stock_code>', methods=['PUT'])
def update_monitor_stock(stock_code):
    """更新监控股票配置"""
    try:
        data = request.get_json()
        
        update_data = {}
        
        # 只更新数据库中存在的基本字段
        if 'is_monitor' in data:
            update_data['is_monitor'] = 1 if data['is_monitor'] else 0
        if 'stock_name' in data:
            update_data['stock_name'] = data['stock_name']
        
        if not update_data:
            return jsonify({'success': False, 'message': '没有需要更新的数据'}), 400
        
        affected = db_manager.execute_update(
            'stocks',
            update_data,
            'stock_code = %s',
            (stock_code,)
        )
        
        return jsonify({
            'success': True,
            'message': '更新成功，请点击重载配置使监控生效',
            'affected': affected
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@monitor_bp.route('/stocks/<string:stock_code>', methods=['DELETE'])
def remove_monitor_stock(stock_code):
    """移除监控股票（设置is_monitor为0）"""
    try:
        affected = db_manager.execute_update(
            'stocks',
            {'is_monitor': 0},
            'stock_code = %s',
            (stock_code,)
        )
        
        return jsonify({
            'success': True,
            'message': '已停止监控，请点击重载配置使更改生效',
            'affected': affected
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# ==================== 告警历史记录接口 ====================

@monitor_bp.route('/alerts', methods=['GET'])
def get_alert_history():
    """获取告警历史记录"""
    try:
        # 获取查询参数
        stock_code = request.args.get('stock_code')
        alert_type = request.args.get('alert_type')
        start_time = request.args.get('start_time')
        end_time = request.args.get('end_time')
        limit = int(request.args.get('limit', 100))
        offset = int(request.args.get('offset', 0))
        
        # 构建查询条件
        conditions = []
        params = []
        
        if stock_code:
            conditions.append("stock_code = %s")
            params.append(stock_code)
        if alert_type:
            conditions.append("alert_type = %s")
            params.append(alert_type)
        if start_time:
            conditions.append("trigger_time >= %s")
            params.append(start_time)
        if end_time:
            conditions.append("trigger_time <= %s")
            params.append(end_time)
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        # 查询总数
        count_query = f"SELECT COUNT(*) as total FROM stock_alert_log WHERE {where_clause}"
        count_result = db_manager.execute_query(count_query, tuple(params) if params else None)
        total = count_result[0]['total'] if count_result else 0
        
        # 查询数据
        query = f"""
            SELECT id, stock_code, stock_name, alert_type, alert_level, 
                   alert_message, trigger_time, windows_sec
            FROM stock_alert_log 
            WHERE {where_clause}
            ORDER BY trigger_time DESC
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])
        
        alerts = db_manager.execute_query(query, tuple(params))
        
        return jsonify({
            'success': True,
            'data': alerts,
            'total': total,
            'limit': limit,
            'offset': offset
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@monitor_bp.route('/alerts/stats', methods=['GET'])
def get_alert_stats():
    """获取告警统计信息"""
    try:
        # 今日告警数量
        today_query = """
            SELECT COUNT(*) as count FROM stock_alert_log 
            WHERE DATE(trigger_time) = CURDATE()
        """
        today_result = db_manager.execute_query(today_query)
        today_count = today_result[0]['count'] if today_result else 0
        
        # 按股票分组统计
        stock_query = """
            SELECT stock_code, stock_name, COUNT(*) as count 
            FROM stock_alert_log 
            WHERE DATE(trigger_time) = CURDATE()
            GROUP BY stock_code, stock_name
            ORDER BY count DESC
            LIMIT 10
        """
        stock_stats = db_manager.execute_query(stock_query)
        
        # 按告警类型分组统计
        type_query = """
            SELECT alert_type, COUNT(*) as count 
            FROM stock_alert_log 
            WHERE DATE(trigger_time) = CURDATE()
            GROUP BY alert_type
            ORDER BY count DESC
        """
        type_stats = db_manager.execute_query(type_query)
        
        return jsonify({
            'success': True,
            'data': {
                'today_count': today_count,
                'stock_stats': stock_stats,
                'type_stats': type_stats
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@monitor_bp.route('/reload', methods=['POST'])
def reload_monitor_config():
    """重新加载监控配置"""
    try:
        from app import config
        config.reload_config()
        
        return jsonify({
            'success': True,
            'message': '配置已重新加载',
            'monitor_count': len(config.MONITOR_STOCKS)
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

