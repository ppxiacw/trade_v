"""
监控相关路由
"""
import threading
import re
import time
from datetime import datetime, timedelta
from flask import Blueprint, jsonify, request
from utils.tushare_utils import IndexAnalysis
from monitor.services.volume_radio import get_volume_ratio_simple
from monitor.config.db_monitor import db_manager, stock_alert_dao
from monitor.config.stock_code import normalize_monitor_stock_code

monitor_bp = Blueprint('monitor', __name__)
_monitor_columns_lock = threading.Lock()
_monitor_columns_ready = False
_route_cache_lock = threading.Lock()
_route_cache = {}

_CACHE_TTL_STOCKS = 15
_CACHE_TTL_ALERTS = 8
_CACHE_TTL_STATS = 12


def _format_datetime_for_client(value):
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    return value


def _serialize_alert_rows_for_client(rows):
    out = []
    for row in rows or []:
        item = dict(row)
        item['trigger_time'] = _format_datetime_for_client(item.get('trigger_time'))
        out.append(item)
    return out


def _cache_get(cache_key):
    now = time.time()
    with _route_cache_lock:
        cached = _route_cache.get(cache_key)
        if not cached:
            return None
        if cached['expires_at'] <= now:
            _route_cache.pop(cache_key, None)
            return None
        return cached['value']


def _cache_set(cache_key, value, ttl_seconds):
    with _route_cache_lock:
        _route_cache[cache_key] = {
            'value': value,
            'expires_at': time.time() + max(1, int(ttl_seconds))
        }


def _invalidate_monitor_cache():
    with _route_cache_lock:
        keys = list(_route_cache.keys())
        for key in keys:
            if key.startswith("monitor:stocks") or key.startswith("monitor:alerts") or key.startswith("monitor:stats"):
                _route_cache.pop(key, None)


def _build_stock_code_aliases(stock_code, stock_name=""):
    code = str(stock_code or "").strip()
    aliases = set()
    if not code:
        return aliases

    aliases.add(code)
    aliases.add(code.lower())
    aliases.add(code.upper())

    normalized = normalize_monitor_stock_code(code, stock_name)
    if normalized:
        aliases.add(normalized)
        aliases.add(normalized.lower())
        aliases.add(normalized.upper())

        pure = normalized.split('.')[0] if '.' in normalized else normalized
        exchange = normalized.split('.')[-1].upper() if '.' in normalized else ''
        if pure.isdigit() and exchange in ('SH', 'SZ'):
            aliases.add(pure)
            aliases.add(f"{exchange.lower()}{pure}")
            aliases.add(f"{pure}.{exchange}")
            aliases.add(f"{pure}.{exchange.lower()}")
            aliases.add(f"{pure}.{'SH' if exchange == 'SZ' else 'SZ'}")
            aliases.add(f"{'sh' if exchange == 'SZ' else 'sz'}{pure}")

    pure_match = re.fullmatch(r"([0-9]{1,6})", code)
    if pure_match:
        pure = pure_match.group(1).zfill(6)
        aliases.update({
            pure,
            f"sh{pure}",
            f"sz{pure}",
            f"{pure}.SH",
            f"{pure}.SZ",
            f"{pure}.sh",
            f"{pure}.sz",
        })

    return {a for a in aliases if a}


def _find_stock_row_by_aliases(stock_code, stock_name=""):
    aliases = list(_build_stock_code_aliases(stock_code, stock_name))
    if not aliases:
        return None
    placeholders = ", ".join(["%s"] * len(aliases))
    rows = db_manager.execute_query(
        f"SELECT id, stock_code, stock_name FROM stocks WHERE stock_code IN ({placeholders}) LIMIT 1",
        tuple(aliases)
    )
    return rows[0] if rows else None


def _ensure_monitor_stock_columns_once():
    """
    确保监控股票表包含告警价格区间字段（幂等）。
    """
    global _monitor_columns_ready
    if _monitor_columns_ready:
        return

    with _monitor_columns_lock:
        if _monitor_columns_ready:
            return
        conn = None
        cursor = None
        try:
            with db_manager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                cursor.execute("SHOW COLUMNS FROM stocks LIKE 'trigger_min_price'")
                has_min = cursor.fetchone() is not None
                cursor.fetchall()
                cursor.execute("SHOW COLUMNS FROM stocks LIKE 'trigger_max_price'")
                has_max = cursor.fetchone() is not None
                cursor.fetchall()

                if not has_min:
                    cursor.execute(
                        "ALTER TABLE stocks ADD COLUMN trigger_min_price DECIMAL(12,4) NULL COMMENT '告警触发最小价格'"
                    )
                if not has_max:
                    cursor.execute(
                        "ALTER TABLE stocks ADD COLUMN trigger_max_price DECIMAL(12,4) NULL COMMENT '告警触发最大价格'"
                    )
            _monitor_columns_ready = True
        except Exception:
            # 字段补齐失败不阻断接口（旧环境继续可用）
            pass
        finally:
            if cursor:
                cursor.close()


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
        stock_codes = [normalize_monitor_stock_code(code) for code in stock_codes.split(',')]

    return get_volume_ratio_simple(stock_codes)


@monitor_bp.route('/ma', methods=['GET'])
@monitor_bp.route('/ma/<string:stock_codes>', methods=['GET'])
def calculate_ma_distances(stock_codes=None):
    """计算均线距离"""
    from app import config, alert_checker
    
    if stock_codes is None:
        stock_codes = list(config.CONFIG_LIST.keys())
    else:
        stock_codes = [normalize_monitor_stock_code(code) for code in stock_codes.split(',')]
    
    v = alert_checker.calculate_ma_distances(stock_codes)
    return v


# ==================== 监控股票管理接口 ====================

@monitor_bp.route('/stocks', methods=['GET'])
def get_monitor_stocks():
    """获取监控中的股票列表"""
    try:
        cache_key = "monitor:stocks:list"
        cached = _cache_get(cache_key)
        if cached is not None:
            return jsonify(cached)

        _ensure_monitor_stock_columns_once()
        # 只查询监控中的股票
        query = "SELECT * FROM stocks WHERE is_monitor = 1 ORDER BY stock_code ASC"
        stocks = db_manager.execute_query(query)
        
        # 为每个股票添加默认的配置字段（如果不存在）
        import json
        for stock in stocks:
            normalized_code = normalize_monitor_stock_code(stock.get('stock_code'), stock.get('stock_name'))
            if stock.get('id') and normalized_code and normalized_code != stock.get('stock_code'):
                db_manager.execute_update(
                    'stocks',
                    {'stock_code': normalized_code},
                    'id = %s',
                    (stock['id'],)
                )
            stock['stock_code'] = normalized_code or stock.get('stock_code')

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

            # 价格区间触发（为空表示不限制）
            stock['trigger_min_price'] = stock.get('trigger_min_price')
            stock['trigger_max_price'] = stock.get('trigger_max_price')
        
        payload = {
            'success': True,
            'data': stocks,
            'total': len(stocks)
        }
        _cache_set(cache_key, payload, _CACHE_TTL_STOCKS)
        return jsonify(payload)
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@monitor_bp.route('/stocks', methods=['POST'])
def add_monitor_stock():
    """添加监控股票"""
    try:
        _ensure_monitor_stock_columns_once()
        data = request.get_json() or {}
        raw_stock_code = data.get('stock_code')
        stock_name = data.get('stock_name', '')
        stock_code = normalize_monitor_stock_code(raw_stock_code, stock_name)
        
        if not stock_code:
            return jsonify({'success': False, 'message': '股票代码不能为空'}), 400
        
        # 检查是否已存在
        existing = _find_stock_row_by_aliases(raw_stock_code, stock_name)
        
        if existing:
            # 更新为监控状态
            db_manager.execute_update(
                'stocks',
                {
                    'is_monitor': 1,
                    'stock_code': stock_code,
                    'stock_name': stock_name or existing.get('stock_name', '')
                },
                'id = %s',
                (existing['id'],)
            )
            _invalidate_monitor_cache()
            return jsonify({'success': True, 'message': '已启用监控'})
        
        # 新增股票 - 只使用基本字段
        insert_data = {
            'stock_code': stock_code,
            'stock_name': stock_name,
            'is_monitor': 1
        }
        if 'trigger_min_price' in data:
            insert_data['trigger_min_price'] = data.get('trigger_min_price')
        if 'trigger_max_price' in data:
            insert_data['trigger_max_price'] = data.get('trigger_max_price')
        
        stock_id = db_manager.execute_insert('stocks', insert_data)
        _invalidate_monitor_cache()
        
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
        _ensure_monitor_stock_columns_once()
        data = request.get_json() or {}
        row = _find_stock_row_by_aliases(stock_code, data.get('stock_name', ''))
        if not row:
            return jsonify({'success': False, 'message': '股票不存在'}), 404
        normalized_code = normalize_monitor_stock_code(stock_code, data.get('stock_name', row.get('stock_name', '')))
        
        update_data = {}
        
        # 只更新数据库中存在的基本字段
        if 'is_monitor' in data:
            update_data['is_monitor'] = 1 if data['is_monitor'] else 0
        if 'stock_name' in data:
            update_data['stock_name'] = data['stock_name']
        update_data['stock_code'] = normalized_code
        if 'common' in data:
            update_data['common'] = 1 if data['common'] else 0
        if 'normal_movement' in data:
            update_data['normal_movement'] = 1 if data['normal_movement'] else 0
        if 'trigger_min_price' in data:
            update_data['trigger_min_price'] = data.get('trigger_min_price')
        if 'trigger_max_price' in data:
            update_data['trigger_max_price'] = data.get('trigger_max_price')
        
        if not update_data:
            return jsonify({'success': False, 'message': '没有需要更新的数据'}), 400
        
        affected = db_manager.execute_update(
            'stocks',
            update_data,
            'id = %s',
            (row['id'],)
        )
        _invalidate_monitor_cache()
        
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
        row = _find_stock_row_by_aliases(stock_code)
        if not row:
            return jsonify({'success': False, 'message': '股票不存在'}), 404
        affected = db_manager.execute_update(
            'stocks',
            {'is_monitor': 0},
            'id = %s',
            (row['id'],)
        )
        _invalidate_monitor_cache()
        
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
            stock_code = normalize_monitor_stock_code(stock_code)
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

        cache_key = (
            f"monitor:alerts:{stock_code or ''}:{alert_type or ''}:"
            f"{start_time or ''}:{end_time or ''}:{limit}:{offset}"
        )
        cached = _cache_get(cache_key)
        if cached is not None:
            return jsonify(cached)
        
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
        alerts = _serialize_alert_rows_for_client(alerts)
        
        payload = {
            'success': True,
            'data': alerts,
            'total': total,
            'limit': limit,
            'offset': offset
        }
        _cache_set(cache_key, payload, _CACHE_TTL_ALERTS)
        return jsonify(payload)
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@monitor_bp.route('/alerts/stats', methods=['GET'])
def get_alert_stats():
    """获取告警统计信息"""
    try:
        cache_key = "monitor:stats:today"
        cached = _cache_get(cache_key)
        if cached is not None:
            return jsonify(cached)

        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        today_end = today_start + timedelta(days=1)
        time_range_params = (today_start, today_end)

        # 今日告警数量
        today_query = """
            SELECT COUNT(*) as count FROM stock_alert_log 
            WHERE trigger_time >= %s AND trigger_time < %s
        """
        today_result = db_manager.execute_query(today_query, time_range_params)
        today_count = today_result[0]['count'] if today_result else 0
        
        # 按股票分组统计
        stock_query = """
            SELECT stock_code, stock_name, COUNT(*) as count 
            FROM stock_alert_log 
            WHERE trigger_time >= %s AND trigger_time < %s
            GROUP BY stock_code, stock_name
            ORDER BY count DESC
            LIMIT 10
        """
        stock_stats = db_manager.execute_query(stock_query, time_range_params)
        
        # 按告警类型分组统计
        type_query = """
            SELECT alert_type, COUNT(*) as count 
            FROM stock_alert_log 
            WHERE trigger_time >= %s AND trigger_time < %s
            GROUP BY alert_type
            ORDER BY count DESC
        """
        type_stats = db_manager.execute_query(type_query, time_range_params)
        
        payload = {
            'success': True,
            'data': {
                'today_count': today_count,
                'stock_stats': stock_stats,
                'type_stats': type_stats
            }
        }
        _cache_set(cache_key, payload, _CACHE_TTL_STATS)
        return jsonify(payload)
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@monitor_bp.route('/reload', methods=['POST'])
def reload_monitor_config():
    """重新加载监控配置"""
    try:
        from app import config
        config.reload_config()
        _invalidate_monitor_cache()
        
        return jsonify({
            'success': True,
            'message': '配置已重新加载',
            'monitor_count': len(config.MONITOR_STOCKS)
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

