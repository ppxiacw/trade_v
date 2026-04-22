"""
监控相关路由
"""
import threading
import re
import time
import json
from datetime import datetime, timedelta
from flask import Blueprint, jsonify, request
from utils.tushare_utils import IndexAnalysis
from utils.GetStockData import get_stock_name
from monitor.services.volume_radio import get_volume_ratio_simple
from monitor.config.db_monitor import db_manager, stock_alert_dao
from monitor.config.stock_code import normalize_monitor_stock_code
from runtime_state import get_alert_checker, get_alert_sender, get_config, get_stock_data

monitor_bp = Blueprint('monitor', __name__)
_monitor_columns_lock = threading.Lock()
_monitor_columns_ready = False
_route_cache_lock = threading.Lock()
_route_cache = {}

_CACHE_TTL_STOCKS = 15
_CACHE_TTL_ALERTS = 8
_CACHE_TTL_STATS = 12

_DIVERGENCE_PERIOD_LABELS = {
    'time': '分时',
    'm1': '1分钟',
    'm5': '5分钟',
    'm15': '15分钟',
    'm30': '30分钟',
    'day': '日K',
    'week': '周K',
    'month': '月K',
}
_DIVERGENCE_PERIOD_SECONDS = {
    'time': 60,
    'm1': 60,
    'm5': 300,
    'm15': 900,
    'm30': 1800,
    'day': 86400,
    'week': 604800,
    'month': 2592000,
}
_DIVERGENCE_COOLDOWN_SECONDS = {
    'time': 120,
    'm1': 120,
    'm5': 300,
    'm15': 900,
    'm30': 1800,
    'day': 21600,
    'week': 86400,
    'month': 172800,
}
_DIVERGENCE_TYPE_LABELS = {
    'top': '顶背离',
    'bottom': '底背离',
}
_DIVERGENCE_ALLOWED_PERIODS = ['m1', 'm5', 'm15', 'm30', 'day', 'week', 'month']
_DIVERGENCE_DEFAULT_PERIODS = ['m30']


def _to_bool(value, default=False):
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    if text in {'1', 'true', 'yes', 'on'}:
        return True
    if text in {'0', 'false', 'no', 'off'}:
        return False
    return bool(default)


def _to_int_or_none(value):
    if value is None or value == '':
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_divergence_periods(periods):
    if isinstance(periods, str):
        matched = re.findall(r"m1|m5|m15|m30|day|week|month", periods.lower())
        candidates = [item.strip().lower() for item in matched]
    elif isinstance(periods, (list, tuple, set)):
        candidates = [str(item or '').strip().lower() for item in periods]
    else:
        candidates = []

    values = []
    for period in candidates:
        if not period or period not in _DIVERGENCE_ALLOWED_PERIODS:
            continue
        if period not in values:
            values.append(period)
    return values or list(_DIVERGENCE_DEFAULT_PERIODS)


def _periods_to_storage_text(periods):
    return json.dumps(_normalize_divergence_periods(periods), ensure_ascii=False)


def _parse_periods_from_storage(raw_value):
    if isinstance(raw_value, str) and raw_value.strip():
        try:
            parsed = json.loads(raw_value)
            return _normalize_divergence_periods(parsed)
        except Exception:
            return _normalize_divergence_periods(raw_value)
    return list(_DIVERGENCE_DEFAULT_PERIODS)


def _apply_divergence_defaults_to_stock(stock):
    stock['divergence_enabled'] = 1 if _to_bool(stock.get('divergence_enabled'), False) else 0
    stock['divergence_macd_enabled'] = 1 if _to_bool(stock.get('divergence_macd_enabled'), True) else 0
    stock['divergence_rsi_enabled'] = 1 if _to_bool(stock.get('divergence_rsi_enabled'), True) else 0
    stock['divergence_top_enabled'] = 1 if _to_bool(stock.get('divergence_top_enabled'), True) else 0
    stock['divergence_bottom_enabled'] = 1 if _to_bool(stock.get('divergence_bottom_enabled'), True) else 0
    stock['divergence_periods'] = _parse_periods_from_storage(stock.get('divergence_periods'))

    scan_interval = _to_int_or_none(stock.get('divergence_scan_interval_seconds'))
    stock['divergence_scan_interval_seconds'] = max(15, scan_interval) if scan_interval is not None else 60

    kline_count = _to_int_or_none(stock.get('divergence_kline_count'))
    stock['divergence_kline_count'] = max(120, kline_count) if kline_count is not None else 240

    lookback = _to_int_or_none(stock.get('divergence_lookback'))
    stock['divergence_lookback'] = max(2, lookback) if lookback is not None else 5


def _build_divergence_patch_from_payload(data):
    patch = {}
    if 'divergence_enabled' in data:
        patch['divergence_enabled'] = 1 if _to_bool(data.get('divergence_enabled')) else 0
    if 'divergence_macd_enabled' in data:
        patch['divergence_macd_enabled'] = 1 if _to_bool(data.get('divergence_macd_enabled'), True) else 0
    if 'divergence_rsi_enabled' in data:
        patch['divergence_rsi_enabled'] = 1 if _to_bool(data.get('divergence_rsi_enabled'), True) else 0
    if 'divergence_top_enabled' in data:
        patch['divergence_top_enabled'] = 1 if _to_bool(data.get('divergence_top_enabled'), True) else 0
    if 'divergence_bottom_enabled' in data:
        patch['divergence_bottom_enabled'] = 1 if _to_bool(data.get('divergence_bottom_enabled'), True) else 0
    if 'divergence_periods' in data:
        patch['divergence_periods'] = _periods_to_storage_text(data.get('divergence_periods'))
    if 'divergence_scan_interval_seconds' in data:
        value = _to_int_or_none(data.get('divergence_scan_interval_seconds'))
        patch['divergence_scan_interval_seconds'] = max(15, value) if value is not None else None
    if 'divergence_kline_count' in data:
        value = _to_int_or_none(data.get('divergence_kline_count'))
        patch['divergence_kline_count'] = max(120, value) if value is not None else None
    if 'divergence_lookback' in data:
        value = _to_int_or_none(data.get('divergence_lookback'))
        patch['divergence_lookback'] = max(2, value) if value is not None else None
    return patch


def _safe_float(value):
    try:
        num = float(value)
        return num
    except (TypeError, ValueError):
        return None


def _build_divergence_alert_message(period, indicator, divergence_type, signal_time, price, indicator_value):
    period_label = _DIVERGENCE_PERIOD_LABELS.get(period, period or '未知周期')
    divergence_label = _DIVERGENCE_TYPE_LABELS.get(divergence_type, divergence_type or '背离')
    pieces = [f"{period_label}{indicator}{divergence_label}"]
    if signal_time:
        pieces.append(f"信号时间:{signal_time}")
    if price is not None:
        pieces.append(f"价格:{price:.2f}")
    if indicator_value is not None:
        pieces.append(f"{indicator}:{indicator_value:.2f}")
    return " | ".join(pieces)


def _get_divergence_cooldown(period):
    return _DIVERGENCE_COOLDOWN_SECONDS.get(period, 600)


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


def _reload_monitor_runtime():
    """
    将数据库中的监控配置同步到运行中内存对象，确保增删改后无需手动点“重载配置”。
    """
    config = get_config()
    stock_data = get_stock_data()
    alert_sender = get_alert_sender()

    config.reload_config()
    monitor_codes = list(config.MONITOR_STOCKS.keys())

    for stock in monitor_codes:
        alert_sender.last_alert_time.setdefault(stock, {})
        stock_data.data_storage.setdefault(
            stock,
            {
                "candles": [],
                "interval": config.BASE_INTERVAL,
            },
        )
        stock_data.last_update_time.setdefault(stock, datetime.now())

    _invalidate_monitor_cache()
    return len(monitor_codes)


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

    return {a for a in aliases if a}


def _find_stock_row_by_aliases(stock_code, stock_name=""):
    aliases = list(_build_stock_code_aliases(stock_code, stock_name))
    if not aliases:
        return None
    placeholders = ", ".join(["%s"] * len(aliases))
    rows = db_manager.execute_query(
        f"SELECT id, stock_code, stock_name, is_monitor, sort_order FROM stocks WHERE stock_code IN ({placeholders}) ORDER BY is_monitor DESC, id DESC LIMIT 1",
        tuple(aliases)
    )
    return rows[0] if rows else None


def _find_stock_row_by_id(stock_id):
    rows = db_manager.execute_query(
        "SELECT id, stock_code, stock_name, is_monitor, sort_order FROM stocks WHERE id = %s LIMIT 1",
        (stock_id,),
    )
    return rows[0] if rows else None


def _find_stock_row_by_exact_code(stock_code, exclude_id=None):
    if not stock_code:
        return None

    query = "SELECT id, stock_code, stock_name, is_monitor, sort_order FROM stocks WHERE stock_code = %s"
    params = [stock_code]
    if exclude_id is not None:
        query += " AND id <> %s"
        params.append(exclude_id)
    query += " ORDER BY is_monitor DESC, id DESC LIMIT 1"

    rows = db_manager.execute_query(query, tuple(params))
    return rows[0] if rows else None


def _get_next_monitor_sort_order():
    rows = db_manager.execute_query(
        "SELECT COALESCE(MAX(sort_order), 0) AS max_order FROM stocks WHERE is_monitor = 1"
    )
    max_order = int(rows[0].get('max_order') or 0) if rows else 0
    return max_order + 1


def _repair_monitor_sort_orders():
    rows = db_manager.execute_query(
        """
        SELECT id, sort_order
        FROM stocks
        WHERE is_monitor = 1
        ORDER BY
            CASE WHEN sort_order IS NULL THEN 1 ELSE 0 END ASC,
            sort_order ASC,
            id ASC
        """
    ) or []
    expected = 1
    for row in rows:
        current = row.get('sort_order')
        if current != expected:
            db_manager.execute_update(
                'stocks',
                {'sort_order': expected},
                'id = %s',
                (row['id'],),
            )
        expected += 1


def _update_monitor_stock_row(row, stock_code_hint, data):
    if not row:
        return jsonify({'success': False, 'message': '股票不存在'}), 404

    normalized_input_code = data.get('stock_code') or stock_code_hint or row.get('stock_code')
    normalized_code = normalize_monitor_stock_code(
        normalized_input_code,
        data.get('stock_name', row.get('stock_name', '')),
    )

    duplicate_row = _find_stock_row_by_exact_code(normalized_code, exclude_id=row['id'])
    if duplicate_row:
        return jsonify({
            'success': False,
            'message': f"股票代码 {normalized_code} 已存在于记录 #{duplicate_row['id']}，请先清理重复数据",
        }), 409

    update_data = {}

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
    if 'sort_order' in data:
        try:
            update_data['sort_order'] = int(data.get('sort_order'))
        except (TypeError, ValueError):
            return jsonify({'success': False, 'message': '排序值格式错误'}), 400

    if ('is_monitor' in data and data.get('is_monitor')) and not row.get('is_monitor'):
        if 'sort_order' not in update_data:
            update_data['sort_order'] = _get_next_monitor_sort_order()
    update_data.update(_build_divergence_patch_from_payload(data))

    if not update_data:
        return jsonify({'success': False, 'message': '没有需要更新的数据'}), 400

    affected = db_manager.execute_update(
        'stocks',
        update_data,
        'id = %s',
        (row['id'],)
    )
    monitor_count = _reload_monitor_runtime()

    return jsonify({
        'success': True,
        'message': '更新成功，修改已自动生效',
        'affected': affected,
        'monitor_count': monitor_count,
    })


def _remove_monitor_stock_row(row):
    if not row:
        return jsonify({'success': False, 'message': '股票不存在'}), 404

    affected = db_manager.execute_update(
        'stocks',
        {'is_monitor': 0},
        'id = %s',
        (row['id'],)
    )
    monitor_count = _reload_monitor_runtime()

    return jsonify({
        'success': True,
        'message': '已从监控列表删除，修改已自动生效',
        'affected': affected,
        'monitor_count': monitor_count,
    })


def _ensure_monitor_stock_columns_once():
    """
    确保监控股票表包含监控配置字段（幂等）。
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
                expected_columns = {
                    'sort_order': "ALTER TABLE stocks ADD COLUMN sort_order INT NULL COMMENT '监控股票排序'",
                    'trigger_min_price': "ALTER TABLE stocks ADD COLUMN trigger_min_price DECIMAL(12,4) NULL COMMENT '告警触发最小价格'",
                    'trigger_max_price': "ALTER TABLE stocks ADD COLUMN trigger_max_price DECIMAL(12,4) NULL COMMENT '告警触发最大价格'",
                    'divergence_enabled': "ALTER TABLE stocks ADD COLUMN divergence_enabled TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否启用背离监控'",
                    'divergence_periods': "ALTER TABLE stocks ADD COLUMN divergence_periods VARCHAR(255) NULL COMMENT '背离监控周期(JSON数组)'",
                    'divergence_scan_interval_seconds': "ALTER TABLE stocks ADD COLUMN divergence_scan_interval_seconds INT NULL COMMENT '背离扫描间隔秒'",
                    'divergence_kline_count': "ALTER TABLE stocks ADD COLUMN divergence_kline_count INT NULL COMMENT '背离计算K线样本数'",
                    'divergence_lookback': "ALTER TABLE stocks ADD COLUMN divergence_lookback INT NULL COMMENT '背离局部极值窗口'",
                    'divergence_macd_enabled': "ALTER TABLE stocks ADD COLUMN divergence_macd_enabled TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否启用MACD背离'",
                    'divergence_rsi_enabled': "ALTER TABLE stocks ADD COLUMN divergence_rsi_enabled TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否启用RSI背离'",
                    'divergence_top_enabled': "ALTER TABLE stocks ADD COLUMN divergence_top_enabled TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否启用顶背离'",
                    'divergence_bottom_enabled': "ALTER TABLE stocks ADD COLUMN divergence_bottom_enabled TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否启用底背离'",
                }

                for column_name, alter_sql in expected_columns.items():
                    cursor.execute(f"SHOW COLUMNS FROM stocks LIKE '{column_name}'")
                    exists = cursor.fetchone() is not None
                    cursor.fetchall()
                    if not exists:
                        cursor.execute(alter_sql)
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
    config = get_config()
    
    if stock_codes is None:
        stock_codes = list(config.CONFIG_LIST.keys())
    else:
        stock_codes = [normalize_monitor_stock_code(code) for code in stock_codes.split(',')]

    return get_volume_ratio_simple(stock_codes)


@monitor_bp.route('/ma', methods=['GET'])
@monitor_bp.route('/ma/<string:stock_codes>', methods=['GET'])
def calculate_ma_distances(stock_codes=None):
    """计算均线距离"""
    config = get_config()
    alert_checker = get_alert_checker()
    
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
        _repair_monitor_sort_orders()
        # 只查询监控中的股票
        query = "SELECT * FROM stocks WHERE is_monitor = 1 ORDER BY sort_order ASC, id ASC"
        stocks = db_manager.execute_query(query)
        
        # 为每个股票添加默认的配置字段（如果不存在）
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
            stock['sort_order'] = int(stock.get('sort_order') or 0)
            _apply_divergence_defaults_to_stock(stock)
        
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

        existing = _find_stock_row_by_exact_code(stock_code) or _find_stock_row_by_aliases(raw_stock_code, stock_name)
        
        if existing:
            reactivate_data = {
                'is_monitor': 1,
                'stock_code': stock_code,
                'stock_name': stock_name or existing.get('stock_name', ''),
            }
            if existing.get('is_monitor'):
                reactivate_data['sort_order'] = existing.get('sort_order') or _get_next_monitor_sort_order()
            else:
                reactivate_data['sort_order'] = _get_next_monitor_sort_order()
            if 'common' in data:
                reactivate_data['common'] = 1 if data['common'] else 0
            if 'normal_movement' in data:
                reactivate_data['normal_movement'] = 1 if data['normal_movement'] else 0
            if 'trigger_min_price' in data:
                reactivate_data['trigger_min_price'] = data.get('trigger_min_price')
            if 'trigger_max_price' in data:
                reactivate_data['trigger_max_price'] = data.get('trigger_max_price')
            reactivate_data.update(_build_divergence_patch_from_payload(data))

            # 更新为监控状态
            db_manager.execute_update(
                'stocks',
                reactivate_data,
                'id = %s',
                (existing['id'],)
            )
            monitor_count = _reload_monitor_runtime()
            return jsonify({
                'success': True,
                'message': '已启用监控，修改已自动生效',
                'monitor_count': monitor_count,
            })
        
        # 新增股票 - 只使用基本字段
        insert_data = {
            'stock_code': stock_code,
            'stock_name': stock_name,
            'is_monitor': 1,
            'sort_order': _get_next_monitor_sort_order(),
        }
        if 'common' in data:
            insert_data['common'] = 1 if data['common'] else 0
        if 'normal_movement' in data:
            insert_data['normal_movement'] = 1 if data['normal_movement'] else 0
        if 'trigger_min_price' in data:
            insert_data['trigger_min_price'] = data.get('trigger_min_price')
        if 'trigger_max_price' in data:
            insert_data['trigger_max_price'] = data.get('trigger_max_price')
        insert_data.update(_build_divergence_patch_from_payload(data))
        
        stock_id = db_manager.execute_insert('stocks', insert_data)
        if not stock_id:
            return jsonify({'success': False, 'message': '添加监控失败，可能存在重复股票代码'}), 409
        monitor_count = _reload_monitor_runtime()
        
        return jsonify({
            'success': True, 
            'message': '添加成功，修改已自动生效',
            'id': stock_id,
            'monitor_count': monitor_count,
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
        return _update_monitor_stock_row(row, stock_code, data)
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@monitor_bp.route('/stocks/<string:stock_code>', methods=['DELETE'])
def remove_monitor_stock(stock_code):
    """移除监控股票（设置is_monitor为0）"""
    try:
        row = _find_stock_row_by_aliases(stock_code)
        return _remove_monitor_stock_row(row)
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@monitor_bp.route('/stocks/by-id/<int:stock_id>', methods=['PUT'])
def update_monitor_stock_by_id(stock_id):
    """按记录 ID 更新监控股票配置，避免历史重复代码导致误更新。"""
    try:
        _ensure_monitor_stock_columns_once()
        data = request.get_json() or {}
        row = _find_stock_row_by_id(stock_id)
        return _update_monitor_stock_row(row, row.get('stock_code') if row else '', data)
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@monitor_bp.route('/stocks/by-id/<int:stock_id>', methods=['DELETE'])
def remove_monitor_stock_by_id(stock_id):
    """按记录 ID 移除监控股票，避免历史重复代码导致误删除。"""
    try:
        row = _find_stock_row_by_id(stock_id)
        return _remove_monitor_stock_row(row)
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@monitor_bp.route('/stocks/by-id/<int:stock_id>/move', methods=['POST'])
def move_monitor_stock_by_id(stock_id):
    """调整监控股票顺序（上移/下移）。"""
    try:
        _ensure_monitor_stock_columns_once()
        payload = request.get_json(silent=True) or {}
        direction = str(payload.get('direction') or '').strip().lower()
        if direction not in {'up', 'down'}:
            return jsonify({'success': False, 'message': 'direction 仅支持 up/down'}), 400

        row = _find_stock_row_by_id(stock_id)
        if not row:
            return jsonify({'success': False, 'message': '股票不存在'}), 404
        if not row.get('is_monitor'):
            return jsonify({'success': False, 'message': '该股票未处于监控列表'}), 400

        _repair_monitor_sort_orders()
        ordered_rows = db_manager.execute_query(
            "SELECT id, sort_order FROM stocks WHERE is_monitor = 1 ORDER BY sort_order ASC, id ASC"
        ) or []
        index_map = {item['id']: idx for idx, item in enumerate(ordered_rows)}
        current_idx = index_map.get(stock_id)
        if current_idx is None:
            return jsonify({'success': False, 'message': '排序数据异常'}), 500

        if direction == 'up':
            target_idx = current_idx - 1
            if target_idx < 0:
                return jsonify({'success': True, 'message': '已是第一位'}), 200
        else:
            target_idx = current_idx + 1
            if target_idx >= len(ordered_rows):
                return jsonify({'success': True, 'message': '已是最后一位'}), 200

        current_row = ordered_rows[current_idx]
        target_row = ordered_rows[target_idx]
        current_order = int(current_row.get('sort_order') or (current_idx + 1))
        target_order = int(target_row.get('sort_order') or (target_idx + 1))

        db_manager.execute_update('stocks', {'sort_order': target_order}, 'id = %s', (current_row['id'],))
        db_manager.execute_update('stocks', {'sort_order': current_order}, 'id = %s', (target_row['id'],))
        _repair_monitor_sort_orders()
        _invalidate_monitor_cache()

        return jsonify({'success': True, 'message': '顺序已更新'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@monitor_bp.route('/stocks/reorder', methods=['POST'])
def reorder_monitor_stocks():
    """按拖拽顺序重排监控股票。"""
    try:
        _ensure_monitor_stock_columns_once()
        payload = request.get_json(silent=True) or {}
        stock_ids = payload.get('stock_ids')
        if not isinstance(stock_ids, list) or not stock_ids:
            return jsonify({'success': False, 'message': 'stock_ids 必须是非空数组'}), 400

        normalized_ids = []
        for stock_id in stock_ids:
            try:
                value = int(stock_id)
            except (TypeError, ValueError):
                return jsonify({'success': False, 'message': 'stock_ids 包含无效ID'}), 400
            if value <= 0:
                return jsonify({'success': False, 'message': 'stock_ids 包含无效ID'}), 400
            if value in normalized_ids:
                return jsonify({'success': False, 'message': 'stock_ids 不允许重复'}), 400
            normalized_ids.append(value)

        _repair_monitor_sort_orders()
        rows = db_manager.execute_query(
            "SELECT id FROM stocks WHERE is_monitor = 1 ORDER BY sort_order ASC, id ASC"
        ) or []
        monitor_ids = [int(row['id']) for row in rows if row.get('id')]
        if set(normalized_ids) != set(monitor_ids) or len(normalized_ids) != len(monitor_ids):
            return jsonify({'success': False, 'message': '排序列表与当前监控股票不一致，请刷新后重试'}), 409

        for idx, stock_id in enumerate(normalized_ids, start=1):
            db_manager.execute_update('stocks', {'sort_order': idx}, 'id = %s', (stock_id,))

        _repair_monitor_sort_orders()
        _invalidate_monitor_cache()
        return jsonify({'success': True, 'message': '顺序已更新'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# ==================== 告警历史记录接口 ====================


@monitor_bp.route('/alerts/divergence', methods=['POST'])
def post_divergence_alert():
    """接收前端背离信号并发送钉钉告警。"""
    try:
        payload = request.get_json(silent=True) or {}

        stock_name = str(payload.get('stock_name') or '').strip()
        stock_code = normalize_monitor_stock_code(payload.get('stock_code'), stock_name)
        if not stock_code:
            return jsonify({'success': False, 'message': '缺少有效股票代码'}), 400

        indicator = str(payload.get('indicator') or '').strip().upper()
        if indicator not in {'MACD', 'RSI'}:
            return jsonify({'success': False, 'message': 'indicator 仅支持 MACD/RSI'}), 400

        divergence_type = str(payload.get('divergence_type') or '').strip().lower()
        if divergence_type not in {'top', 'bottom'}:
            return jsonify({'success': False, 'message': 'divergence_type 仅支持 top/bottom'}), 400

        period = str(payload.get('period') or 'm30').strip().lower()
        signal_time = str(payload.get('signal_time') or '').strip()
        price = _safe_float(payload.get('price'))
        indicator_value = _safe_float(payload.get('indicator_value'))

        alert_message = _build_divergence_alert_message(
            period=period,
            indicator=indicator,
            divergence_type=divergence_type,
            signal_time=signal_time,
            price=price,
            indicator_value=indicator_value,
        )
        cooldown = _get_divergence_cooldown(period)
        stock_display_name = stock_name or get_stock_name(stock_code)

        alert_data = {
            'stock_code': stock_code,
            'stock_name': stock_display_name,
            'alert_type': '背离',
            'alert_level': 2,
            'alert_message': alert_message,
            'trigger_time': datetime.now(),
            'windows_sec': _DIVERGENCE_PERIOD_SECONDS.get(period, 0),
        }

        get_alert_sender().send_alert(stock_code, [(alert_data, cooldown)], force_send=True)

        return jsonify({
            'success': True,
            'message': '背离告警已处理',
            'data': {
                'stock_code': stock_code,
                'stock_name': stock_display_name,
                'indicator': indicator,
                'divergence_type': divergence_type,
                'period': period,
                'cooldown': cooldown,
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@monitor_bp.route('/settings/divergence', methods=['GET'])
def get_divergence_settings():
    """获取后端背离监控运行配置。"""
    try:
        checker = get_alert_checker()
        config = checker.get_divergence_config()
        return jsonify({
            'success': True,
            'data': config,
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@monitor_bp.route('/settings/divergence', methods=['PUT'])
def update_divergence_settings():
    """更新后端背离监控运行配置（立即生效并持久化）。"""
    try:
        payload = request.get_json(silent=True) or {}
        scan_interval_seconds = payload.get('scan_interval_seconds')
        kline_count = payload.get('kline_count')
        lookback = payload.get('lookback')

        try:
            if scan_interval_seconds is not None:
                scan_interval_seconds = int(scan_interval_seconds)
            if kline_count is not None:
                kline_count = int(kline_count)
            if lookback is not None:
                lookback = int(lookback)
        except (TypeError, ValueError):
            return jsonify({'success': False, 'message': '数值配置格式错误'}), 400

        checker = get_alert_checker()
        checker.update_divergence_config(
            periods=payload.get('periods'),
            scan_interval_seconds=scan_interval_seconds,
            kline_count=kline_count,
            lookback=lookback,
            persist=True,
            reset_state=True,
        )
        config = checker.get_divergence_config()
        return jsonify({
            'success': True,
            'message': '背离监控配置已更新并立即生效',
            'data': config,
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@monitor_bp.route('/alerts', methods=['GET'])
def get_alert_history():
    """获取告警历史记录"""
    try:
        _ensure_monitor_stock_columns_once()
        # 获取查询参数
        stock_code = request.args.get('stock_code')
        alert_type = request.args.get('alert_type')
        start_time = request.args.get('start_time')
        end_time = request.args.get('end_time')
        monitor_only = _to_bool(request.args.get('monitor_only'), False)
        limit = int(request.args.get('limit', 100))
        offset = int(request.args.get('offset', 0))
        limit = max(1, min(limit, 5000))
        offset = max(0, offset)
        
        # 构建查询条件
        conditions = []
        params = []
        
        if stock_code:
            stock_code = normalize_monitor_stock_code(stock_code)
            conditions.append("l.stock_code = %s")
            params.append(stock_code)
        if alert_type:
            conditions.append("l.alert_type = %s")
            params.append(alert_type)
        if start_time:
            conditions.append("l.trigger_time >= %s")
            params.append(start_time)
        if end_time:
            conditions.append("l.trigger_time <= %s")
            params.append(end_time)
        if monitor_only:
            conditions.append(
                "EXISTS (SELECT 1 FROM stocks ms WHERE ms.is_monitor = 1 AND BINARY ms.stock_code = BINARY l.stock_code)"
            )
        conditions.append(
            "NOT (l.windows_sec IN (1, 5) AND (l.alert_message LIKE %s OR l.alert_message LIKE %s))"
        )
        params.extend(['%up_down_up%', '%down_up_down%'])

        cache_key = (
            f"monitor:alerts:{stock_code or ''}:{alert_type or ''}:"
            f"{start_time or ''}:{end_time or ''}:{int(monitor_only)}:{limit}:{offset}"
        )
        cached = _cache_get(cache_key)
        if cached is not None:
            return jsonify(cached)
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        # 查询总数
        count_query = f"SELECT COUNT(*) as total FROM stock_alert_log l WHERE {where_clause}"
        count_result = db_manager.execute_query(count_query, tuple(params) if params else None)
        total = count_result[0]['total'] if count_result else 0
        
        # 查询数据
        query = f"""
            SELECT l.id, l.stock_code, l.stock_name, l.alert_type, l.alert_level, 
                   l.alert_message, l.trigger_time, l.windows_sec
            FROM stock_alert_log l
            LEFT JOIN stocks s ON BINARY s.stock_code = BINARY l.stock_code
            WHERE {where_clause}
            ORDER BY
                CASE WHEN s.sort_order IS NULL THEN 1 ELSE 0 END ASC,
                s.sort_order ASC,
                l.stock_code ASC,
                l.trigger_time DESC,
                l.id DESC
            LIMIT {limit} OFFSET {offset}
        """
        query_params = tuple(params) if params else None
        alerts = db_manager.execute_query(query, query_params)
        if not alerts and total > 0 and offset < total:
            # 兼容旧库排序字段/字符集异常时的兜底查询，避免历史页空白。
            fallback_query = f"""
                SELECT l.id, l.stock_code, l.stock_name, l.alert_type, l.alert_level, 
                       l.alert_message, l.trigger_time, l.windows_sec
                FROM stock_alert_log l
                WHERE {where_clause}
                ORDER BY l.trigger_time DESC, l.id DESC
                LIMIT {limit} OFFSET {offset}
            """
            alerts = db_manager.execute_query(fallback_query, query_params)
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


@monitor_bp.route('/alerts/stocks', methods=['GET'])
def get_alert_history_stocks():
    """获取告警历史涉及的股票列表（用于前端筛选下拉）"""
    try:
        limit = int(request.args.get('limit', 5000))
        limit = max(1, min(limit, 10000))

        cache_key = f"monitor:alerts:stocks:{limit}"
        cached = _cache_get(cache_key)
        if cached is not None:
            return jsonify(cached)

        query = """
            SELECT
                l.stock_code,
                COALESCE(
                    NULLIF(MAX(COALESCE(l.stock_name, '')), ''),
                    l.stock_code
                ) AS stock_name,
                MAX(l.trigger_time) AS latest_trigger_time
            FROM stock_alert_log l
            WHERE l.stock_code IS NOT NULL AND l.stock_code <> ''
            GROUP BY l.stock_code
            ORDER BY latest_trigger_time DESC
            LIMIT %s
        """
        rows = db_manager.execute_query(query, (limit,)) or []

        dedup = {}
        for row in rows:
            raw_code = str(row.get('stock_code') or '').strip()
            raw_name = str(row.get('stock_name') or '').strip()
            normalized_code = normalize_monitor_stock_code(raw_code, raw_name)
            if not normalized_code:
                continue

            existing = dedup.get(normalized_code)
            if existing:
                if not existing.get('stock_name') and raw_name:
                    existing['stock_name'] = raw_name
                continue

            dedup[normalized_code] = {
                'stock_code': normalized_code,
                'stock_name': raw_name,
            }

        stock_list = sorted(dedup.values(), key=lambda item: item['stock_code'])
        payload = {
            'success': True,
            'data': stock_list,
            'total': len(stock_list),
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
        monitor_count = _reload_monitor_runtime()
        
        return jsonify({
            'success': True,
            'message': '配置已重新加载并自动生效',
            'monitor_count': monitor_count
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

