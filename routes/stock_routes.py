"""
股票相关路由
"""
import logging
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request
from config.runtime_config import get_app_runtime_env, get_app_runtime_version
from monitor.config.db_monitor import db_manager
from runtime_state import get_config, get_stock_data
from utils.common import format_stock_code
from services.stock_screen_service import (
    screen_stocks_by_mv_and_pct,
    load_future_events_by_stock_codes,
    load_theme_kline_data,
)
from services.daily_kline_sync_service import (
    start_daily_kline_full_sync,
    start_daily_kline_incremental_sync,
    get_daily_kline_sync_status,
)

stock_bp = Blueprint('stock', __name__)
_logger = logging.getLogger(__name__)
_APP_STARTED_AT = datetime.now(timezone.utc).isoformat(timespec='seconds')


@stock_bp.route('/health', methods=['GET'])
def health_check():
    """
    轻量健康检查：用于部署后自动验收（进程 + 数据库）。
    """
    db_ok = False
    db_error = None
    try:
        probe = db_manager.execute_query("SELECT 1 AS ok")
        db_ok = bool(probe and int((probe[0] or {}).get('ok', 0)) == 1)
    except Exception as e:
        db_error = str(e)
        _logger.warning("health_check 数据库探活失败: %s", e)

    payload = {
        'success': db_ok,
        'service': 'trade_v',
        'status': 'ok' if db_ok else 'degraded',
        'env': get_app_runtime_env(),
        'version': get_app_runtime_version(),
        'started_at': _APP_STARTED_AT,
        'db_ok': db_ok,
    }
    if db_error:
        payload['db_error'] = db_error
    return jsonify(payload), (200 if db_ok else 503)


@stock_bp.route('/version', methods=['GET'])
def get_version():
    """
    返回当前运行版本信息，用于前后端发布一致性核对。
    """
    return jsonify({
        'success': True,
        'service': 'trade_v',
        'env': get_app_runtime_env(),
        'version': get_app_runtime_version(),
        'started_at': _APP_STARTED_AT,
    })


@stock_bp.route('/get_stock_list')
def get_stock_list():
    """获取股票列表"""
    stocks = "select * from stocks order by id desc"
    result = db_manager.execute_query(stocks) or []

    for stock in result:
        stock['stock_code'] = format_stock_code(stock['stock_code'], 'prefix')
    return jsonify(result)


@stock_bp.route('/screen/mv_pct', methods=['GET'])
def screen_mv_pct():
    """
    市值+涨幅筛选：支持实时模式与历史日期模式。
    查询参数：
      min_mv_yi: 最小总市值（亿元），默认 50
      min_pct_chg: 最小涨跌幅（%），默认 0
      limit: 最大返回条数，默认 3000，最大 8000
      trade_date: 可选，历史日期（YYYY-MM-DD / YYYYMMDD）
    """
    try:
        min_mv_yi = request.args.get('min_mv_yi', default=50.0, type=float)
        min_pct_chg = request.args.get('min_pct_chg', default=0.0, type=float)
        limit = request.args.get('limit', default=3000, type=int)
        trade_date = (request.args.get('trade_date', default='', type=str) or '').strip()

        if min_mv_yi < 0:
            return jsonify({'success': False, 'message': 'min_mv_yi 不能为负数'}), 400
        if limit < 1 or limit > 8000:
            return jsonify({'success': False, 'message': 'limit 需在 1～8000 之间'}), 400

        data, meta = screen_stocks_by_mv_and_pct(
            min_mv_yi=min_mv_yi,
            min_pct_chg=min_pct_chg,
            limit=limit,
            trade_date=trade_date or None,
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
                'trade_date': trade_date or None,
            },
        })
    except ValueError as e:
        return jsonify({'success': False, 'message': str(e)}), 400
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


@stock_bp.route('/screen/future_events', methods=['POST'])
def screen_future_events():
    """
    为筛选页按股票批量补充“今日及之后”的公司大事提醒。
    """
    try:
        payload = request.get_json(silent=True) or {}
        stock_codes = payload.get('stock_codes') or []
        if not isinstance(stock_codes, list):
            return jsonify({'success': False, 'message': 'stock_codes 必须为数组'}), 400
        if len(stock_codes) > 200:
            return jsonify({'success': False, 'message': '单次最多请求 200 只股票'}), 400

        data, meta = load_future_events_by_stock_codes(stock_codes)
        return jsonify({
            'success': True,
            'data': data,
            'meta': meta,
        })
    except Exception as e:
        _logger.exception('screen_future_events 失败')
        return jsonify({
            'success': False,
            'message': f'未来事件补充失败，请稍后重试。详情: {str(e)}',
        }), 503


@stock_bp.route('/screen/theme_kline', methods=['GET'])
def screen_theme_kline():
    """
    获取板块/概念K线（站内页面使用）。
    参数：
      theme_type: industry|concept
      theme_code: BKxxxx（可选，优先）
      theme_name: 主题名称（可选）
      period: time|m1|m5|m15|m30|day|week|month
      limit: 默认240，范围20~2000
    """
    try:
        theme_type = (request.args.get('theme_type', default='industry', type=str) or '').strip()
        theme_code = (request.args.get('theme_code', default='', type=str) or '').strip()
        theme_name = (request.args.get('theme_name', default='', type=str) or '').strip()
        period = (request.args.get('period', default='day', type=str) or '').strip()
        limit = request.args.get('limit', default=240, type=int)
        if limit < 20 or limit > 2000:
            return jsonify({'success': False, 'message': 'limit 需在 20～2000 之间'}), 400
        allowed_periods = {'time', 'm1', 'm5', 'm15', 'm30', 'day', 'week', 'month'}
        if period.lower() not in allowed_periods:
            return jsonify({'success': False, 'message': 'period 仅支持 time/m1/m5/m15/m30/day/week/month'}), 400
        if not theme_code and not theme_name:
            return jsonify({'success': False, 'message': 'theme_code 和 theme_name 不能同时为空'}), 400

        rows, meta = load_theme_kline_data(
            theme_type=theme_type,
            theme_code=theme_code,
            theme_name=theme_name,
            period=period,
            limit=limit,
        )
        return jsonify({
            'success': True,
            'data': rows,
            'meta': meta,
        })
    except ValueError as e:
        return jsonify({'success': False, 'message': str(e)}), 400
    except Exception as e:
        _logger.exception('screen_theme_kline 失败')
        return jsonify({
            'success': False,
            'message': f'板块/概念K线拉取失败，请稍后重试。详情: {str(e)}',
        }), 503


@stock_bp.route('/reload_config')
def reload_config():
    """重新加载配置"""
    config = get_config()
    stock_data = get_stock_data()
    config.reload_config()
    stock_data.initialize_data_storage()
    return jsonify({'success': True, 'message': '配置重载成功'})


@stock_bp.route('/daily_kline/sync_full', methods=['POST'])
def sync_daily_kline_full():
    """
    启动全量历史日K入库任务（后台异步执行）。
    可选参数：start_date=YYYYMMDD，默认近3年
    """
    payload = request.get_json(silent=True) or {}
    start_date = payload.get('start_date')
    accepted, message = start_daily_kline_full_sync(trigger='manual', start_date=start_date)
    status = get_daily_kline_sync_status()
    return jsonify({
        'success': accepted,
        'message': message,
        'status': status,
    }), (202 if accepted else 409)


@stock_bp.route('/daily_kline/sync_incremental', methods=['POST'])
def sync_daily_kline_incremental():
    """
    启动增量日K补齐任务（后台异步执行）。
    """
    accepted, message = start_daily_kline_incremental_sync(trigger='manual')
    status = get_daily_kline_sync_status()
    return jsonify({
        'success': accepted,
        'message': message,
        'status': status,
    }), (202 if accepted else 409)


@stock_bp.route('/daily_kline/sync_status', methods=['GET'])
def sync_daily_kline_status():
    """
    查询日K同步任务状态。
    """
    return jsonify({
        'success': True,
        'status': get_daily_kline_sync_status(),
    })

