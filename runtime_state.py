"""
运行时共享对象注册表。

避免在路由层 `import app` 时再次执行 `app.py`，导致监控线程和 Flask 服务被重复启动。
"""
import threading


_runtime_lock = threading.Lock()
_runtime_state = {
    'config': None,
    'stock_data': None,
    'alert_checker': None,
    'alert_sender': None,
}


def init_runtime(*, config, stock_data, alert_checker, alert_sender):
    with _runtime_lock:
        _runtime_state['config'] = config
        _runtime_state['stock_data'] = stock_data
        _runtime_state['alert_checker'] = alert_checker
        _runtime_state['alert_sender'] = alert_sender


def _get_runtime_item(key):
    value = _runtime_state.get(key)
    if value is None:
        raise RuntimeError(f'运行时对象未初始化: {key}')
    return value


def get_config():
    return _get_runtime_item('config')


def get_stock_data():
    return _get_runtime_item('stock_data')


def get_alert_checker():
    return _get_runtime_item('alert_checker')


def get_alert_sender():
    return _get_runtime_item('alert_sender')
