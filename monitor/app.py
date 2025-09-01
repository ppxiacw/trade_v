import sys
import os
from flask import Flask, jsonify
import urllib3
from urllib3.exceptions import InsecureRequestWarning

# 禁用 HTTPS 不安全请求警告
urllib3.disable_warnings(InsecureRequestWarning)# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import Config
from models.stock_data import StockData
from services.data_fetcher import DataFetcher
from services.alert_checker import AlertChecker
from services.alert_sender import AlertSender
from services.stock_monitor import StockMonitor
import threading

app = Flask(__name__)

# 初始化配置和组件
config = Config()
stock_data = StockData(config)
data_fetcher = DataFetcher(config, config.DEBUG_MODE)
alert_checker = AlertChecker(config, stock_data)
alert_sender = AlertSender(config)

# 创建监控器
monitor = StockMonitor(config, data_fetcher, alert_checker, alert_sender, stock_data)


@app.route('/api/alerts')
def get_alerts():
    return jsonify({
        'status': 'success',
        'alerts': alert_sender.get_alert_history()
    })


@app.route('/api/manual_trigger_detection')
def manual_input():
    # 这里需要实现手动触发检测的逻辑
    return "手动触发功能"


@app.route('/api/reload_config')
def reload_config():
    # 这里需要实现重新加载配置的逻辑
    return "配置重载功能"

    # 启动监控线程


if not config.DEBUG_MODE:
    monitor_thread = threading.Thread(target=monitor.start_monitoring, daemon=True)
    monitor_thread.start()

app.run(host='0.0.0.0', port=5000)
