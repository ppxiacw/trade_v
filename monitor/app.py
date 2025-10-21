import sys
import os
from flask import Flask, jsonify
import urllib3
from urllib3.exceptions import InsecureRequestWarning

from flask_cors import CORS

# 禁用 HTTPS 不安全请求警告
urllib3.disable_warnings(InsecureRequestWarning)# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import Config
from models.stock_data import StockData
from services.data_fetcher import DataFetcher
from services.alert_checker import AlertChecker
from services.alert_sender import AlertSender
from services.stock_monitor import StockMonitor
from services.volume_radio import get_volume_ratio_simple
from utils.tushare_utils import IndexAnalysis

import threading

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
    get_volume_ratio_simple(stock_codes)
    return 'success'

@app.route('/api/ma', methods=['GET'])
@app.route('/api/ma/<string:stock_codes>', methods=['GET'])
def calculate_ma_distances(stock_codes=None):
    if stock_codes is None:
        stock_codes = list(config.CONFIG_LIST.keys())
    else:
        stock_codes = stock_codes.split(',')
    v = alert_checker.calculate_ma_distances(stock_codes)
    return v


@app.route('/api/reload_config')
def reload_config():
    # 这里需要实现重新加载配置的逻辑
    config.reload_config()
    stock_data.initialize_data_storage()
    return "配置重载功能"

    # 启动监控线程

if __name__ == "__main__" or "monitor.app":
    monitor_thread = threading.Thread(target=monitor.start_monitoring, daemon=True)
    monitor_thread.start()

    app.run(host='0.0.0.0', port=5000)
