import logging
import base64
import io
import logging
import random
import sys
import threading

import requests
import urllib3
from PIL import Image
from flask import Flask, request, jsonify
from flask_cors import CORS
from urllib3.exceptions import InsecureRequestWarning

from utils.tushare_utils import IndexAnalysis

# 在app.py开头添加
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# 禁用 HTTPS 不安全请求警告
urllib3.disable_warnings(InsecureRequestWarning)# 添加项目根目录到 Python 路径
# sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

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


if __name__ == "__main__" or __name__ == 'app':
    monitor_thread = threading.Thread(target=monitor.start_monitoring, daemon=True)
    monitor_thread.start()

    app.run(host='0.0.0.0', port=5000)
