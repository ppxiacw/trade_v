"""
交易系统后端主入口
"""
import io
import sys
import threading

import urllib3
from flask import Flask
from flask_cors import CORS
from urllib3.exceptions import InsecureRequestWarning

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

# ==================== 应用初始化1 ====================

app = Flask(__name__)
CORS(app)

# 初始化配置和组件
config = Config()
data_fetcher = DataFetcher(config, config.DEBUG_MODE)
stock_data = StockData(config, data_fetcher)
alert_checker = AlertChecker(config, stock_data)
alert_sender = AlertSender(config)

# 创建监控器
monitor = StockMonitor(config, data_fetcher, alert_checker, alert_sender, stock_data)

# ==================== 注册路由 ====================

from routes import register_routes
register_routes(app)


# ==================== 启动应用 ====================

if __name__ == "__main__" or __name__ == 'app':
    # 启动监控线程（开市时启用）
    # monitor_thread = threading.Thread(target=monitor.start_monitoring, daemon=True)
    # monitor_thread.start()

    # 启动Flask应用（启用多线程以支持并发请求）
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
