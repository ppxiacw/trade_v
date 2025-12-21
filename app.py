import logging
import base64
import io
import logging
import random
import sys

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

def merge_kline_charts_base64(sina_code, periods, direction='vertical'):
    """
    合并K线图并返回Base64编码

    参数:
        sina_code: 股票代码，如 'sh600519'
        periods: 周期列表，如 ['daily', 'weekly', 'monthly']
        direction: 'vertical'(垂直) 或 'horizontal'(水平)

    返回:
        (success, result) - success为布尔值，result为Base64字符串或错误信息
    """
    images = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'Referer': 'https://finance.sina.com.cn/',
        'Host': 'image.sinajs.cn'
    }

    # 周期映射表（新浪接口参数）
    period_map = {
        'shf': 'min',  # 分时图
        '5min': 'mink5',  # 5分钟线
        '15min': 'mink15',  # 15分钟线
        '30min': 'mink30',  # 30分钟线
        '60min': 'mink60',  # 60分钟线
        'daily': 'daily',  # 日线
        'weekly': 'weekly',  # 周线
        'monthly': 'monthly'  # 月线
    }

    for period in periods:
        # 获取实际周期参数
        sina_period = period_map.get(period, period)
        url = f"http://image.sinajs.cn/newchart/{sina_period}/n/{sina_code}.gif"

        # 添加随机参数避免缓存
        url += f"?t={random.randint(10000, 99999)}"

        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            img = Image.open(io.BytesIO(response.content))
            if img.mode != 'RGB':
                img = img.convert('RGB')
            images.append(img)

        except Exception as e:
            logging.info(url)
            return False, f"下载{period}周期图表失败: {str(e)}"

    if not images:
        return False, "未成功加载任何图片"

    # 统一宽度并调整尺寸
    target_width = images[0].width
    resized_images = []

    for img in images:
        ratio = target_width / img.width
        new_height = int(img.height * ratio)
        resized_img = img.resize((target_width, new_height), Image.Resampling.LANCZOS)
        resized_images.append(resized_img)

    # 创建合并后的图片
    if direction == 'vertical':
        total_height = sum(img.height for img in resized_images)
        merged_image = Image.new('RGB', (target_width, total_height))
        y_offset = 0
        for img in resized_images:
            merged_image.paste(img, (0, y_offset))
            y_offset += img.height
    else:
        total_width = sum(img.width for img in resized_images)
        max_height = max(img.height for img in resized_images)
        merged_image = Image.new('RGB', (total_width, max_height))
        x_offset = 0
        for img in resized_images:
            y_offset = (max_height - img.height) // 2
            merged_image.paste(img, (x_offset, y_offset))
            x_offset += img.width

    # 转换为Base64
    buffered = io.BytesIO()
    merged_image.save(buffered, format="PNG", optimize=True, quality=90)
    img_base64 = base64.b64encode(buffered.getvalue()).decode('utf-8')

    return True, img_base64

@app.route('/api/merge-kline', methods=['GET', 'POST'])
def merge_kline():
    """合并K线图API接口"""
    try:
        if request.method == 'POST':
            data = request.get_json()
            stock_code = data.get('stock_code', 'sh600519')
            periods = data.get('periods', ['daily', 'weekly', 'monthly'])
            direction = data.get('direction', 'vertical')
        else:
            # GET请求通过查询参数获取
            stock_code = request.args.get('stock_code', 'sh600519')
            periods_str = request.args.get('periods', 'daily,weekly,monthly')
            periods = [p.strip() for p in periods_str.split(',')]
            direction = request.args.get('direction', 'vertical')

        success, result = merge_kline_charts_base64(stock_code, periods, direction)

        if success:
            return jsonify({
                'code': 200,
                'message': 'success',
                'data': {
                    'image_base64': result,
                    'stock_code': stock_code,
                    'periods': periods,
                    'direction': direction,
                    'image_type': 'png'
                }
            })
        else:
            return jsonify({
                'code': 500,
                'message': result,
                'data': None
            })

    except Exception as e:
        return jsonify({
            'code': 500,
            'message': f'服务器内部错误: {str(e)}',
            'data': None
        })

@app.route('/api/available-periods', methods=['GET'])
def get_available_periods():
    """获取可用的周期列表"""
    periods = [
        {'value': 'daily', 'label': '日线'},
        {'value': 'weekly', 'label': '周线'},
        {'value': 'monthly', 'label': '月线'},
        {'value': '60min', 'label': '60分钟'},
        {'value': '30min', 'label': '30分钟'},
        {'value': '15min', 'label': '15分钟'},
        {'value': '5min', 'label': '5分钟'},
        {'value': 'min', 'label': '分时'}
    ]
    return jsonify({'code': 200, 'data': periods})

    # 启动监控线程

if __name__ == "__main__" or __name__ == 'app':
    # monitor_thread = threading.Thread(target=monitor.start_monitoring, daemon=True)
    # monitor_thread.start()

    app.run(host='0.0.0.0', port=5000)
