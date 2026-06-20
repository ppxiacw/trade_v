import os

import requests
import random
import re
import logging
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_DEFAULT_MA_WEBHOOK_URL = 'https://oapi.dingtalk.com/robot/send?access_token=79b1100719c51a60877658bd24e1cdc9d758f55a678a5bf4f4061b8a924d6331'
_DEFAULT_COMMON_WEBHOOK_URL = 'https://oapi.dingtalk.com/robot/send?access_token=d1c41a2a5bc285a143e535843c4633382ae43db2f19fc98811387bbe6ab0762e'
_REQUEST_TIMEOUT = (
    float(os.getenv('DINGTALK_CONNECT_TIMEOUT_SECONDS', '3')),
    float(os.getenv('DINGTALK_READ_TIMEOUT_SECONDS', '10')),
)
ma_webhook_url = os.getenv('DINGTALK_WEBHOOK_MA', _DEFAULT_MA_WEBHOOK_URL)
common = os.getenv('DINGTALK_WEBHOOK_COMMON', _DEFAULT_COMMON_WEBHOOK_URL)

# 监控周期 -> 新浪缩略图 k_type
_ALERT_PERIOD_TO_SINA_KTYPE = {
    'm5': ('mink5', '5分钟'),
    'm15': ('mink15', '15分钟'),
    'm30': ('mink30', '30分钟'),
    'day': ('daily', '日K'),
    'week': ('weekly', '周K'),
    'month': ('monthly', '月K'),
}
_DEFAULT_CHART_PRIMARY = ('min', '分时')
_DEFAULT_CHART_SECONDARY = ('daily', '日K')


def _post_without_env_proxy(url, **kwargs):
    session = requests.Session()
    session.trust_env = False
    try:
        return session.post(url, **kwargs)
    finally:
        session.close()


def resolve_dingtalk_chart_types(chart_period=None):
    """
    根据告警周期选择钉钉附图。
    有明确周期时：主图=对应 K 线，辅图=分时；否则：分时 + 日 K。
    """
    period = str(chart_period or '').strip().lower()
    if period == 'm1':
        return ('min', '1分钟分时'), _DEFAULT_CHART_SECONDARY
    if period in _ALERT_PERIOD_TO_SINA_KTYPE:
        primary = _ALERT_PERIOD_TO_SINA_KTYPE[period]
        return primary, _DEFAULT_CHART_PRIMARY
    return _DEFAULT_CHART_PRIMARY, _DEFAULT_CHART_SECONDARY


def send_dingtalk_message(title, tsCode, chart_period=None, webhook_url=common):
    headers = {'Content-Type': 'application/json'}
    (primary_k, primary_label), (secondary_k, secondary_label) = resolve_dingtalk_chart_types(chart_period)
    image_url1 = generate_stock_image_url(tsCode, primary_k)
    image_url2 = generate_stock_image_url(tsCode, secondary_k)
    if 'ma' in title:
        webhook_url = ma_webhook_url
    data = {
        "msgtype": "actionCard",
        "actionCard": {
            "title": f"{title}\n\n !",
            "text": (
                f"{title}\n\n "
                f"![{primary_label}]({image_url1}) \n\n "
                f"![{secondary_label}]({image_url2})"
            ),
            "btns": [
                {
                    "title": primary_label,
                    "actionURL": image_url1
                },
                {
                    "title": secondary_label,
                    "actionURL": image_url2
                }
            ],
            "btnOrientation": "1"
        }
    }
    logging.info(title + "\n")
    if os.getenv('ENABLE_REQUESTS') is None:
        try:
            response = _post_without_env_proxy(
                webhook_url,
                headers=headers,
                json=data,
                verify=False,
                timeout=_REQUEST_TIMEOUT,
            )
            if response.status_code >= 400:
                logging.warning("钉钉推送失败 status=%s body=%s", response.status_code, response.text[:200])
        except Exception as e:
            logging.warning("钉钉推送异常: %s", type(e).__name__)


def generate_stock_image_url(stock_code: str, k_type='min') -> str:
    """
    生成新浪股票走势图 URL（支持 000001.SH 格式）

    :param stock_code: 股票代码（格式如 000001.SH 或 300750.SZ）
    :param k_type: 新浪图表类型，如 min、mink5、daily
    :return: 图片 URL
    :raises ValueError: 格式错误时抛出异常
    """
    if not re.match(r'^\d{6}\.(SH|SZ)$', stock_code, re.IGNORECASE):
        raise ValueError("股票代码格式错误，应为 6位数字 + .SH/.SZ，例如：000001.SH")

    code_part, exchange_part = stock_code.upper().split('.')
    sina_code = f"{exchange_part.lower()}{code_part}"
    return f"https://image.sinajs.cn/newchart/{k_type}/n/{sina_code}.gif?t={random.randint(0, 99999)}"
