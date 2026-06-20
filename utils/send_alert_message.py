import logging
import os

from utils.env_loader import load_local_env
from utils.send_dingding import send_dingtalk_message
from utils.send_wecom import send_wecom_message, send_wecom_webhook_message


load_local_env()

_logger = logging.getLogger(__name__)


def send_alert_message(title, ts_code, chart_period=None):
    channel = (os.getenv('ALERT_CHANNEL') or 'dingtalk').strip().lower()
    if channel in {'both', 'all', 'dingtalk_wecom', 'dingtalk+wecom', '钉钉企业微信'}:
        send_dingtalk_message(title, ts_code, chart_period=chart_period)
        send_wecom_webhook_message(title, ts_code, chart_period=chart_period)
        return
    if channel in {'wecom_webhook', 'wechat_work_webhook', 'qywx_webhook', '企业微信群机器人'}:
        send_wecom_webhook_message(title, ts_code, chart_period=chart_period)
        return
    if channel in {'wecom', 'wechat_work', 'qywx', '企业微信'}:
        send_wecom_message(title, ts_code, chart_period=chart_period)
        return
    if channel not in {'dingtalk', 'dingding', '钉钉'}:
        _logger.warning("未知告警通道 %s，回退到钉钉", channel)
    send_dingtalk_message(title, ts_code, chart_period=chart_period)
