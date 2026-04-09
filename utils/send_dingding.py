import os

import requests
import random
import re
import logging

_DEFAULT_MA_WEBHOOK_URL = 'https://oapi.dingtalk.com/robot/send?access_token=79b1100719c51a60877658bd24e1cdc9d758f55a678a5bf4f4061b8a924d6331'
_DEFAULT_COMMON_WEBHOOK_URL = 'https://oapi.dingtalk.com/robot/send?access_token=d1c41a2a5bc285a143e535843c4633382ae43db2f19fc98811387bbe6ab0762e'
_REQUEST_TIMEOUT = (
    float(os.getenv('DINGTALK_CONNECT_TIMEOUT_SECONDS', '3')),
    float(os.getenv('DINGTALK_READ_TIMEOUT_SECONDS', '10')),
)
ma_webhook_url = os.getenv('DINGTALK_WEBHOOK_MA', _DEFAULT_MA_WEBHOOK_URL)
common = os.getenv('DINGTALK_WEBHOOK_COMMON', _DEFAULT_COMMON_WEBHOOK_URL)


def send_dingtalk_message(title, tsCode, webhook_url=common):
    headers = {'Content-Type': 'application/json'}
    image_url1 = generate_stock_image_url(tsCode)
    image_url2 = generate_stock_image_url(tsCode, 'mink5')
    if 'ma' in title:
        webhook_url = ma_webhook_url
    data = {
        "msgtype": "actionCard",
        "actionCard": {
            "title": f"{title}\n\n !",
            "text": f"{title}\n\n ![走势缩略图1]({image_url1}) \n\n ![走势缩略图2]({image_url2})",
            "btns": [
                {
                    "title": "分时图",
                    "actionURL": image_url1
                },
                {
                    "title": "五分图",
                    "actionURL": image_url2
                }
            ],
            "btnOrientation": "1"  # 设置按钮垂直排列，如果按钮多的话
        }
    }
    logging.info(title + "\n")
    if os.getenv('ENABLE_REQUESTS') is None:
        try:
            response = requests.post(
                webhook_url,
                headers=headers,
                json=data,
                verify=False,
                timeout=_REQUEST_TIMEOUT,
            )
            if response.status_code >= 400:
                logging.warning("钉钉推送失败 status=%s body=%s", response.status_code, response.text[:200])
        except Exception as e:
            logging.warning("钉钉推送异常: %s", e)


def generate_stock_image_url(stock_code: str, k_type='min') -> str:
    """
    生成新浪股票日线图 URL（支持 000001.SH 格式）

    :param stock_code: 股票代码（格式如 000001.SH 或 300750.SZ）
    :return: 图片 URL
    :raises ValueError: 格式错误时抛出异常
    """
    # 格式校验（6位数字 + .SH/.SZ）
    if not re.match(r'^\d{6}\.(SH|SZ)$', stock_code, re.IGNORECASE):
        raise ValueError("股票代码格式错误，应为 6位数字 + .SH/.SZ，例如：000001.SH")

    # 分割代码和交易所
    code_part, exchange_part = stock_code.upper().split('.')

    # 转换为新浪需要的格式（sh/sz + 代码）
    sina_code = f"{exchange_part.lower()}{code_part}"

    # 生成 URL
    return f"http://image.sinajs.cn/newchart/{k_type}/n/{sina_code}.gif?t={random.randint(0, 99999)}"

# send_dingtalk_message('00001','000001.SH')
