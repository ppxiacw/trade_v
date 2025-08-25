import requests
import json
import random
ma_webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=79b1100719c51a60877658bd24e1cdc9d758f55a678a5bf4f4061b8a924d6331'
bottom_line_webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=bce85be747a6d8d29caa7b910b54bb442fb86fe77b7839375c4e41e71fe6fdae'
shrink_webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=d1c41a2a5bc285a143e535843c4633382ae43db2f19fc98811387bbe6ab0762e'
def send_dingtalk_message(title, tsCode,webhook_url='https://oapi.dingtalk.com/robot/send?access_token=d1c41a2a5bc285a143e535843c4633382ae43db2f19fc98811387bbe6ab0762e'):
    headers = {'Content-Type': 'application/json'}
    image_url = generate_stock_image_url(tsCode)

    data = {
        "msgtype": "actionCard",
        "actionCard": {
            "title": title,
            "text": f"**股票代码**: {tsCode} \n\n ![走势缩略图]({image_url})",
            "btns": [
                {
                    "title": "查看高清大图",
                    "actionURL": image_url  # 点击按钮跳转浏览器打开
                }
            ],
            "btnOrientation": "0"
        }
    }
    print(str(data))
    response = requests.post(webhook_url, headers=headers, json=data,verify=False)

    if response.status_code == 200:
        print("消息发送成功")
    else:
        print(f"失败状态码：{response.status_code}")

import re


def generate_stock_image_url(stock_code: str) -> str:
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
    return f"http://image.sinajs.cn/newchart/min/n/{sina_code}.png?t={random.randint(0, 99999)}"




# send_dingtalk_message('00001','000001.SH')
