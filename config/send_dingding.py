import requests
import json

webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=79b1100719c51a60877658bd24e1cdc9d758f55a678a5bf4f4061b8a924d6331'

def send_dingtalk_message( message):
    headers = {
        'Content-Type': 'application/json'
    }

    data = {
        "msgtype": "text",
        "text": {
            "content": message
        }
    }

    response = requests.post(webhook_url, headers=headers, data=json.dumps(data))

    if response.status_code == 200:
        print("消息发送成功")
    else:
        print(f"消息发送失败，状态码：{response.status_code}")


