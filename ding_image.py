import requests
import tempfile
import os
from PIL import Image

def send_image_to_dingtalk(image_url, upload_url, robot_webhook):
    temp_path = None
    # 1. 下载图片到临时文件
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        response = requests.get(image_url)
        if response.status_code != 200:
            raise Exception(f"下载失败: HTTP {response.status_code}")
        tmp_file.write(response.content)
        temp_path = tmp_file.name

    # 2. 验证文件类型和完整性


    with Image.open(temp_path) as img:
        img.verify()

    # 3. 检查文件大小
    file_size = os.path.getsize(temp_path)
    if file_size > 10 * 1024 * 1024:  # 钉钉限制10MB
        raise ValueError(f"文件大小 {file_size//1024}KB 超过限制")

    # 4. 上传到钉钉（multipart/form-data）
    # with open(temp_path, 'rb') as f:
    #     files = {
    #         'media': ('C:/Users/曹威/Pictures/Screenshots/111.png', f, 'application/octet-stream')
    #         # 严格遵循钉钉要求的字段名和格式
    #     }
    #     response = requests.post(upload_url, files=files)
    #
    # upload_result = response.json()
    # if upload_result.get('errcode') != 0:
    #     raise Exception(f"上传失败: {upload_result}")

    # 5. 发送消息到钉钉群
    data = {
        "type": "file",
        "media_id": '@lALPDetfgNlJqwPNA3LNBl0',
        'msgtype':'image',
        'image':'@lALPDetfgNlJqwPNA3LNBl0'
    }
    send_response = requests.post(robot_webhook, json=data)
    return send_response.json()


# 使用示例
if __name__ == '__main__':
    image_url = "http://image.sinajs.cn/newchart/daily/n/sh600000.png"  # 替换为实际URL
    APP_KEY = "dingmlliitx4zby6ymvr"
    APP_SECRET = "tbnJ74-AgwimHZ27DXwKIL2Mxc2DLTW7ksWBcf8IGHfr5OPPOFGjilOPzMtFobNI"
    ROBOT_TOKEN = "79b1100719c51a60877658bd24e1cdc9d758f55a678a5bf4f4061b8a924d6331"

    url = f"https://oapi.dingtalk.com/gettoken?appkey={APP_KEY}&appsecret={APP_SECRET}"
    response = requests.get(url)
    access_token = response.json()["access_token"]
    upload_url = f"https://oapi.dingtalk.com/media/upload?access_token={access_token}"
    robot_webhook = f"https://oapi.dingtalk.com/message/send?access_token={access_token}"

    # result = send_image_to_dingtalk(image_url, upload_url, robot_webhook)
    # print("发送结果:", result)

    # 配置参数

    # 消息体（使用 media_id）
    payload = {
        'agentid':'3748253179',
        "msgtype": "image",
        "touser": "@all",
        "image": {
            "media_id": "@lAjPDfYH_MeeOSXOPDjb585UlYlW"  # 替换为实际 media_id
        }
    }

    # 发送请求
    response = requests.post(robot_webhook, json=payload)
    print(response.json())