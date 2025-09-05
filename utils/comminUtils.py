import uuid


def get_uuid():
    # 生成一个基于主机ID、序列号和当前时间的UUID，具有唯一性
    random_uuid = uuid.uuid1()
    random_string = str(random_uuid).replace('-', '')  # 去掉连字符，得到32位字符串
    return random_string[:6]
