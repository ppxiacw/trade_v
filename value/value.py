from datetime import datetime

current_datetime = datetime.now()

# 格式化日期和时间为 'yy-mm' 格式
today = current_datetime.strftime('%Y%m%d')
today_ = current_datetime.strftime('%Y-%m-%d')