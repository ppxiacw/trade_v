from flask import Flask
from pattern.TestShape import find_bottom_line,find_new_high
from apscheduler.schedulers.background import BackgroundScheduler
import logging

app = Flask(__name__)

# 设置日志级别
logging.basicConfig(level=logging.INFO)

# 定义一个定时任务函数，并添加异常捕获
def job():
    try:
        print("定时任务执行")
    except Exception as e:
        print(f"定时任务遇到错误: {e}")

@app.route('/find_bottom_line')
def find():
    value = find_bottom_line()
    return value



@app.route('/find_nwe_high')
def find_nwe_high():
    value = find_new_high()
    return value

if __name__ == "__main__":
    # 创建后台调度器
    scheduler = BackgroundScheduler()
    # 添加定时任务，每5秒执行一次
    scheduler.add_job(job, 'interval', seconds=5)
    scheduler.start()

    print("Scheduler started")

    try:
        # 启动Flask应用，注意debug参数
        app.run(debug=False)  # 尝试关闭调试模式，或根据需要设置为True
    except KeyboardInterrupt:
        # 关闭调度器
        scheduler.shutdown()