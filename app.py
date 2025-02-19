import threading

from flask import Flask
from pattern.TestShape import find_bottom_line,find_new_high
import logging

app = Flask(__name__)

# 设置日志级别
logging.basicConfig(level=logging.INFO)


from apscheduler.schedulers.background import BackgroundScheduler

def job():
    print("精确到秒的定时任务")

# 定义一个定时任务函数，并添加异常捕获


@app.route('/find_bottom_line')
def find():
    value = find_bottom_line()
    return value



@app.route('/find_new_high')
def find_nwe_high():
    value = find_new_high()
    return value


if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    scheduler.add_job(job, 'interval', seconds=2)

    try:
        scheduler.start()
        app.logger.info("✅ 调度器已启动 (任务数: %d)", len(scheduler.get_jobs()))
    except Exception as e:
        app.logger.error("❗ 调度器启动失败: %s", e)

    app.run(debug=False, use_reloader=False)