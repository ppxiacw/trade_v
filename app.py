import logging
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from pattern.TestShape import find_bottom_line, find_new_high
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from trade_schedule import AppendMarketData,UpdateFiles


# 全局初始化调度器
scheduler = BackgroundScheduler(timezone="Asia/Shanghai")


def create_app():
    app = Flask(__name__)
    app.logger.setLevel(logging.INFO)

    # 注册路由
    @app.route('/find_bottom_line')
    def find_bottom_line_route():
        return find_bottom_line()

    @app.route('/find_new_high')
    def find_new_high_route():
        return find_new_high()

    # 初始化调度器（仅在应用上下文中执行一次）
    with app.app_context():
        if not scheduler.running:
            # 初始化调度器（设置中国时区）
            scheduler.add_job(find_new_high, 'interval', seconds=30)

            scheduler.add_job(
                schedule_flushFile,
                trigger=CronTrigger(hour=16, minute=36),
                id="daily_4pm_schedule_flushFile"
            )
            # 添加每天16:00执行的任务
            scheduler.add_job(
                schedule_append_market_data,
                trigger=CronTrigger(hour=16, minute=36),
                id="daily_4pm_schedule_append_market_data"
            )

            scheduler.start()

    return app


def schedule_flushFile():
    with app.app_context():  # 必须包裹应用上下文
        print("flushing file")
        UpdateFiles.new_high_()

def schedule_append_market_data():
    with app.app_context():  # 必须包裹应用上下文
        print("appending market data")
        AppendMarketData.append_market_data()

app = create_app()

if __name__ == "__main__":
    app.run(debug=False, use_reloader=False)
