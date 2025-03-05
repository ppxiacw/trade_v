import logging
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from pattern.TestShape import find_bottom_line, find_new_high
from apscheduler.triggers.cron import CronTrigger
from trade_schedule import AppendMarketData, UpdateFiles
from config.send_dingding import  send_dingtalk_message

# 全局初始化调度器
scheduler = BackgroundScheduler(timezone="Asia/Shanghai")

stored_new_high_codes = set()

def create_app():
    app = Flask(__name__)
    app.logger.setLevel(logging.INFO)

    # 注册路由
    @app.route('/find_bottom_line')
    def find_bottom_line_route():
        return find_bottom_line()

    @app.route('/find_new_high')
    def find_new_high_route():
        app.logger.info("开始执行 find_new_high_route")
        current_stocks = find_new_high()
        new_stocks = current_stocks - stored_new_high_codes
        if new_stocks:
            app.logger.info(f"发现新的股票代码: {new_stocks}")
            stored_new_high_codes.update(new_stocks)
            send_dingtalk_message(list(new_stocks))
        else:
            app.logger.info("没有发现新的股票代码")
        return str(new_stocks)

    # 初始化调度器（仅在应用上下文中执行一次）
    with app.app_context():
        if not scheduler.running:
            # 初始化调度器（设置中国时区）
            scheduler.add_job(find_new_high_route, 'interval', seconds=30)

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
    with app.app_context():
        app.logger.info("开始执行 schedule_flushFile")
        UpdateFiles.new_high_()

def schedule_append_market_data():
    with app.app_context():
        app.logger.info("开始执行 schedule_append_market_data")
        AppendMarketData.append_market_data()

app = create_app()

if __name__ == "__main__":
    app.run(debug=True, use_reloader=True)