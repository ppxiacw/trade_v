import logging
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from pattern.TestShape import find_bottom_line,find_shrinkage,find_shirnkage_after
from apscheduler.triggers.cron import CronTrigger
from trade_schedule import AppendMarketData

# 全局初始化调度器
scheduler = BackgroundScheduler(timezone="Asia/Shanghai")

stored_new_high_codes = set()
stored_shrinkage_codes = set()

def create_app():
    app = Flask(__name__)
    app.logger.setLevel(logging.INFO)

    # 注册路由
    @app.route('/find_bottom_line')
    def find_bottom_line_route():
        return find_bottom_line()




    @app.route('/find_shrinkage')
    def find_shrinkage_route():
        app.logger.info("开始执行 find_shrinkage")
        return find_shrinkage()


    @app.route('/shirnkage_after')
    def find_shrinkage_after_route():
        app.logger.info("开始执行 find_shrinkage_after")
        return find_shirnkage_after()

    # 初始化调度器（仅在应用上下文中执行一次）
    with app.app_context():
        if not scheduler.running:
            # 添加每天16:00执行的任务
            scheduler.add_job(
                schedule_append_market_data,
                trigger=CronTrigger(hour=16, minute=36),
                id="daily_4pm_schedule_append_market_data"
            )

            scheduler.start()

    return app


def schedule_append_market_data():
    with app.app_context():
        app.logger.info("开始执行 schedule_append_market_data")
        AppendMarketData.append_market_data()

app = create_app()

if __name__ == "__main__":
    app.run(debug=True, use_reloader=True)