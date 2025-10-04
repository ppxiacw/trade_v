import logging
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from pattern.TestShape import find_bottom_line,find_shrinkage,find_shirnkage_after
from apscheduler.triggers.cron import CronTrigger
from trade_schedule import AppendMarketData
from utils.tushare_utils import IndexAnalysis

# 全局初始化调度器
scheduler = BackgroundScheduler(timezone="Asia/Shanghai")

stored_new_high_codes = set()
stored_shrinkage_codes = set()

def create_app():
    app = Flask(__name__)
    app.logger.setLevel(logging.INFO)

    # 注册路由
    @app.route('/rt_min')
    def rt_min():
        return IndexAnalysis.rt_min('000001.SH',1)




    @app.route('/find_shrinkage')
    def find_shrinkage_route():
        app.logger.info("开始执行 find_shrinkage")
        return find_shrinkage()



    return app


def schedule_append_market_data():
    with app.app_context():
        app.logger.info("开始执行 schedule_append_market_data")
        AppendMarketData.append_market_data()

app = create_app()

if __name__ == "__main__":
    app.run(debug=True, use_reloader=True)

