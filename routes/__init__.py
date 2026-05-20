"""
路由模块初始化
统一注册所有蓝图
"""
from flask import Flask


def register_routes(app: Flask):
    """注册所有路由蓝图"""
    from .stock_routes import stock_bp
    from .order_routes import order_bp
    from .group_routes import group_bp
    from .monitor_routes import monitor_bp
    from .indicator_routes import indicator_bp
    from .no_buy_reason_routes import no_buy_reason_bp
    from .order_gate_routes import order_gate_bp
    
    # 注册蓝图，统一使用 /api 前缀
    app.register_blueprint(stock_bp, url_prefix='/api')
    app.register_blueprint(order_bp, url_prefix='/api')
    app.register_blueprint(group_bp, url_prefix='/api')
    app.register_blueprint(monitor_bp, url_prefix='/api/monitor')
    app.register_blueprint(indicator_bp, url_prefix='/api/indicator')
    app.register_blueprint(no_buy_reason_bp, url_prefix='/api')
    app.register_blueprint(order_gate_bp, url_prefix='/api')

