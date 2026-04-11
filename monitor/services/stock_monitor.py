import concurrent.futures
import logging
import os
import threading
import time
from datetime import datetime, time as dt_time, timedelta
from monitor.config.market_time import now_in_market_tz


class StockMonitor:
    def __init__(self, config, data_fetcher, alert_checker, alert_sender, stock_data):
        self.config = config
        self.data_fetcher = data_fetcher
        self.alert_checker = alert_checker
        self.alert_sender = alert_sender
        self.stock_data = stock_data
        self.lock = threading.Lock()
        self._last_market_state = None
        self._last_closed_log_ts = 0.0
        self._closed_log_interval_seconds = int(
            os.getenv('MARKET_CLOSED_LOG_INTERVAL_SECONDS', '1800')
        )
        self._non_trading_max_sleep_seconds = int(
            os.getenv('NON_TRADING_MAX_SLEEP_SECONDS', '300')
        )

    def _is_force_monitoring_enabled(self):
        return os.getenv('ENABLE_REQUESTS') is not None

    def _get_market_state(self, now_dt=None):
        """返回市场状态：open / lunch_break / closed"""
        now_dt = (now_dt or now_in_market_tz()).replace(tzinfo=None)
        now = dt_time(now_dt.hour, now_dt.minute, now_dt.second)
        current_weekday = now_dt.weekday()

        # 周末休市
        if current_weekday >= 5:
            return 'closed'

        # 交易时段：9:30-11:30 和 13:00-15:00
        morning_session = dt_time(9, 30) <= now < dt_time(11, 30)
        afternoon_session = dt_time(13, 0) <= now < dt_time(15, 0)
        if morning_session or afternoon_session:
            return 'open'
        if dt_time(11, 30) <= now < dt_time(13, 0):
            return 'lunch_break'
        return 'closed'

    def is_market_open(self, now_dt=None):
        """检查当前时间是否在股票市场开盘时间内"""
        return self._get_market_state(now_dt) == 'open'

    def _seconds_until_next_trading_check(self, now_dt=None):
        """
        计算下次检查交易时段的等待秒数。
        - 非交易时段尽量睡到下一关键时刻附近，避免每分钟空转。
        - 上限由 NON_TRADING_MAX_SLEEP_SECONDS 控制，防止睡太久影响响应。
        """
        now_dt = (now_dt or now_in_market_tz()).replace(tzinfo=None)
        state = self._get_market_state(now_dt)
        now_date = now_dt.date()

        def _seconds_until(target_dt):
            return max(5, int((target_dt - now_dt).total_seconds()))

        if state == 'lunch_break':
            next_dt = datetime.combine(now_date, dt_time(13, 0))
            return min(_seconds_until(next_dt), self._non_trading_max_sleep_seconds)

        if state == 'closed':
            # 当天收盘后或开盘前，目标是下一个交易时段开始（优先次日 9:30）
            if now_dt.time() < dt_time(9, 30):
                next_dt = datetime.combine(now_date, dt_time(9, 30))
            else:
                next_dt = datetime.combine(now_date + timedelta(days=1), dt_time(9, 30))
                while next_dt.weekday() >= 5:  # 跳过周末
                    next_dt += timedelta(days=1)
            return min(_seconds_until(next_dt), self._non_trading_max_sleep_seconds)

        return max(1, int(self.config.BASE_INTERVAL))

    def _maybe_log_non_trading(self, state, sleep_seconds):
        now_ts = time.time()
        should_log = (
            self._last_market_state != state or
            (now_ts - self._last_closed_log_ts) >= self._closed_log_interval_seconds
        )
        if not should_log:
            return
        state_text = '午休' if state == 'lunch_break' else '收盘'
        logging.info("市场%s，监控进入低频待机，%s 秒后重试", state_text, sleep_seconds)
        self._last_closed_log_ts = now_ts

    def start_monitoring(self):
        logging.info(f"开始监控 {len(self.config.MONITOR_STOCKS)} 个股票")
        stock_codes = list(self.config.MONITOR_STOCKS.keys())

        # 使用线程池处理数据和检查
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)

        while True:
            now_dt = now_in_market_tz().replace(tzinfo=None)
            market_state = self._get_market_state(now_dt)
            force_monitoring = self._is_force_monitoring_enabled()

            # 非交易时段：默认低频待机，不做行情拉取与告警检查。
            if market_state != 'open' and not force_monitoring:
                sleep_seconds = self._seconds_until_next_trading_check(now_dt)
                self._maybe_log_non_trading(market_state, sleep_seconds)
                self._last_market_state = market_state
                time.sleep(sleep_seconds)
                continue

            if market_state != self._last_market_state:
                if market_state == 'open':
                    logging.info("市场开盘，恢复实时监控")
                elif force_monitoring:
                    logging.info("非交易时段，但 ENABLE_REQUESTS 已设置，继续监控")
                self._last_market_state = market_state

            try:
                # 获取数据
                data_list = self.data_fetcher.fetch_realtime_data(stock_codes)
                if not data_list:
                    time.sleep(max(1, self.config.BASE_INTERVAL))
                    continue

                # 并行处理数据更新和警报检查
                with self.lock:
                    self.stock_data.update_data(data_list)

                futures = []
                for stock in stock_codes:
                    futures.append(executor.submit(self.check_and_send_alerts, stock))

                # 等待所有检查完成
                concurrent.futures.wait(futures)

            except Exception as e:
                logging.error(f"监控出错: {e}")

            # 每次循环后等待一段时间，避免请求过于频繁
            time.sleep(self.config.BASE_INTERVAL)




    def check_and_send_alerts(self, stock):
        """并行检查并发送警报的辅助方法"""
        if not self._is_force_monitoring_enabled() and not self.is_market_open():
            return
        alerts = self.alert_checker.check_all_conditions(stock)
        if alerts:
            self.alert_sender.send_alert(stock, alerts)
