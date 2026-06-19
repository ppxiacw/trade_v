import concurrent.futures
import logging
import os
import threading
import time
from monitor.config.market_calendar import (
    get_market_state,
    is_market_open,
    seconds_until_next_trading_check,
)
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
        return get_market_state(now_dt)

    def is_market_open(self, now_dt=None):
        return is_market_open(now_dt)

    def _seconds_until_next_trading_check(self, now_dt=None):
        return seconds_until_next_trading_check(
            now_dt,
            max_sleep_seconds=self._non_trading_max_sleep_seconds,
        )

    def _maybe_log_non_trading(self, state, sleep_seconds, now_dt=None):
        now_ts = time.time()
        should_log = (
            self._last_market_state != state or
            (now_ts - self._last_closed_log_ts) >= self._closed_log_interval_seconds
        )
        if not should_log:
            return
        state_text = '午休' if state == 'lunch_break' else '休市'
        if state == 'closed':
            from monitor.config.market_calendar import is_trading_day
            check_dt = (now_dt or now_in_market_tz()).replace(tzinfo=None)
            if not is_trading_day(check_dt):
                state_text = '节假日休市'
        logging.info("市场%s，监控进入低频待机，%s 秒后重试", state_text, sleep_seconds)
        self._last_closed_log_ts = now_ts

    def start_monitoring(self):
        logging.info(f"开始监控 {len(self.config.MONITOR_STOCKS)} 个股票")
        # 使用线程池处理数据和检查
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
        last_stock_codes = None

        while True:
            stock_codes = list(self.config.MONITOR_STOCKS.keys())
            current_stock_codes = tuple(stock_codes)
            if current_stock_codes != last_stock_codes:
                logging.info("当前监控股票数已更新为 %s", len(stock_codes))
                last_stock_codes = current_stock_codes

            now_dt = now_in_market_tz().replace(tzinfo=None)
            market_state = self._get_market_state(now_dt)
            force_monitoring = self._is_force_monitoring_enabled()

            # 非交易时段：默认低频待机，不做行情拉取与告警检查。
            if market_state != 'open' and not force_monitoring:
                sleep_seconds = self._seconds_until_next_trading_check(now_dt)
                self._maybe_log_non_trading(market_state, sleep_seconds, now_dt)
                self._last_market_state = market_state
                time.sleep(sleep_seconds)
                continue

            if not stock_codes:
                time.sleep(max(1, self.config.BASE_INTERVAL))
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
        if stock not in self.config.MONITOR_STOCKS:
            return
        if not self._is_force_monitoring_enabled() and not self.is_market_open():
            return
        alerts = self.alert_checker.check_all_conditions(stock)
        if alerts:
            self.alert_sender.send_alert(stock, alerts)
