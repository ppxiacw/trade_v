from datetime import datetime


class AlertChecker:
    def __init__(self, config, stock_data):
        self.config = config
        self.stock_data = stock_data
        self.price_threshold_alerted = {}
        self.change_threshold_alerted = {}

        for stock in self.config.MONITOR_STOCKS.keys():
            self.price_threshold_alerted[stock] = {}
            self.change_threshold_alerted[stock] = {}

    def check_all_conditions(self, stock):
        alerts = []

        # 检查价格阈值条件
        price_threshold_alerts = self._check_price_thresholds(stock)
        alerts.extend(price_threshold_alerts)

        # 检查涨跌幅阈值条件
        change_threshold_alerts = self._check_change_thresholds(stock)
        alerts.extend(change_threshold_alerts)

        # 检查时间窗口条件
        for window_sec in self.config.MONITOR_STOCKS[stock]["windows_sec"]:
            conditions = self._check_time_window_conditions(stock, window_sec)
            alerts.extend(conditions)

        return alerts

    def _check_time_window_conditions(self, stock, window_sec):
        triggered_conditions = []
        candles = self.stock_data.get_stock_data(stock)

        window_str = str(window_sec)
        thresholds = self.config.MONITOR_STOCKS[stock]["thresholds"].get(window_str, {})

        price_alerts = self._check_price_movement(candles, window_sec, self.config.MONITOR_STOCKS[stock])
        triggered_conditions.extend(price_alerts)

        return triggered_conditions

    def _check_price_movement(self, price_array, window_sec, thresholds_config):
        triggered_alerts = []

        if len(price_array) == 0:
            return triggered_alerts

        window_str = str(window_sec)
        thresholds = thresholds_config["thresholds"].get(window_str, {})
        window_length = window_sec // self.config.BASE_INTERVAL

        recent_prices = price_array[-window_length:] if len(price_array) >= window_length else price_array
        prices = [candle['close'] for candle in recent_prices]

        highest_price = max(prices) if prices else 0
        lowest_price = min(prices) if prices else 0
        current_price = prices[-1] if prices else 0

        drawdown_from_high = (current_price - highest_price) / highest_price * 100 if highest_price else 0
        gain_from_low = (current_price - lowest_price) / lowest_price * 100 if lowest_price else 0

        # 检查下跌阈值
        price_drop_thresholds = thresholds.get("price_drop", [])
        for threshold in price_drop_thresholds:
            if drawdown_from_high <= threshold:
                alert_type = f"price_drop_{abs(threshold)}%_{window_sec}s"
                triggered_alerts.append(alert_type)

        # 检查上涨阈值
        price_rise_thresholds = thresholds.get("price_rise", [])
        for threshold in price_rise_thresholds:
            if gain_from_low >= threshold:
                alert_type = f"price_rise_{threshold}%_{window_sec}s"
                triggered_alerts.append(alert_type)

        return triggered_alerts

    def _check_price_thresholds(self, stock):
        triggered_alerts = []
        candles = self.stock_data.get_stock_data(stock)

        if not candles:
            return triggered_alerts

        current_price = candles[-1]['close']
        price_thresholds = self.config.MONITOR_STOCKS[stock].get("price_thresholds", [])

        for threshold_config in price_thresholds:
            threshold_price = threshold_config["price"]
            direction = threshold_config["direction"]
            alert_id = f"price_{direction}_{threshold_price}"

            condition_met = False
            if direction == "above" and current_price >= threshold_price:
                condition_met = True
            elif direction == "below" and current_price <= threshold_price:
                condition_met = True

            if condition_met:
                if not self.price_threshold_alerted[stock].get(alert_id, False):
                    triggered_alerts.append(f"price_threshold_{alert_id}")
                    self.price_threshold_alerted[stock][alert_id] = True
            else:
                self.price_threshold_alerted[stock][alert_id] = False

        return triggered_alerts

    def _check_change_thresholds(self, stock):
        triggered_alerts = []
        candles = self.stock_data.get_stock_data(stock)

        if not candles:
            return triggered_alerts

        current_price = candles[-1]['close']
        pre_close = candles[-1]['pre_close']

        if pre_close > 0:
            change_percent = (current_price - pre_close) / pre_close * 100
        else:
            return triggered_alerts

        change_thresholds = self.config.MONITOR_STOCKS[stock].get("change_thresholds", [])

        for threshold_config in change_thresholds:
            threshold_change = threshold_config["change"]
            direction = threshold_config["direction"]
            alert_id = f"change_{direction}_{threshold_change}"

            condition_met = False
            if direction == "above" and change_percent >= threshold_change:
                condition_met = True
            elif direction == "below" and change_percent <= threshold_change:
                condition_met = True

            if condition_met:
                if not self.change_threshold_alerted[stock].get(alert_id, False):
                    triggered_alerts.append(f"change_threshold_{alert_id}")
                    self.change_threshold_alerted[stock][alert_id] = True
            else:
                self.change_threshold_alerted[stock][alert_id] = False

        return triggered_alerts