from datetime import datetime
from utils.tushare_utils import IndexAnalysis
from utils.comminUtils import get_uuid


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

        # 检查固定价格阈值条件
        price_threshold_alerts = self._check_price_thresholds(stock)
        alerts.extend(price_threshold_alerts)

        # 检查涨跌幅百分比阈值条件
        change_threshold_alerts = self._check_change_thresholds(stock)
        alerts.extend(change_threshold_alerts)

        ma_alerts = self._check_ma_breakdown(stock)
        alerts.extend(ma_alerts)

        # 检查时间窗口内涨跌条件
        for window_sec in self.config.MONITOR_STOCKS.get(stock, {}).get("windows_sec", []):
            conditions = self._check_time_window_conditions(stock, window_sec)
            alerts.extend(conditions)

        if self.config.MONITOR_STOCKS[stock].get("common",True):
            common_alerts, minutes = self._check_common(stock)
            # 将 common_alerts 中值为 True 的键添加到警报列表
            for alert_type, is_triggered in common_alerts.items():
                if is_triggered:
                    # 可以根据需要格式化警报消息
                    alert_message = f"{stock}: {alert_type} {minutes}"
                    alerts.append(alert_message)

        return alerts

    def _check_time_window_conditions(self, stock, window_sec):
        triggered_conditions = []
        candles = self.stock_data.get_stock_data(stock)
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

    def _check_ma_breakdown(self, stock):
        triggered_alerts = []

        """监控股票是否跌破均线"""
        if not self.config.MONITOR_STOCKS[stock].get("break_ma", True):
            return triggered_alerts
        candles = self.stock_data.get_stock_data(stock)
        ma = IndexAnalysis.get_ma(stock)[:1]
        if not candles:
            return triggered_alerts

        # 获取当前价格
        current_price = candles[-1]['close']

        # 获取所有均线配置
        ma_types = self.config.MONITOR_STOCKS[stock].get("ma_types", [5, 10, 20, 30, 60, 120])

        for ma_type in ma_types:
            ma_key = f"ma{ma_type}"

            # 检查是否存在该均线数据
            if ma_key not in ma:
                continue

            ma_value = ma[ma_key]
            if len(ma_value) == 0:
                return []
            # 如果当前价格小于等于均线值
            if current_price <= ma_value[0] < candles[-1]['pre_close']:
                # 生成告警消息
                message = f"{stock} break{ma_type}ma"

                triggered_alerts.append(message)

        return triggered_alerts

    def _check_change_thresholds(self, stock):
        triggered_alerts = []
        candles = self.stock_data.get_stock_data(stock)

        if not candles:
            return triggered_alerts

        current_price = candles[-1]['close']
        pre_close = candles[-1]['pre_close']

        change_percent = (current_price - pre_close) / pre_close * 100

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

    def _check_common(self, stock):
        # 获取1分钟K线数据 todo 加入五分钟的
        results_1_min = IndexAnalysis.rt_min(stock, 1)

        # 获取最后三根K线数据
        last_three = results_1_min.iloc[-3:]
        last_k = last_three.iloc[-1]  # 最后一根K线
        prev_k = last_three.iloc[-2]  # 倒数第二根K线
        prev_prev_k = last_three.iloc[-3]  # 倒数第三根K线

        # 初始化结果字典
        results = {
            "sudden_volume": False,  # 是否突然放巨量
            "up_down_up": False,  # 阳-阴-阳组合
            "down_up_down": False,  # 阴-阳-阴组合
            "engulfing": False  # 吞没形态
        }

        # 1. 检查是否突然放巨量（当前量能是过去平均的3倍）
        avg_volume = results_1_min['amount'].mean()

        # 当前成交量超过平均成交量的3倍
        results["sudden_volume"] = last_k['amount'] > 3 * avg_volume and last_k['amount'] > 3 * prev_k['amount']

        # 2. 检查阳线-阴线-阳线组合
        # 形态要求：阳线 → 阴线 → 阳线
        # 量能要求：两个阳线的成交量都大于中间的阴线
        if (prev_prev_k['close'] > prev_prev_k['open'] and  # 第一根阳线
                prev_k['close'] < prev_k['open'] and  # 第二根阴线
                last_k['close'] > last_k['open']):  # 第三根阳线

            # 检查成交量：两个阳线成交量都大于中间的阴线
            if (prev_prev_k['amount'] > prev_k['amount'] and
                    last_k['amount'] > prev_k['amount']):
                results["up_down_up"] = True

        # 3. 检查阴线-阳线-阴线组合
        # 形态要求：阴线 → 阳线 → 阴线
        # 量能要求：两个阴线的成交量都大于中间的阳线
        if (prev_prev_k['close'] < prev_prev_k['open'] and  # 第一根阴线
                prev_k['close'] > prev_k['open'] and  # 第二根阳线
                last_k['close'] < last_k['open']):  # 第三根阴线

            # 检查成交量：两个阴线成交量都大于中间的阳线
            if (prev_prev_k['amount'] > prev_k['amount'] and
                    last_k['amount'] > prev_k['amount']):
                results["down_up_down"] = True

        # 4. 检查吞没形态（阳包阴或阴包阳）
        # 阳包阴：当前阳线实体完全包裹前一根阴线实体
        if (last_k['open'] <= prev_k['close'] < prev_k['open'] <= last_k['close'] and  # 前一根是阴线
                last_k['close'] > last_k['open']):
            results["engulfing"] = True

        # 阴包阳：当前阴线实体完全包裹前一根阳线实体
        elif (last_k['open'] >= prev_k['close'] > prev_k['open'] >= last_k['close'] and  # 前一根是阳线
              last_k['close'] < last_k['open']):
            results["engulfing"] = True

        return results, '1min' + str(last_k['candle_end_time'].minute)
