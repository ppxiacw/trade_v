from datetime import datetime
from utils.tushare_utils import IndexAnalysis
from utils.IndicatorCalculation import IndicatorCalculation
from utils.GetStockData import get_stock_name


class AlertChecker:
    def __init__(self, config, stock_data):
        self.config = config
        self.stock_data = stock_data
        self._last_candle_times = {}
        self._rsi_trigger_states = {}

    def check_all_conditions(self, stock):
        alerts = []

        # 检查固定价格阈值条件
        price_threshold_alerts = self._check_price_thresholds(stock)
        alerts.extend(price_threshold_alerts)

        # 检查涨跌幅百分比阈值条件
        change_threshold_alerts = self._check_change_thresholds(stock)
        alerts.extend(change_threshold_alerts)

        # ma_alerts = self._check_ma_breakdown(stock)
        # alerts.extend(ma_alerts)

        # 检查时间窗口内涨跌条件
        if self.config.MONITOR_STOCKS.get(stock, {}).get("normal_movement"):
            conditions = self._check_time_window_conditions(stock, 20)
            alerts.extend(conditions)

        if self.config.MONITOR_STOCKS[stock].get("common", False):
            periods = [1, 5, 30]  # 保持硬编码，不修改配置读取
            for period in periods:
                common_alerts = self._check_common_by_min(stock, period)
                alerts.extend(common_alerts)

        return alerts

    def _check_time_window_conditions(self, stock, window_sec):
        triggered_conditions = []
        candles = self.stock_data.get_stock_data(stock)
        price_alerts = self._check_price_movement(stock, candles, window_sec, self.config.MONITOR_STOCKS[stock])
        triggered_conditions.extend(price_alerts)
        return triggered_conditions

    def _check_price_movement(self, stock, price_array, window_sec, thresholds_config):
        if not price_array:
            return []

        window_str = str(window_sec)
        thresholds =     {
                            "price_drop": [
                              -0.7
                            ],
                            "price_rise": [
                              0.7
                            ]
                          }

        window_length = window_sec // self.config.BASE_INTERVAL

        recent_prices = price_array[-window_length:] if len(price_array) >= window_length else price_array
        prices = [candle['close'] for candle in recent_prices]

        if not prices:
            return []

        highest_price = max(prices)
        lowest_price = min(prices)
        current_price = prices[-1]

        drawdown_from_high = (current_price - highest_price) / highest_price * 100
        gain_from_low = (current_price - lowest_price) / lowest_price * 100

        return self._create_movement_alerts(stock, window_sec, drawdown_from_high, gain_from_low, thresholds)

    def _create_movement_alerts(self, stock, window_sec, drawdown, gain, thresholds):
        """创建价格变动警报"""
        alerts = []

        # 检查下跌阈值
        for threshold in thresholds.get("price_drop", []):
            if drawdown <= threshold:
                alerts.append(self._create_alert_data(
                    stock, f"price_drop_{abs(threshold)}%_{window_sec}s", window_sec
                ))

        # 检查上涨阈值
        for threshold in thresholds.get("price_rise", []):
            if gain >= threshold:
                alerts.append(self._create_alert_data(
                    stock, f"price_rise_{threshold}%_{window_sec}s", window_sec
                ))

        return alerts

    def _check_price_thresholds(self, stock):
        candles = self.stock_data.get_stock_data(stock)
        if not candles:
            return []

        current_price = candles[-1]['close']
        price_thresholds = self.config.MONITOR_STOCKS[stock].get("price_thresholds", [])
        alerts = []

        for threshold_config in price_thresholds:
            threshold_price = threshold_config["price"]
            direction = threshold_config["direction"]
            alert_id = f"price_{direction}_{threshold_price}"

            condition_met = (
                    (direction == "above" and current_price >= threshold_price) or
                    (direction == "below" and current_price <= threshold_price)
            )

            if condition_met:
                alerts.append(self._create_alert_data(stock, alert_id))

        return alerts

    def _check_ma_breakdown(self, stock):
        triggered_alerts = []
        if not self.config.MONITOR_STOCKS[stock].get("break_ma", True):
            return triggered_alerts

        candles = self.stock_data.get_stock_data(stock)
        if not candles:
            return triggered_alerts

        data = IndexAnalysis.my_pro_bar(stock)
        ma = IndexAnalysis.calculate_realtime_ma(data, candles[-1])
        current_price = candles[-1]['low']
        ma_types = self.config.MONITOR_STOCKS[stock].get("ma_types", [5, 10, 20, 30, 60, 120])

        for ma_type in ma_types:
            ma_key = f"ma{ma_type}"
            if ma_key not in ma:
                continue

            ma_value = ma[ma_key]
            if current_price <= ma_value < candles[-1]['pre_close']:
                alert_message = f"{stock} break{ma_type}ma"
                alert_data = self._create_alert_data(stock, alert_message)
                # 保留冷却时间逻辑
                triggered_alerts.append((alert_data, 1000 * 100))

        return triggered_alerts

    def _check_change_thresholds(self, stock):
        change_thresholds = self.config.MONITOR_STOCKS[stock].get("change_thresholds", [])
        if not change_thresholds:
            return []

        candles = self.stock_data.get_stock_data(stock)
        if not candles:
            return []

        current_price = candles[-1]['close']
        pre_close = candles[-1]['pre_close']
        change_percent = (current_price - pre_close) / pre_close * 100
        alerts = []

        for threshold_config in change_thresholds:
            threshold_change = threshold_config["change"]
            direction = threshold_config["direction"]
            alert_id = f"change_{direction}_{threshold_change}"

            condition_met = (
                    (direction == "above" and change_percent >= threshold_change) or
                    (direction == "below" and change_percent <= threshold_change)
            )

            if condition_met:
                alert_message = f"change_threshold_{alert_id}"
                alerts.append(self._create_alert_data(stock, alert_message))

        return alerts

    def _check_common_by_min(self, stock, window=1):
        if not self._is_new_candle_data(stock, window):
            return []

        results_min = IndexAnalysis.rt_min(stock, window)
        if len(results_min) < 4:
            return []

        return self._analyze_technical_patterns(stock, window, results_min)

    def _is_new_candle_data(self, stock, window):
        """检查是否有新的K线数据"""
        results_min = IndexAnalysis.rt_min(stock, window)
        if results_min.empty:
            return False

        current_time = results_min.iloc[-1]['candle_end_time']
        state_key = f"{stock}_{window}"
        last_time = self._last_candle_times.get(state_key)

        if last_time is None or current_time > last_time:
            self._last_candle_times[state_key] = current_time
            return True
        return False

    def _analyze_technical_patterns(self, stock, window, results_min):
        """分析技术形态模式"""
        alerts = []
        last_four = results_min.iloc[-4:]
        last_k, prev_k, prev_prev_k = last_four.iloc[-2], last_four.iloc[-3], last_four.iloc[-4]

        # RSI分析
        rsi_alerts = self._analyze_rsi_patterns(stock, window, results_min, last_k, prev_k, prev_prev_k)
        alerts.extend(rsi_alerts)

        # K线形态分析
        pattern_alerts = self._analyze_candle_patterns(stock, window, last_k, prev_k, prev_prev_k)
        alerts.extend(pattern_alerts)

        return alerts

    def _analyze_rsi_patterns(self, stock, window, results_min, last_k, prev_k, prev_prev_k):
        """分析RSI相关模式"""
        alerts = []
        rsi_6 = IndicatorCalculation.calculate_rsi(results_min[:-1], 6).__round__(1)
        pre_rsi_6 = IndicatorCalculation.calculate_rsi(results_min[:-2], 6).__round__(1)

        # RSI边界警报
        rsi_alert = self._check_rsi_boundary(stock, window, rsi_6, pre_rsi_6)
        if rsi_alert:
            alerts.append(rsi_alert)

        # RSI极端值模式
        extreme_alerts = self._check_rsi_extreme_patterns(stock, window, pre_rsi_6, last_k, prev_k)
        alerts.extend(extreme_alerts)

        return alerts

    def _check_rsi_boundary(self, stock, window, rsi_6, pre_rsi_6):
        # if window == 1:
        #     return None
        """检查RSI边界条件"""
        state_key = f"{stock}_{window}"
        if state_key not in self._rsi_trigger_states:
            self._rsi_trigger_states[state_key] = {'last_rsi_triggered': False}

        current_state = self._rsi_trigger_states[state_key]

        if not 20 <= rsi_6 <= 70:
            is_consecutive_trigger = (pre_rsi_6 is not None and not 20 <= pre_rsi_6 <= 70)

            if not is_consecutive_trigger or not current_state['last_rsi_triggered']:
                current_state['last_rsi_triggered'] = True
                return self._create_alert_data(
                    stock, f"({window}min)rsi_6:{rsi_6}", window, '观察'
                )
            else:
                current_state['last_rsi_triggered'] = True
        else:
            current_state['last_rsi_triggered'] = False

        return None

    def _check_rsi_extreme_patterns(self, stock, window, pre_rsi_6, last_k, prev_k):
        """检查RSI极端值的K线模式"""
        alerts = []

        # RSI低位反弹模式
        if pre_rsi_6 <= 20 and self._is_bullish_reversal(last_k, prev_k):
            alerts.append(self._create_alert_data(
                stock, f"({window}min)rsi_6_up", window, '买点'
            ))

        # RSI高位回落模式
        if pre_rsi_6 >= 70 and self._is_bearish_reversal(last_k, prev_k):
            alerts.append(self._create_alert_data(
                stock, f"({window}min)rsi_6_down", window, '卖点'
            ))

        return alerts

    def _analyze_candle_patterns(self, stock, window, last_k, prev_k, prev_prev_k):
        """分析K线形态模式"""
        alerts = []

        # 吞没形态
        engulfing_alert = self._check_engulfing_pattern(stock, window, last_k, prev_k)
        if engulfing_alert:
            alerts.append(engulfing_alert)

        # 三根K线组合模式
        triple_pattern_alerts = self._check_triple_candle_patterns(stock, window, last_k, prev_k, prev_prev_k)
        alerts.extend(triple_pattern_alerts)

        return alerts

    def _check_engulfing_pattern(self, stock, window, last_k, prev_k):
        if window == 1:
            return None
        """检查吞没形态"""
        # 阳包阴
        if (last_k['open'] < prev_k['close'] < prev_k['open'] < last_k['close'] and
                last_k['close'] > last_k['open'] and last_k['amount'] > prev_k['amount']):
            return self._create_alert_data(
                stock, f"({window}min)engulfing_up", window, '买点'
            )

        # 阴包阳
        elif (last_k['open'] > prev_k['close'] > prev_k['open'] > last_k['close'] and
              last_k['close'] < last_k['open'] and last_k['amount'] > prev_k['amount']):
            return self._create_alert_data(
                stock, f"({window}min)engulfing_down", window, '卖点'
            )

        return None

    def _check_triple_candle_patterns(self, stock, window, last_k, prev_k, prev_prev_k):
        """检查三根K线组合模式"""
        if window == 1:
            return []
        alerts = []

        # 阳-阴-阳组合
        if (prev_prev_k['close'] > prev_prev_k['open'] and
                prev_k['close'] < prev_k['open'] and
                last_k['close'] > last_k['open'] and
                prev_prev_k['amount'] > prev_k['amount'] and
                last_k['amount'] > prev_k['amount']):
            alerts.append(self._create_alert_data(
                stock, f"({window}min)up_down_up", window, '买点'
            ))

        # 阴-阳-阴组合
        if (prev_prev_k['close'] < prev_prev_k['open'] and
                prev_k['close'] > prev_k['open'] and
                last_k['close'] < last_k['open'] and
                prev_prev_k['amount'] > prev_k['amount'] and
                last_k['amount'] > prev_k['amount']):
            alerts.append(self._create_alert_data(
                stock, f"({window}min)down_up_down", window, '卖点'
            ))

        return alerts

    def _is_bullish_reversal(self, last_k, prev_k):
        """判断是否看涨反转形态"""
        return (prev_k['close'] < prev_k['open'] and  # 前一根阴线
                last_k['close'] >= last_k['open'] and  # 当前阳线
                last_k['amount'] > prev_k['amount'])  # 放量

    def _is_bearish_reversal(self, last_k, prev_k):
        """判断是否看跌反转形态"""
        return (prev_k['close'] > prev_k['open'] and  # 前一根阳线
                last_k['close'] <= last_k['open'] and  # 当前阴线
                last_k['amount'] > prev_k['amount'])  # 放量

    def _create_alert_data(self, stock, alert_message, window_sec=None, alert_type='观察'):
        """创建统一的警报数据结构"""
        alert_data = {
            'stock_code': stock,
            'stock_name': get_stock_name(stock),
            'alert_type': alert_type,
            'alert_level': 2,
            'alert_message': alert_message,
            'trigger_time': datetime.now()
        }

        if window_sec is not None:
            alert_data['windows_sec'] = window_sec

        return alert_data

    def calculate_ma_distances(self, stock_list):
        all_distances = {}

        for stock in stock_list:
            candles = self.stock_data.get_stock_data(stock)
            if not candles:
                continue

            ma_distances = self._calculate_single_stock_ma_distances(stock, candles)
            if ma_distances:
                all_distances[stock] = ma_distances

        return all_distances

    def _calculate_single_stock_ma_distances(self, stock, candles):
        data = IndexAnalysis.my_pro_bar(stock)
        ma = IndexAnalysis.calculate_realtime_ma(data, candles[-1])
        current_price = candles[-1]['close']

        stock_config = self.config.MONITOR_STOCKS.setdefault(stock, {})
        ma_types = stock_config.get("ma_types", [5, 10, 20, 30, 60, 120])
        ma_distances = {}

        for ma_type in ma_types:
            ma_key = f"ma{ma_type}"
            if ma_key not in ma or ma[ma_key] <= 0:
                continue

            ma_value = ma[ma_key]
            price_diff = current_price - ma_value
            percent_diff = (price_diff / ma_value) * 100

            ma_distances[ma_type] = {
                "diff": round(price_diff, 2),
                "percent": round(percent_diff, 2),
                "current_price": round(current_price, 2),
                "ma_value": round(ma_value, 2)
            }

        return ma_distances