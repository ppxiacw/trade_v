import pandas as pd
import numpy as np
import pandas_ta as ta
import tushare as ts
import time
from datetime import datetime, timedelta
import os
import traceback


class DivergenceDetector:
    def __init__(self, symbol):
        self.symbol = symbol
        # 多周期数据缓存
        self.data_cache = {
            '1min': pd.DataFrame(),
            '5min': pd.DataFrame(),
            '15min': pd.DataFrame(),
            '30min': pd.DataFrame(),
            '60min': pd.DataFrame()
        }
        # 各周期配置参数
        self.period_config = {
            '1min': {'rsi_period': 14, 'lookback': 120, 'pivot_window': 5},
            '5min': {'rsi_period': 14, 'lookback': 240, 'pivot_window': 4},
            '15min': {'rsi_period': 14, 'lookback': 480, 'pivot_window': 3},
            '30min': {'rsi_period': 21, 'lookback': 720, 'pivot_window': 3},
            '60min': {'rsi_period': 21, 'lookback': 1440, 'pivot_window': 3}
        }

    def fetch_base_data(self, base_freq='1min', lookback_hours=2000):
        lookback_hours = 2000
        """获取基础数据并确保有足够的预热数据"""
        # 获取最大需要的预热周期数
        max_warmup = max(
            self.period_config[base_freq]['rsi_period'],
            self.period_config[base_freq]['pivot_window'] * 2,
            30  # 最小30个点
        )

        # 计算需要额外的时间（基于频率）
        if base_freq == '1min':
            extra_minutes = max_warmup
        else:
            freq_minutes = int(base_freq.replace('min', ''))
            extra_minutes = max_warmup * freq_minutes

        end_time = datetime.now()
        # 增加额外的时间来获取更多数据点（包含预热数据）
        start_time = end_time - timedelta(hours=lookback_hours, minutes=extra_minutes)

        print(f"获取数据: {self.symbol} {base_freq} 从 {start_time} 到 {end_time}")

        # 获取数据（包含额外的预热数据）
        raw_data = self._get_minute_data(
            self.symbol,
            start_time.strftime('%Y%m%d %H:%M:%S'),
            end_time.strftime('%Y%m%d %H:%M:%S'),
            base_freq
        )

        # 预处理数据
        if not raw_data.empty:
            raw_data = self.preprocess_data(raw_data, base_freq)

        # 只保留有效数据部分（去除多余的历史数据）
        if len(raw_data) > max_warmup:
            # 保留最后 (lookback_hours * 60) 分钟的数据 + 预热数据
            keep_minutes = lookback_hours * 60 + extra_minutes
            base_data = raw_data.iloc[-keep_minutes:]
        else:
            base_data = raw_data

        # 生成多周期数据
        self.data_cache[base_freq] = base_data
        self._generate_multi_timeframe_data(base_data, base_freq)

        return self.data_cache

    def preprocess_data(self, data, timeframe):
        """预处理数据：处理空值、排序、填充等"""
        # 确保时间索引正确
        if not data.index.is_monotonic_increasing:
            data.sort_index(inplace=True)

        # 检查并处理空值
        for col in ['open', 'high', 'low', 'close']:
            if data[col].isna().any():
                print(f"警告: {timeframe} 数据中的 {col} 包含空值，使用前向填充")
                data[col] = data[col].ffill().bfill()

        # 特殊处理成交量 - 0可能有效
        if 'vol' in data.columns and data['vol'].isna().any():
            data['vol'] = data['vol'].fillna(0)

        return data

    def _generate_multi_timeframe_data(self, base_data, base_freq):
        """从基础频率生成其他周期数据"""
        # 转换基础频率为分钟数
        base_minutes = int(base_freq.replace('min', ''))

        for target_freq in self.data_cache.keys():
            if target_freq == base_freq:
                continue

            # 计算目标周期分钟数
            target_minutes = int(target_freq.replace('min', ''))

            # 检查是否整数倍关系
            if target_minutes % base_minutes != 0:
                print(f"警告: {target_freq} 不是 {base_freq} 的整数倍，跳过生成")
                continue

            # 重采样规则
            agg_rules = {
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'vol': 'sum'
            }

            # 重采样 - 使用'min'替代'T'
            ratio = target_minutes // base_minutes
            resampled = base_data.resample(f'{target_minutes}min', closed='right', label='right').agg(agg_rules)

            # 去除空值（不完整周期）
            resampled = resampled.dropna(subset=['close'])

            # 存储到缓存
            self.data_cache[target_freq] = resampled

    def _get_minute_data(self, symbol, start_time, end_time, freq='1min'):
        """获取分钟级数据"""
        try:
            # 转换日期格式
            start_date = pd.to_datetime(start_time).strftime('%Y%m%d')
            end_date = pd.to_datetime(end_time).strftime('%Y%m%d')

            # 获取数据
            df = ts.pro_bar(
                ts_code=symbol,
                asset='E',
                freq=freq,
                start_date=start_date,
                end_date=end_date
            )

            # 处理数据
            if not df.empty:
                df['trade_time'] = pd.to_datetime(df['trade_time'])
                df.set_index('trade_time', inplace=True)
                df.sort_index(inplace=True)
                return df[['open', 'high', 'low', 'close', 'vol']]
            return pd.DataFrame()
        except Exception as e:
            print(f"获取数据错误: {str(e)}")
            return pd.DataFrame()

    def calculate_rsi(self,close, period=14):
        """
        向量化实现，无循环，完全匹配同花顺/通达信结果
        """
        close = pd.Series(close)
        delta = close.diff()

        # 计算涨跌值
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)

        # 计算初始平均值（跳过第一个NaN）
        init_gain = gain.iloc[1:period + 1].mean()
        init_loss = loss.iloc[1:period + 1].mean()

        # 创建递归计算数组
        gain_arr = gain.values
        loss_arr = loss.values

        # 初始化数组
        avg_gain = np.full(len(close), np.nan)
        avg_loss = np.full(len(close), np.nan)

        # 设置初始值
        avg_gain[period] = init_gain
        avg_loss[period] = init_loss

        # 向量化计算递归值
        for i in range(period + 1, len(close)):
            avg_gain[i] = (avg_gain[i - 1] * (period - 1) + gain_arr[i]) / period
            avg_loss[i] = (avg_loss[i - 1] * (period - 1) + loss_arr[i]) / period

        # 计算RSI
        rs = np.divide(avg_gain, avg_loss, out=np.zeros_like(avg_gain), where=avg_loss != 0)
        rsi = 100 - (100 / (1 + rs))

        return pd.Series(rsi, index=close.index)

    def detect_divergence_for_timeframe(self, timeframe):
        """检测特定时间周期的背离信号"""
        if self.data_cache[timeframe].empty:
            print(f"无 {timeframe} 数据，请先获取数据")
            return self.create_empty_signals()

        data = self.data_cache[timeframe]
        config = self.period_config[timeframe]

        # 检查数据充分性
        min_length = max(config['rsi_period'] * 2, config['pivot_window'] * 4, 50)
        if len(data) < min_length:
            print(f"跳过 {timeframe} 周期 - 数据不足 ({len(data)} < {min_length})")
            return self.create_empty_signals(data)

        # 计算RSI
        rsi_period = config['rsi_period']
        data['rsi'] = self.calculate_rsi(data['close'],period=6)


        # 特殊处理初始NaN值
        first_valid_idx = data['rsi'].first_valid_index()
        if first_valid_idx:
            first_valid_value = data.loc[first_valid_idx, 'rsi']
            first_valid_loc = data.index.get_loc(first_valid_idx)

            # 从50（中性值）线性过渡到第一个有效值
            for i in range(first_valid_loc):
                if first_valid_loc > 1:
                    progress = i / (first_valid_loc - 1)
                else:
                    progress = 1
                interpolated_value = 50 * (1 - progress) + first_valid_value * progress
                data.iloc[i, data.columns.get_loc('rsi')] = interpolated_value

        # 处理任何剩余的NaN（应该不会有，但保险起见）
        data['rsi'] = data['rsi'].ffill().bfill().fillna(50)

        # 验证预热数据处理
        self.validate_warmup_data(data, rsi_period)

        # 寻找关键点
        pivots = self._find_pivots(data['rsi'], config['pivot_window'])

        # 检测背离
        signals = self._detect_divergence(
            data,
            data['rsi'],
            pivots,
            config['lookback']
        )

        return signals

    def validate_warmup_data(self, data, rsi_period):
        """验证预热数据是否已正确处理"""
        # 检查前rsi_period个点是否有空值
        if len(data) > rsi_period and data['rsi'].iloc[:rsi_period].isna().any():
            print(f"错误: 预热数据未正确处理！前{rsi_period}个点包含空值")
            return False

        # 检查前rsi_period个点的值是否合理
        if len(data) > rsi_period:
            rsi_values = data['rsi'].iloc[:rsi_period]
            if (rsi_values < 0).any() or (rsi_values > 100).any():
                print(f"警告: 预热数据中的RSI值超出0-100范围")

        return True

    def create_empty_signals(self, data=None):
        """创建空信号DataFrame"""
        if data is None:
            return pd.DataFrame()

        signals = pd.DataFrame(index=data.index)
        for signal_type in ['bull', 'hidden_bull', 'bear', 'hidden_bear']:
            signals[signal_type] = 0
        return signals

    def _find_pivots(self, series, window=5):
        """使用高效方法寻找关键点"""
        from scipy.signal import argrelextrema

        # 处理NaN值
        series_clean = series.ffill().bfill()

        # 如果全部是None或数据不足，返回空序列
        if series_clean.isna().all() or len(series_clean) < window * 2 + 1:
            return pd.Series(index=series.index, dtype=float)

        # 寻找波谷 (低点)
        minima_idx = argrelextrema(series_clean.values, np.less, order=window)[0]
        minima = pd.Series(series_clean.iloc[minima_idx], index=series_clean.index[minima_idx])

        # 寻找波峰 (高点)
        maxima_idx = argrelextrema(series_clean.values, np.greater, order=window)[0]
        maxima = pd.Series(series_clean.iloc[maxima_idx], index=series_clean.index[maxima_idx])

        # 合并关键点 (波谷为负值，波峰为正值)
        pivots = pd.concat([
            pd.Series(-minima.values, index=minima.index),  # 波谷为负
            maxima  # 波峰为正
        ]).sort_index()

        return pivots

    def _detect_divergence(self, data, rsi, pivots, lookback_minutes):
        """检测背离信号"""
        signals = pd.DataFrame(index=data.index)
        signals['price'] = data['close']
        signals['rsi'] = rsi
        signals['pivots'] = pivots

        # 初始化信号列
        signal_types = ['bull', 'hidden_bull', 'bear', 'hidden_bear']
        for st in signal_types:
            signals[st] = 0

        # 获取所有关键点
        pivot_points = pivots[pivots.notna()]
        if len(pivot_points) < 2:
            return signals

        # 检测背离
        for i in range(1, len(pivot_points)):
            current_idx = pivot_points.index[i]
            prev_idx = pivot_points.index[i - 1]

            # 检查索引是否有效
            if current_idx is None or prev_idx is None or current_idx not in data.index or prev_idx not in data.index:
                continue

            # 时间间隔检查
            try:
                time_diff = (current_idx - prev_idx).total_seconds() / 60
                if time_diff > lookback_minutes:
                    continue
            except TypeError:
                continue

            current_val = pivot_points.loc[current_idx]
            prev_val = pivot_points.loc[prev_idx]

            # 检查关键点值是否有效
            if pd.isna(current_val) or pd.isna(prev_val):
                continue

            # 波谷检测 (负值)
            if current_val < 0 and prev_val < 0:
                current_price = data.loc[current_idx, 'low']
                prev_price = data.loc[prev_idx, 'low']

                # 检查价格是否有效
                if pd.isna(current_price) or pd.isna(prev_price):
                    continue

                # 常规看涨背离: 价格新低但RSI抬高
                if current_price < prev_price and abs(current_val) > abs(prev_val):
                    signals.loc[current_idx, 'bull'] = 1

                # 隐藏看涨背离: 价格抬高但RSI新低
                elif current_price > prev_price and abs(current_val) < abs(prev_val):
                    signals.loc[current_idx, 'hidden_bull'] = 1

            # 波峰检测 (正值)
            elif current_val > 0 and prev_val > 0:
                current_price = data.loc[current_idx, 'high']
                prev_price = data.loc[prev_idx, 'high']

                # 检查价格是否有效
                if pd.isna(current_price) or pd.isna(prev_price):
                    continue

                # 常规看跌背离: 价格新高但RSI降低
                if current_price > prev_price and current_val < prev_val:
                    signals.loc[current_idx, 'bear'] = 1

                # 隐藏看跌背离: 价格降低但RSI新高
                elif current_price < prev_price and current_val > prev_val:
                    signals.loc[current_idx, 'hidden_bear'] = 1

        return signals

    def monitor_multi_timeframe(self, base_freq='1min', interval_minutes=5):
        """多周期实时监控"""
        last_check = datetime.now() - timedelta(minutes=interval_minutes)

        while True:
            now = datetime.now()
            # 检查是否到达执行时间
            if (now - last_check).total_seconds() < interval_minutes * 60:
                time.sleep(10)
                continue

            try:
                print(f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] 检查多周期背离信号...")

                # 刷新数据
                self.fetch_base_data(base_freq, lookback_hours=48)

                # 检查各周期信号
                for timeframe in self.data_cache.keys():
                    if self.data_cache[timeframe].empty:
                        print(f"跳过 {timeframe} 周期 - 数据为空")
                        continue

                    # 添加try-except保护每个周期的检测
                    try:
                        signals = self.detect_divergence_for_timeframe(timeframe)
                        if signals is None or signals.empty:
                            continue

                        # 检查最新信号
                        if not signals.empty:
                            last_signal = signals.iloc[-1]

                            # 生成信号报告
                            signal_report = []
                            if last_signal.get('bull', 0):
                                signal_report.append(f"常规看涨背离 @ {last_signal['price']:.2f}")
                            if last_signal.get('hidden_bull', 0):
                                signal_report.append(f"隐藏看涨背离 @ {last_signal['price']:.2f}")
                            if last_signal.get('bear', 0):
                                signal_report.append(f"常规看跌背离 @ {last_signal['price']:.2f}")
                            if last_signal.get('hidden_bear', 0):
                                signal_report.append(f"隐藏看跌背离 @ {last_signal['price']:.2f}")

                            if signal_report:
                                print(f"  [{timeframe}] 发现信号: {', '.join(signal_report)}")
                    except Exception as e:
                        print(f"  [{timeframe}] 周期检测错误: {str(e)}")
                        # 记录详细错误到日志
                        with open('divergence_errors.log', 'a') as f:
                            f.write(f"[{datetime.now()}] {timeframe} error: {str(e)}\n")

                last_check = now
                time.sleep(60)  # 检查后休息1分钟

            except Exception as e:
                # 增强错误日志
                error_msg = f"监控错误: {str(e)}\n{traceback.format_exc()}"
                print(error_msg)

                # 记录到文件
                with open('divergence_errors.log', 'a') as f:
                    f.write(f"[{datetime.now()}] {error_msg}\n")

                time.sleep(300)  # 出错后等待5分钟重试


# 初始化Tushare
ts.set_token('410070664c78124d98ca5e81c3921530bd27534856b174c702d698a5')  # 替换为你的实际token
pro = ts.pro_api()

# 使用示例
if __name__ == "__main__":
    symbol = '399006.SZ'  # 茅台示例
    detector = DivergenceDetector(symbol)

    # 首次获取数据（使用1分钟基础数据）
    print("初始化数据...")
    detector.fetch_base_data('1min', lookback_hours=72)

    # 检测特定周期背离
    print("\n=== 5分钟周期背离检测 ===")
    signals_5min = detector.detect_divergence_for_timeframe('5min')
    if not signals_5min.empty:
        print(signals_5min.tail())

    print("\n=== 15分钟周期背离检测 ===")
    signals_15min = detector.detect_divergence_for_timeframe('15min')
    if not signals_15min.empty:
        print(signals_15min.tail())

    # 启动多周期实时监控
    print("\n启动多周期实时监控...")
    detector.monitor_multi_timeframe('1min', interval_minutes=5)