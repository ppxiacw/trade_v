import hashlib
import json
import time
from datetime import datetime, timedelta
import hmac
import base64
import urllib.parse
import requests
import pandas as pd
from typing import List, Dict, Set
from config.tushare_utils import ts
from dto.StockDataDay import StockDataDay
from config.tushare_utils import IndexAnalysis
from utils import stockAnalysis
from value.value import today, today_
from TopList import export_top_list

start_date = stockAnalysis.get_date_by_step(today_,-60)

class EnhancedStockMonitor:
    def __init__(self):
        # 基础配置
        self.ma_levels = [20,30, 60]
        self.batch_size = 20



        # 状态管理
        self.sent_alerts: Set[str] = set()  # 用于去重
        self.alert_cooldown = timedelta(minutes=30)  # 相同警报冷却时间
        self.last_alert_time: Dict[str, datetime] = {}  # 各股票最后警报时间



    def _send_dingtalk_alert(self, content: str):
        """发送钉钉告警"""
        try:
            headers = {"Content-Type": "application/json"}
            data = {
                "msgtype": "markdown",
                "markdown": {
                    "title": "股票预警通知",
                    "text": content
                }
            }
            url = 'https://oapi.dingtalk.com/robot/send?access_token=79b1100719c51a60877658bd24e1cdc9d758f55a678a5bf4f4061b8a924d6331'
            response = requests.post(url, headers=headers, data=json.dumps(data))
            response.raise_for_status()
        except Exception as e:
            print(f"钉钉消息发送失败: {str(e)}")

    def _generate_alert_id(self, ts_code: str, alert_type: str) -> str:
        """生成唯一警报标识符"""
        return f"{ts_code}_{alert_type}_{datetime.now().strftime('%Y%m%d')}"

    def _check_cooldown(self, alert_id: str) -> bool:
        """检查是否在冷却期内"""
        last_time = self.last_alert_time.get(alert_id)
        if last_time and (datetime.now() - last_time) < self.alert_cooldown:
            return True
        self.last_alert_time[alert_id] = datetime.now()
        return False


    def generate_alert(self, ts_code: str, quote: StockDataDay,
                       hierarchy: List[str], ma_data: Dict,
                       broken: List[str], nearby: List[str]):
        """生成并发送预警信息（同时处理跌破和接近均线）"""
        alert_content = []

        # 合并触发条件生成唯一ID（避免重复报警）
        alert_id = self._generate_alert_id(ts_code, f"B:{'_'.join(broken)}_N:{'_'.join(nearby)}")

        if self._check_cooldown(alert_id):
            return

        # 基础信息
        alert_content.append(f"### 🔔 股票预警 {ts_code} {getattr(quote, 'name', '')}")
        alert_content.append(f"- 当前价格: `{quote.close:.2f}` 元")
        alert_content.append(f"- 昨日收盘: `{quote.pre_close:.2f}` 元\n")

        # 均线突破情况
        if broken:
            alert_content.append("#### 📉 均线跌破警报")
            for ma in broken:
                distance = quote.close - ma_data[ma]
                alert_content.append(f"- **{ma.upper()}**: `{ma_data[ma]:.2f}` 元")
                alert_content.append(f"  ➠ 跌破幅度: `{distance:.2f}` 元 (`{distance / quote.close * 100:.2f}%`)\n")

        # 接近均线预警
        if nearby:
            alert_content.append("#### ⚠️ 均线接近预警")
            for ma in nearby:
                distance_pct = (quote.close - ma_data[ma]) / ma_data[ma] * 100
                alert_content.append(f"- **{ma.upper()}**: `{ma_data[ma]:.2f}` 元")
                alert_content.append(f"  ➠ 当前距离: `{distance_pct:.2f}%` (安全阈值: 0.5%)\n")

        # 快速跌向检测（保留原有逻辑）


        # 趋势分析（优化显示）
        if hierarchy:
            top_ma = hierarchy[0]
            distance_pct = (ma_data[top_ma] - quote.close) / quote.close * 100
            alert_content.append("#### 📊 趋势分析")
            alert_content.append(f"- 最高均线 `{top_ma.upper()}` 偏离: `{distance_pct:.2f}%`")
            alert_content.append(f"- 均线层级: {' → '.join(hierarchy)}\n")

        # 发送警报
        if alert_content:
            self._send_dingtalk_alert("\n".join(alert_content))
            self.sent_alerts.add(alert_id)




    def get_ma_hierarchy(self, ma_data: Dict) -> List[str]:
        """动态生成当前均线层级（从高到低）"""
        valid_ma = {k: v for k, v in ma_data.items() if pd.notnull(v)}
        return sorted(valid_ma.keys(),
                      key=lambda x: valid_ma[x],
                      reverse=True)  # 按当前值从高到低排序



    def check_any_break(self, quote: StockDataDay, hierarchy: List[str], ma_data: Dict) -> tuple[List[str], List[str]]:
        """
        检查价格与均线关系，返回两个列表：
        1. 已跌破的均线
        2. 在均线上但距离小于0.5%的均线

        参数说明：
        - 前一日收盘价需大于等于均线值
        - 当前价格需满足以下条件之一：
          1. 收盘价 < 均线值 → 已跌破
          2. 均线值 ≤ 收盘价 ≤ 均线值*1.005 → 接近但未跌破
        """
        below = []
        near = []

        for ma in hierarchy:
            ma_value = ma_data[ma]
            # 检查前一日收盘价是否在均线上方
            if quote.pre_close >= ma_value:
                # 判断当前价格状态
                if quote.close < ma_value:
                    below.append(ma)
                elif quote.close <= ma_value * 1.005:
                    near.append(ma)

        return below, near


    def fetch_ma_data(self, ts_code: str) -> Dict:
        """获取最新的均线数据"""
        try:
            df = ts.pro_bar(
                ts_code=ts_code,
                adj='qfq',
                ma=self.ma_levels,
                start_date=start_date,
                end_date=today
            )

            if df.empty or len(df) < max(self.ma_levels):
                return {}

            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
            latest = df.sort_values('trade_date').iloc[-1]

            return {f'ma{period}': latest[f'ma{period}'] for period in self.ma_levels}
        except Exception as e:
            print(f"获取 {ts_code} 均线数据异常: {str(e)}")
            return {}
    def process_batch(self, batch: List[str]):
        """处理一批股票"""
        quotes: List[StockDataDay] = IndexAnalysis.realtime_quote(','.join(batch))
        quote_map = {q.ts_code: q for q in quotes}

        for code in batch:
            quote = quote_map.get(code)
            if not quote or not quote.close:
                continue

            # 获取均线数据
            ma_data = self.fetch_ma_data(code)
            if not ma_data:
                continue

            # 生成当前均线层级
            hierarchy = self.get_ma_hierarchy(ma_data)
            if not hierarchy:
                continue

            # 检查跌破情况


            # 在策略逻辑中
            broken_mas, nearby_mas = self.check_any_break(quote, hierarchy, ma_data)

            # 任意条件触发即生成警报
            if broken_mas or nearby_mas:
                self.generate_alert(
                    ts_code=code,
                    quote=quote,
                    hierarchy=hierarchy,
                    ma_data=ma_data,
                    broken=broken_mas,  # 传入跌破列表
                    nearby=nearby_mas  # 新增接近列表
                )

    def run_forever(self, interval: int = 300):
        """持续运行监控"""
        with open('./top_list_files/all_stocks.txt', 'r', encoding='utf-8') as f:
            # 使用集合推导式 + 去除换行符 + 过滤空行
            all_stocks = {line.strip() for line in f if line.strip()}
        with open('./top_list_files/black_list.txt', 'r', encoding='utf-8') as f:
            # 使用集合推导式 + 去除换行符 + 过滤空行
            black_list = {line.strip() for line in f if line.strip()}
        all_stocks = list(all_stocks-black_list)

        while True:
            start_time = datetime.now()
            print(f"\n⏰ 开始轮询检查 {len(all_stocks)} 支股票 [{start_time.strftime('%H:%M:%S')}]")

            for i in range(0, len(all_stocks), self.batch_size):
                print(i)
                batch = all_stocks[i:i + self.batch_size]
                self.process_batch(batch)

            # 清理过期警报记录
            self._cleanup_old_alerts()



    def _cleanup_old_alerts(self):
        """清理过期的警报记录"""
        cutoff = datetime.now() - self.alert_cooldown
        expired = [k for k, v in self.last_alert_time.items() if v < cutoff]
        for key in expired:
            del self.last_alert_time[key]


# 使用示例
if __name__ == "__main__":
    monitor = EnhancedStockMonitor()
    monitor.run_forever(interval=60)  # 每1分钟全量检查一次