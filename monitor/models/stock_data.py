from datetime import datetime

class StockData:
    def __init__(self, config):
        self.data_storage = {}
        self.last_update_time = {}
        self.config = config
        self.initialize_data_storage()

    def initialize_data_storage(self):
        base_retention = self.config.DATA_RETENTION_HOURS * 3600 // self.config.BASE_INTERVAL

        for stock, config in self.config.MONITOR_STOCKS.items():
            self.data_storage[stock] = {}
            self.last_update_time[stock] = datetime.now()

            self.data_storage[stock]["base"] = {
                "candles": [],
                "maxlen": base_retention,
                "interval": self.config.BASE_INTERVAL
            }

    def _add_to_array(self, array, item, maxlen):
        array.append(item)
        if len(array) > maxlen:
            return array[1:]
        return array

    def update_data(self, data_list):
        if len(data_list) == 0:
            return

        current_time = datetime.now()

        for row in data_list:
            stock = row.ts_code

            candle_data = {
                'timestamp': current_time,
                'open': row.open,
                'high': getattr(row, 'high', row.high),
                'low': getattr(row, 'low', row.low),
                'close': row.close,
                'vol': row.vol,
                'pre_close': getattr(row, 'pre_close', 0)
            }

            base_data = self.data_storage[stock]["base"]
            base_data["candles"] = self._add_to_array(
                base_data["candles"], candle_data, base_data["maxlen"]
            )

            self.last_update_time[stock] = current_time

    def get_stock_data(self, stock):
        return self.data_storage.get(stock, {}).get("base", {}).get("candles", [])