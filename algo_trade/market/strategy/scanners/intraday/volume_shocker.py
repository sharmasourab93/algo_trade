import pandas as pd
from asyncio import run
from typing import Tuple
from functools import cache

from algo_trade.market.strategy.indicators.moving_averages import MovingAverages
from algo_trade.market.strategy.scanners.scanner_generics.swing_generic import SwingTradingGeneric
from algo_trade.data_handler.calendar.constants import DATE_FMT
from algo_trade.data_handler import DataHandler
from algo_trade.utils.meta import AsyncLoggingMeta


class VolumeShockers(SwingTradingGeneric, metaclass=AsyncLoggingMeta):
    STRATEGY_NAME = 'VolumeShockerStrategy'
    
    def __init__(self, nse: int = 500, period: str = '2d', interval: str = '5m', *args, **kwargs):
        super(VolumeShockers, self).__init__(nse, *args, **kwargs)
        
        self.moving_avg = MovingAverages()
        self.period = period
        self.interval = interval
    
    async def _summarize_sma_volume(self, ticker: str) -> Tuple[str, float]:
        result = self.processor.get_period_data(ticker, period=self.period, interval=self.interval)
        result = result.sort_values(by="datetime", ascending=True)
        result = self.moving_avg.add_moving_averages(result, column="volume", ma="SMA", ma_range=(10,))
        last_sma10 = (float(result.tail(1).SMA10.iloc[0]))
        last_volume = float(result.groupby(by=result.datetime.map(lambda x:x.date())).volume.sum().iloc[-1])
        last_close = result.tail(1).close.iloc[0]
        day_pct_close = result.groupby(by=result.datetime.map(lambda x:x.date()))["pct_change"].sum().iloc[-1]
        result = (ticker, last_close, day_pct_close, last_volume, last_sma10)
        
        self.logger.debug(
            "Ticker {0}: Last Close: {1}, Day Total Volume:{2}, "
            "SMA Total Volume: {3}, Day % Close {4}".format(ticker, *result))
        
        return result
    
    async def _get_sma_for_volume(self, data: tuple) -> pd.DataFrame:
        sma_data = {i:await self._summarize_sma_volume(i) for i in data}
        
        sma_data = pd.DataFrame(tuple(sma_data.values()),
                                columns=["symbol", "last_close", "day_pct_change", "day_volume", "10sma_volume"])
        
        return sma_data
    
    async def _get_processed_cpr_data(self) -> pd.DataFrame:
        data = self.processor.retrieve_strategy_output('IntradayCPRAnalysis',
                                                       self.processor.prev_day.strftime(DATE_FMT))
        data = data.loc[data.cpr.isin(["Narrow CPR", "Mid CPR", "Compact CPR"]), ["symbol", "close", "pct_change",
                                                                                  "time_con_range", "volume",
                                                                                  "cpr"]]
        
        data = data.rename(columns={"close":"prev_close", "pct_change":"prev_pct_change",
                                    "volume":"prev_volume"})
        data = data.sort_values(by=["time_con_range"])
        
        return data
    
    async def _trade_ready_filtered_list(self) -> pd.DataFrame:
        data = await self._get_processed_cpr_data()
        tickers = data.symbol.tolist()
        
        # Fetching Volume's 10 Simple Moving Average and integrating with the data.
        volume_sma = await self._get_sma_for_volume(tuple(tickers))
        data = pd.merge(data, volume_sma, on="symbol", how="left")
        
        return data
    
    def run_volume_shocker_output(self) -> pd.DataFrame:
        data = run(self._trade_ready_filtered_list())
        
        self.logger.debug("Data Generated. Data Shape: {0}, {1}".format(*data.shape))
        
        # Condition 1. Lastest Volume > SMA10_volume * 2
        # and
        # (Condition 2. Latest Close > prev_close * 1.05
        # or
        # Condition 3. Latest Close < prev_close * 0.95)
        
        data = data.loc[((data.day_volume>(data['10sma_volume'] * 2))
                         &((data.last_close>(data.prev_close * 1.05))|
                           (data.last_close<(data.prev_close * 0.95)))), :]
        data = data.sort_values(by=['day_pct_change', 'day_volume'], ascending=False)
        
        return data


if __name__ == '__main__':
    obj = VolumeShockers()
    data = obj.run_volume_shocker_output()
    print(data)
