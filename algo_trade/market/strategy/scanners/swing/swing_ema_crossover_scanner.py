# Strategy 6
# TODO: Scanning stocks that are close to 20-50ema crossover.
# Crossover with max returns
# 1. 10 days EMA crosses 20days EMA
# 2. 20days EMA crosses 50 days EMA
# 3. 50 days EMA crosses 200day ema

import pandas as pd
from asyncio import run

from algo_trade.utils.meta import AsyncLoggingMeta
from algo_trade.market.strategy.indicators.moving_averages import MovingAverages
from algo_trade.market.strategy.scanners.scanner_generics.swing_generic import SwingTradingGeneric


class SwingMovingAverageCrossOver(SwingTradingGeneric, metaclass=AsyncLoggingMeta):
    __name__ = "Moving Average Crossover"
    
    def __init__(self, *args, **kwargs):
        super(SwingMovingAverageCrossOver, self).__init__(*args, **kwargs)
        
        self.moving_avg = MovingAverages()
    
    async def _identify_crossover(self, data: pd.DataFrame) -> str:
        data = data.sort_values(by="date", ascending=True)
        
        data = self.moving_avg.add_moving_averages(data)
        emas = (('EMA10', 'EMA20'), ('EMA20', 'EMA50'), ('EMA50', 'EMA200'))
        
        result = [self.moving_average_crossover(data, i, j) for i, j in emas]
        
        final = str()
        
        for i, j in enumerate(result):
            
            if j[0] is True:
                final += "{0} Crossover on {1} X {2}.".format(j[1], *emas[i])
        
        if final == str():
            final = 'No Crossover'
        
        return final
    
    async def _cross_over_method(self, tickers: tuple):
        data = {i:self.processor.get_period_data(i, period='12mo', interval='1d')
                for i in tickers}
        
        data = {i:await self._identify_crossover(j) for i, j in data.items()}
        
        return pd.DataFrame(data.items(), columns=["symbol", "crossover"])
    
    def generate_swing_output(self):
        data = self.get_swing_trading_ready_list()
        
        tickers = data.symbol.tolist()
        
        result = run(self._cross_over_method(tuple(tickers)))
        
        result = pd.merge(data, result, on="symbol", how="left")
        
        result["Strategy"] = self.__name__
        
        result = result.loc[~(result.crossover == 'No Crossover'), :]
        
        self.append_swing_results(result)


if __name__ == '__main__':
    obj = SwingMovingAverageCrossOver()
    
    result = obj.swing_filtered_list()
    
    print(result)
