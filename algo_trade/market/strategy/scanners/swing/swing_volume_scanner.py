# algo_trade/scanners/swing/swing_volume_scanner.py

# Strategy 1 - The Big Bang - Trading The Active Ones, The Volume Theory
# Ground Level Strategy:
# 1. Daily Price Gain > 5% (Less than this will not indicate volume blast)
# 2. Price share being traded must be Rs X minumum
# 3. Volume being 10x of daily average & more than prev day volume
# 4. If Breakout after Consolidation, Sure shot buy
# 5. Fundamental Analysis for the reason of breakout
# 6. Broader market must be in an uptrend.
#
# Entry:
# 1. Keep an eye on 1h or 1D candle
# 2. Can be followed by 2.5 days of consolidation to a 10, 20 or 50 EMA.
# 3. SL at 1%-2% of the capital, can stretch upto 8% if momentum is expected to be big.
#
# Exit 1:3 Min, if Higher, start pyramidding.

import pandas as pd
import numpy as np
import asyncio
from algo_trade.market.strategy.scanners.scanner_generics.swing_generic import \
    SwingTradingGeneric
from algo_trade.market.strategy.constants import (
    SWING_MIN_PCT_CHANGE,
    SWING_MIN_SHARE_PRICE,
    SWING_VOLUME_DIFF,
    SWING_VOLUME_BIGBANG_ORDER,
    )


class SwingVolumeScanner(SwingTradingGeneric):
    """SwingVolumeScanner alass allows you to identify
    trades likely to shoot up based on volume explosion.
    """
    
    __name__ = "BIG-BANG VOLUME"
    
    def __init__(
            self,
            tf: str = "1d",
            min_price: float = SWING_MIN_SHARE_PRICE,
            min_pct_change: float = SWING_MIN_PCT_CHANGE,
            order_by: list = SWING_VOLUME_BIGBANG_ORDER,
            *args,
            **kwargs
            ):
        super(SwingVolumeScanner, self).__init__(*args, **kwargs)
        
        self.timeframe = tf
        self.min_price = min_price
        self.min_pct_change = min_pct_change
        self.order_by = order_by
    
    def swing_filtered_list(self) -> pd.DataFrame:
        """
        Applies conditions such as Min Price & Min % Change
        for the desired Swing Trade list.
        """
        
        data = self.get_swing_trading_ready_list()
        
        data = data.loc[
               (
                       (data.close>=self.min_price)
                       &(abs(data["pct_change"])>=self.min_pct_change)
               ),
               :,
               ]
        
        return data
    
    def swing_volume_analysis_util(self, symbol: str) -> dict:
        """Identifies volume difference from the previous day volume."""
        
        data = self.processor.get_period_data(
            symbol, period="10d", interval="1d"
            ).reset_index()
        data.loc[:, "date"] = pd.to_datetime(data.date).apply(lambda x:
                                                              x.date())
        data["prev_volume"] = (
            data.volume.rolling(window=1).mean().shift(-1).fillna(1).astype(
                int)
        )
        data["volumebyprevvolume"] = round((data.volume /
                                            data.prev_volume), 2)
        data = data.sort_values("date", ascending=False)
        
        # data["AvgRollingVolume"] = data.Volume.rolling(window=20).
        # mean().shift(-20)
        
        data = data.sort_values(by="date", ascending=False)
        return {
            symbol:data.loc[0, ["volumebyprevvolume"]].values.tolist().pop()}
    
    def swing_volume_analysis(self, data,
                              swing_vol_diff: int = SWING_VOLUME_DIFF) -> \
            pd.DataFrame:
        """
        Swing Volume Analysis for every stock in the list after the filtering
        conditions are applied.
        """
        
        symbols = data.symbol.values.tolist()
        result = dict()
        
        for iterate in map(self.swing_volume_analysis_util, symbols):
            result.update(iterate)
        
        result = pd.DataFrame.from_dict(list(result.items())).rename(
            columns={0:"symbol", 1:"volume_diff"}
            )
        
        data = pd.merge(data, result, on="symbol", how="left")
        
        data = data.loc[data.volume_diff>=swing_vol_diff]
        
        return data
    
    def generate_swing_output(self):
        """Generating Swing Output for Swing Volume Scanner."""
        
        # 1. Receiving Filtered List of Stocks.
        data = self.swing_filtered_list()
        
        # 2. Analyzing Volumes on Swing Output.
        data = self.swing_volume_analysis(data)
        
        # 3. Ordering the Data per order columns.
        data = data.sort_values(by=self.order_by, ascending=False)
        
        # 4. Setting the Strategy Name for the frame.
        data["Strategy"] = self.__name__
        
        # 5. Sharing the Swing result File across multiple strategies.
        self.append_swing_results(data)
        
        self.telegram.send_message(data, cols=["symbol", "pct_change", "volume_diff"],
                                   additional_text=self.__name__)


if __name__ == "__main__":
    obj = SwingVolumeScanner()
    obj.generate_swing_output()
