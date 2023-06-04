# Strategy 3 - Buying stocks reversing from 50ema
#
# Ground Level Strategy
# 1. Look for stocks which are up more than
#    i. >20% in the past 30 days or
#   ii. >30% in the past 90 days or
#  iii. >50% in the past 180 days.
#   Meaning we buy pullbacks only sound stocks with strong momentum.
# 2. Look for some consolidation in the stock -
#    these consolidations can be of time or price.
#    For time it will be in sideways moves and
#    for price it will move around 10, 20, 50 EMA.
# 3. Stocks > 300Rs in terms of Price.
# 4. Minimum trading volume in stock is at least 1 Crore/day.
#
# Look at your path
# 1. Make sure that the major market is in an uptrend,
#    and we aren't trading dead cat bounce.
# 2. High momentum stocks are preferred which belongs
#    to market favourite industries.
#    Seasonal ones will not make much money for a trader.
# 3. A trader may buy to pullback which is towards rising 10, 20 or 50 EMA
#    provided stock is showing low volume near 10/20/50 EMAs.
#    In other words, we buy pullbacks if it is happening with low volumes.
# 4. As a trader you may not only look for a pullback which is towards
#    a random 10 or 20 or 50 days moving average,
#    but you must do some wait and watch until the trend gets confirmed.
#    Usually a 3-5% bounce near a stock's 10 or 20 or 50 EMAs is good signal to work on it.
# 5. Look for the stocks or indices which are in a long term uptrend.
#    and have traced back to 50 days EMA, this presents an
#    opportunity to buy the stock.Stock can retrace 50 EMA and
#    can even go down for intraday day and bounces back the same day or stock can
#    close below 50-ema with lower volume than earlier days
#    and then next day stock bounces back above its 50EMA line.
#
# The 50 EMA Dip buying parameters:
# 1. Price bounces near the 50EMA and closes above it,
#    price moves below 50 EMA intraday but closes back above it,
#    or price closes below the 50 day EMA line but next day closes above it.
# 2. Profit target could be in the range of 6-10%
# 3. Example of Deepak Nitrite bouncing off from 50ema in 2020-21.
#
# Entry:
# 1. Try to enter in its tight price and low volume trading days.
# 2. From your list of stocks, enter the stongest stocks
#    when those make new 20 day high or even very near to that.
# 3. Enter a pullback around a rising 10/20/50 EMA before you buy the stock.
#    This stage might singal the start of a new runup.
#
# Stop Loss
# SL: 0.5% below the entry day's low or stock's 10 or 20 or 50 ema.
#
# Exit:
# 1. Around half qty be sold at 2.5 days to 5 days after entry.
# 2. Sell the rest on a close below a 10/20/50 day EMA.
# 3. Selling on strength works a lot and profit booking is always done on good days.
#    Target 8-12% of price rise.


import numpy as np
import pandas as pd

from algo_trade.scanners.swing.swing_strategies import SwingTradingGeneric
from algo_trade.utils.constants import SWING_MIN_SHARE_PRICE, SWING_BUYING_PULLBACKS


class SwingBuyingPullBackScanner(SwingTradingGeneric):
    """
    Swing Buying Pull back Scanner
    Identify Pullback Trade opportunities
    """

    __name__ = "Buying Pull-backs"

    def __init__(
        self,
        tf: str = "1d",
        min_price: float = SWING_MIN_SHARE_PRICE,
        order_by: list = SWING_BUYING_PULLBACKS,
        *args,
        **kwargs
    ):
        super(SwingBuyingPullBackScanner, self).__init__(*args, **kwargs)

        self.timeframe = tf
        self.min_price = SWING_MIN_SHARE_PRICE
        self.order_by = order_by

    def swing_filtered_list(self) -> pd.DataFrame:
        """Applies conditions for the current swing Strategy."""

        data = self.get_swing_trading_ready_list()

        data = data.loc[(data.Last >= self.min_price), :]

        # TODO: Write Method to get % change over 30, 90 & 180 days
        data = 0
        pass
