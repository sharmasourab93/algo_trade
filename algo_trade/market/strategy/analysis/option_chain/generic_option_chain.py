import abc
from typing import Tuple, Union
from datetime import datetime, timezone
import pandas as pd
from numpy import inf
from pandas import DataFrame, json_normalize
from math import ceil

from algo_trade.market.strategy.constants import PCR_VERDICT_RANGE
from algo_trade.data_handler.calendar import MarketCalendarTools
from algo_trade.utils import AsyncLoggingMeta
from algo_trade.data_handler.source.constants import TRADEABLE_INDICES
from algo_trade.data_handler.calendar.constants import TIME_ZONE, TODAY
from algo_trade.data_handler import DataHandler


class GenericOptionChain(metaclass=AsyncLoggingMeta):
    
    def __init__(self):
        
        self.processor = DataHandler()
    
    def pcr_verdict(self, pcr: float) -> str:
        """
        Gets you the Put Call Ration based Verdict.
        :param pcr: float
        :return:
        """
        verdict_range = PCR_VERDICT_RANGE
        
        for limits, classification in verdict_range.items():
            lower_limit, upper_limit = limits
            
            if lower_limit<=pcr<upper_limit:
                return classification
        
        return 'Invalid'
    
    def get_option_chain(self, symbol: str,
                         key: str = "index",
                         expiry_delta: int = 1) -> Tuple[dict, str]:
        
        """ Method that toggles between Stock & Index Option chain. """
        
        symbol = symbol.upper()
        
        if symbol in TRADEABLE_INDICES:
            
            key: str = "index"
            expiry = MarketCalendarTools.get_weekly_expiry(symbol)
        
        else:
            key: str = "equity"
            expiry = MarketCalendarTools.get_monthly_expiry()
        
        data = self.processor.nse_option_chain(key, symbol)
        
        i = 0
        while i<expiry_delta:
            expiry_date = next(expiry)
            i += 1
        
        option_chain_data = self.quote_option_chain(symbol, data,
                                                    expiry_date)
        
        return option_chain_data, expiry_date
    
    def quote_option_chain(self, symbol: str, data: dict, expiry: str) -> \
            Union[dict, None]:
        """
        Method to process Option Chain for a select symbol and
        selected expiry into algo_trade module consumable format.
        The Data Structure has hard coded values as in the data received
        from NSE officially.
        """
        
        records = data['records']
        filtered = data['filtered']
        all_expiries: list = records["expiryDates"]
        last_updated: str = records["timestamp"]
        strike_prices: list = records["strikePrices"]
        underlying: float = records["underlyingValue"]
        
        # Creating A Respone
        response = dict()
        response.update(
            {
                "symbol":symbol,
                "selected_expiry":expiry,
                "all_expiries":all_expiries,
                "last_updated":last_updated,
                "strikes":strike_prices,
                "underlying_value":underlying,
                }
            )
        
        # DataFrame Operations Section.
        df = DataFrame(json_normalize(data["records"]["data"]))
        df = df.loc[(df.expiryDate == expiry), :]
        
        if 0 in df.shape:
            return None
        
        df.set_index("strikePrice", inplace=True)
        
        # Common Columns
        columns = [
            "openInterest",
            "changeinOpenInterest",
            "pchangeinOpenInterest",
            "totalTradedVolume",
            "impliedVolatility",
            "lastPrice",
            "change",
            "pChange",
            "totalBuyQuantity",
            "totalSellQuantity",
            "bidQty",
            "bidprice",
            "askQty",
            "askPrice",
            ]
        
        options = ["Calls", "Puts"]
        
        ce_data = df.loc[:, df.columns.str.match("^(CE|expiryDate)")]
        pe_data = df.loc[:, df.columns.str.match("^(PE|expiryDate)")]
        
        # Call Option data frame maintainance.
        ce_data.columns = ce_data.columns.str.replace(r"CE.", "")
        ce_data = ce_data[columns].round(2)
        ce_data.columns = "CE_" + ce_data.columns
        ce_data.reset_index(inplace=True)
        
        # Put Option data frame maintainance.
        pe_data.columns = pe_data.columns.str.replace(r"PE.", "")
        pe_data = pe_data[columns].round(2)
        pe_data.columns = "PE_" + pe_data.columns
        pe_data.reset_index(inplace=True)
        
        # Merge Call & Put data.
        merged_call_put = pd.merge(ce_data, pe_data, on="strikePrice",
                                   how="left")
        
        # Put Call Ration calculation.
        merged_call_put["pcr_oi"] = merged_call_put.PE_openInterest / \
                                    merged_call_put.CE_openInterest
        merged_call_put["pcr_oi"] = merged_call_put.pcr_oi.replace(inf, 10.0)
        merged_call_put.loc[merged_call_put.pcr_oi>=10, "pcr_oi"] = 10.0
        overall_pcr = round((filtered['PE']['totOI'] /
                             filtered['CE']['totOI']), 2)
        pcr_verdict = self.pcr_verdict(overall_pcr)
        
        # Updating the final response.
        response.update(
            {
                "OptionChain":merged_call_put.round(2),
                "ResponseGenerateTime":datetime.now(tz=TIME_ZONE),
                "overall_pcr":overall_pcr,
                "pcr_verdict":pcr_verdict
                }
            )
        
        return response
    
    def identify_straddle(self,
                          option_chain_dict: dict,
                          avg_ce_volume: float,
                          avg_pe_volume: float,
                          left_limit: float = 0.8,
                          right_limit: float = 1.2) -> str:
        """
        Method to identify Straddles from the given dictionary.
        The condition is as follows:

        1. If (Left Limit <= PCR <= Right Limit
                or
              left Limit <= price_pe_by_ce <= Right limit)
              and
              ((Call Traded Volume > Avg Call Volumes) and
              (Put Traded Volume > Avg Put Traded Volumes)):
              We identify the strddle.

        2. Else
            Returns Empty string.

        :param option_chain_dict:
        :param avg_ce_volume:
        :param avg_pe_volume:
        :param left_limit:
        :param right_limit:
        :return:

        """
        
        str_result = str()
        
        price_pe_by_ce = option_chain_dict['PE_lastPrice'] / \
                         option_chain_dict['CE_lastPrice']
        pcr = option_chain_dict['pcr_oi']
        
        condition_1 = ((left_limit<=pcr<=right_limit) or
                       (left_limit<=price_pe_by_ce<=right_limit))
        condition_2 = (option_chain_dict['CE_totalTradedVolume']>
                       avg_ce_volume) \
                      and \
                      (option_chain_dict[
                           'PE_totalTradedVolume']>avg_pe_volume)
        
        if condition_1 and condition_2:
            str_result = "Straddles written at {0}.\n". \
                format(option_chain_dict['strikePrice'])
            
            return str_result
        
        return ''
    
    def get_strike_price(self, symbol: str, underlying: float) -> int:
        """

        :param symbol:
        :param underlying:
        :return:
        """
        # TODO: Needs to be extended for stock prices of varying range.
        multiple = self.index_option_chain_multiples[symbol]
        
        strike_price = ceil(underlying / multiple) * multiple
        
        return strike_price
