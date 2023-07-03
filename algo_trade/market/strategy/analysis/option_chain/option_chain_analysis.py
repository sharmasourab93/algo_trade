from math import ceil
from datetime import datetime, timezone
from time import perf_counter
import pandas as pd
from numpy import inf, where
from pandas import DataFrame
from pandas import json_normalize
from typing import Dict, Union, Tuple, List
from time import perf_counter
from algo_trade.data_handler import DataHandler
from algo_trade.utils.meta import AsyncLoggingMeta
from algo_trade.market.strategy.analysis.option_chain import GenericOptionChain
from algo_trade.data_handler.calendar import MarketCalendarTools
from algo_trade.data_handler.source.constants import TRADEABLE_INDICES
from algo_trade.data_handler.calendar.constants import TIME_ZONE, TODAY


class OptionChainAnalysis(GenericOptionChain, metaclass=AsyncLoggingMeta):
    
    def __init__(self):
        super(OptionChainAnalysis, self).__init__()
        self.index_option_chain_multiples = {'NIFTY':50,
                                             'BANKNIFTY':100,
                                             'FINNIFTY':50}
    
    def index_option_chain_analysis(self,
                                    symbol: str,
                                    delta: int = 5,
                                    expiry_delta: int = 1) -> str:
        
        """
        Index Option Chain Analysis method.
        :param symbol:
        :param delta:
        :param expiry_delta:
        :return:
        """
        
        option_chain, expiry_date = self.get_option_chain(symbol,
                                                          expiry_delta=expiry_delta)
        underlying_value = option_chain["underlying_value"]
        oc_df = option_chain["OptionChain"]
        
        strike_price = self.get_strike_price(symbol, underlying_value)
        analysis_range = self.index_option_chain_multiples[symbol] * delta
        
        analysis_range = list(range(int(strike_price - analysis_range),
                                    int(strike_price + analysis_range),
                                    int(analysis_range / delta)))
        
        # Identify Max Call & Put OI. Can give 2 records or give 1 record.
        call_put_max = oc_df.loc[(oc_df.CE_openInterest ==
                                  oc_df.CE_openInterest.max())|(
                                         oc_df.PE_openInterest == oc_df.PE_openInterest.max()),
                       :].to_dict(orient='records')
        
        # This section only covers most liquid and actively trade options in
        # the option chain. The OI values are roughly between 0.1 and 10.
        min_max_pcr = oc_df.loc[
                      (0.1<=oc_df.pcr_oi)&(oc_df.pcr_oi<10), :]
        
        avg_pe_volume = round(float(min_max_pcr.PE_openInterest.mean()), 2)
        avg_ce_volume = round(float(min_max_pcr.CE_openInterest.mean()), 2)
        
        text_result = '*Option Chain Analysis for ' \
                      '{1} Expiry Due: {0}*\n'.format(expiry_date, symbol)
        
        # If we have 2 items in the Option chain with Max Calls and Max Puts.
        # We will have two strikes to identify and also mention Supports and
        # lower levels and resistances at higher levels.
        # Else:
        # We have situation where there are enough straddles to be looked at
        # with very little resistance and support within active/liquid strikes.
        if len(call_put_max) == 2:
            text_result += 'Max Put OI at: {0}. PCR: {1}.\n'.format(
                call_put_max[0]['strikePrice'], call_put_max[0]['pcr_oi'])
            text_result += 'Max Call OI at: {0}. PCR: {1}.\n'.format(
                call_put_max[1]['strikePrice'], call_put_max[1]['pcr_oi'])
            
            support = min_max_pcr.loc[
                      (min_max_pcr.strikePrice<strike_price)&(
                              (min_max_pcr.pcr_oi<=2)&(
                              min_max_pcr.pcr_oi>=0.8)), :]
            
            resistance = min_max_pcr.loc[
                         (min_max_pcr.strikePrice>=strike_price)&(
                                 (min_max_pcr.pcr_oi<0.8)&(
                                 min_max_pcr.pcr_oi>=0.1)), :]
            
            resistance = min_max_pcr.loc[(min_max_pcr.CE_openInterest ==
                                          min_max_pcr.CE_openInterest.max()),
                         :]
            
            if 0 not in support.shape:
                if 1 in support.shape:
                    text_result += 'Nearest Support at {0}.\n'. \
                        format(int(support.strikePrice.iloc[0]))
                
                else:
                    text_result += "Multiple supports near strike " \
                                   "such as {0}. \n".format(','.join(
                        map(str, support.strikePrice.to_list()[::-1])))
            else:
                
                text_result += "Very little to no support. Reversal possible. "
            
            if 0 not in resistance.shape:
                if 1 in resistance.shape:
                    text_result += "Resistance at {0}.".format(
                        resistance.strikePrice.iloc[0])
                
                else:
                    text_result += "Multiple resistances near strike such " \
                                   "as {0}.".format(', '.join(map(str,
                                                                  resistance.strikePrice.to_list())))
            
            else:
                text_result += "Not many resistances. Reversal possible.\n"
            
            straddle = [self.identify_straddle(i, avg_ce_volume,
                                               avg_pe_volume) for i in
                        call_put_max]
            straddle = [i for i in straddle if i != '']
            text_result += "\n{0}".format('.'.join(straddle))
        
        else:
            call_put_max = call_put_max[0]
            text_result += 'Max Put & Call OI at: {0}. PCR {1}. \n' \
                           ''.format(
                call_put_max['strikePrice'], call_put_max['pcr_oi'])
            
            support = min_max_pcr.loc[(min_max_pcr.strikePrice<strike_price),
                      :]
            support = support.loc[(support.PE_openInterest ==
                                   support.PE_openInterest.max()),
                      :].strikePrice.iloc[0]
            resistance = min_max_pcr.loc[
                         min_max_pcr.strikePrice>strike_price,
                         :]
            resistance = resistance.loc[(resistance.CE_openInterest ==
                                         resistance.CE_openInterest.max()),
                         :].strikePrice.iloc[0]
            text_result += 'Support at {0}. Resistance at {1}.'.format(support,
                                                                       resistance)
            
            # Conditions for identifying straddle.
            text_result += self.identify_straddle(call_put_max, avg_ce_volume,
                                                  avg_pe_volume)
        
        text_result += "Overall PCR: {0}. Verdict: {1}.\n". \
            format(option_chain["overall_pcr"], option_chain["pcr_verdict"])
        
        return text_result
    
    def all_indices_analysis(self, return_dict: bool = False) -> Union[dict,
    List[str]]:
        
        """
        We run a thorough Option chain analysis on each of the
        Tradable index namely : Nifty, Banknifty & Finnifty.

        :param return_dict:
        :return:
        """
        
        # Indicative of Thursday expiry in Nifty & Bank Nifty,
        # Tuesdays in FINNIFTY per weekday numbers.
        symbol_expiry = {"NIFTY":3, "BANKNIFTY":3, "FINNIFTY":1}
        analysis_results = list()
        for i, j in symbol_expiry.items():
            
            if TODAY.weekday() != j:
                output = self.index_option_chain_analysis(i,
                                                          expiry_delta=1)
                output += "\n"
            
            else:
                output = self.index_option_chain_analysis(i,
                                                          expiry_delta=2)
                output += "\n*Note*: Analysis generated on expiry day may " \
                          "not be accurate.\n"
            
            analysis_results.append(output)
        
        if not return_dict:
            return analysis_results
        
        return dict(zip(list(symbol_expiry.keys()), analysis_results))


if __name__ == '__main__':
    obj = OptionChainAnalysis()
    
    start = perf_counter()
    result = obj.all_indices_analysis()
    end = perf_counter() - start
    print("Executed in: {0}s".format(end))
    print(result)
