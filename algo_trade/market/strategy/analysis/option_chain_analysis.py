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
from algo_trade.market.strategy.constants import PCR_VERDICT_RANGE
from algo_trade.data_handler.calendar import MarketCalendarTools
from algo_trade.data_handler.source.constants import TRADEABLE_INDICES
from algo_trade.data_handler.calendar.constants import TIME_ZONE, TODAY


class OptionChainAnalysis(metaclass=AsyncLoggingMeta):

    def __init__(self):
        self.processor = DataHandler()
        self.index_option_chain_multiples = {'NIFTY': 50,
                                             'BANKNIFTY': 100,
                                             'FINNIFTY': 50}

    def get_select_stock_options_bhavcopy(self, symbol: str, expiry: str):

        expiry_set: list = self.get_all_monthly_expiries()
        fo_symbols: list = self.get_fo_list()
        ce_data, pe_data = self.get_stock_options_bhavcopy()

        if expiry in expiry_set and symbol in fo_symbols:
            ce_data = ce_data.loc[
                      (ce_data.Symbol == symbol) & (
                              ce_data.Expiry_dt == expiry), :
                      ]
            pe_data = pe_data.loc[
                      (pe_data.Symbol == symbol) & (
                              pe_data.Expiry_dt == expiry), :
                      ]

            return ce_data.round(2), pe_data.round(2)

        return None

    def get_select_stock_option_chain(
            self, symbol: str, expiry: str, strike: int = None
    ):

        expiries = self.get_all_monthly_expiries()
        all_symbols = self.get_fo_list()

        if symbol in all_symbols and expiry in expiries:
            data, _ = self.get_option_chain(symbol)

            return data

        return None

    def pcr_verdict(self, pcr: float) -> str:
        """
        Gets you the Put Call Ration based Verdict.
        :param pcr: float
        :return:
        """
        verdict_range = PCR_VERDICT_RANGE

        for limits, classification in verdict_range.items():
            lower_limit, upper_limit = limits

            if lower_limit <= pcr < upper_limit:
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
        while i < expiry_delta:
            expiry_date = next(expiry)
            i += 1

        option_chain_data = self.quote_option_chain(symbol, data,
                                                    expiry_date)

        return option_chain_data, expiry_date

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

        condition_1 = ((left_limit <= pcr <= right_limit) or
                       (left_limit <= price_pe_by_ce <= right_limit))
        condition_2 = (option_chain_dict['CE_totalTradedVolume'] >
                       avg_ce_volume) \
                      and \
                      (option_chain_dict[
                           'PE_totalTradedVolume'] > avg_pe_volume)

        if condition_1 and condition_2:
            str_result = "Straddles written at {0}.\n". \
                format(option_chain_dict['strikePrice'])

            return str_result

        return ''

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
                "symbol": symbol,
                "selected_expiry": expiry,
                "all_expiries": all_expiries,
                "last_updated": last_updated,
                "strikes": strike_prices,
                "underlying_value": underlying,
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

        if symbol == "NIFTY":
            ce_data = df.iloc[:, -19:]
            pe_data = df.iloc[:, :20]
        elif symbol == "FINNIFTY":
            ce_data = df.iloc[:, :20]
            pe_data = df.iloc[:, -19:]

        else:
            ce_data = df.iloc[:, :20]
            pe_data = df.iloc[:, 20:]

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
        merged_call_put.loc[merged_call_put.pcr_oi >= 10, "pcr_oi"] = 10.0
        overall_pcr = round((filtered['PE']['totOI'] /
                             filtered['CE']['totOI']), 2)
        pcr_verdict = self.pcr_verdict(overall_pcr)

        # Updating the final response.
        response.update(
            {
                "OptionChain": merged_call_put.round(2),
                "ResponseGenerateTime": datetime.now(tz=TIME_ZONE),
                "overall_pcr": overall_pcr,
                "pcr_verdict": pcr_verdict
            }
        )

        return response

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
                                  oc_df.CE_openInterest.max()) | (
                                         oc_df.PE_openInterest == oc_df.PE_openInterest.max()),
                       :].to_dict(orient='records')

        # This section only covers most liquid and actively trade options in
        # the option chain. The OI values are roughly between 0.1 and 10.
        min_max_pcr = oc_df.loc[
                      (0.1 <= oc_df.pcr_oi) & (oc_df.pcr_oi < 10), :]

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
                      (min_max_pcr.strikePrice < strike_price) & (
                              (min_max_pcr.pcr_oi <= 2) & (
                              min_max_pcr.pcr_oi >= 0.8)), :]

            resistance = min_max_pcr.loc[
                         (min_max_pcr.strikePrice >= strike_price) & (
                                 (min_max_pcr.pcr_oi < 0.8) & (
                                 min_max_pcr.pcr_oi >= 0.1)), :]

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

            support = min_max_pcr.loc[(min_max_pcr.strikePrice < strike_price),
                      :]
            support = support.loc[(support.PE_openInterest ==
                                   support.PE_openInterest.max()),
                      :].strikePrice.iloc[0]
            resistance = min_max_pcr.loc[
                         min_max_pcr.strikePrice > strike_price,
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
        symbol_expiry = {"NIFTY": 3, "BANKNIFTY": 3, "FINNIFTY": 1}
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
