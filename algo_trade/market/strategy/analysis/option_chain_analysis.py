import pandas as pd
from pandas import DataFrame
from pandas import json_normalize
from algo_trade.data_handler import DataHandler
from algo_trade.utils.meta import AsyncLoggingMeta
from algo_trade.data_handler.calendar import MarketCalendarTools
from algo_trade.data_handler.source.constants import TRADEABLE_INDICES


# TODO: Option Chain Analysis
class OptionChainAnalysis(metaclass=AsyncLoggingMeta):

    def __init__(self):
        self.processor = DataHandler()

    def get_option_chain(self, symbol: str):

        """ Method that toggles between Stock & Index Option chain. """

        symbol = symbol.upper()

        if symbol in TRADEABLE_INDICES:

            key: str = "index"
            expiry = MarketCalendarTools.get_weekly_expiry()

        else:
            key: str = "equity"
            expiry = MarketCalendarTools.get_monthly_expiry()

        data = self.processor.nse_option_chain(key, symbol)
        expiry = next(expiry)

        option_chain_data = self.quote_option_chain(symbol, data, expiry)

        return option_chain_data

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
            data = self.get_option_chain(symbol)

            return data

        return None

    def get_select_index_option_chain(
            self, symbol: str, expiry: str, strike: int = None
    ):

        expiries = self.get_all_weekly_expiries()
        all_symbols = self.get_processed_index_names()

        if symbol in all_symbols and expiry in expiries:
            data = self.get_option_chain_index(symbol)

            return data

        return None

    def quote_option_chain(self, symbol: str, data: dict, expiry: str):

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

        # if strike is not None:
        #     df = df.loc[df.strikePrice == strike, :]

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
        # Call Options
        if "NIFTY" in symbol:
            ce_data = df.iloc[:, -19:]
            pe_data = df.iloc[:, :20]
        else:
            ce_data = df.iloc[:, :20]
            pe_data = df.iloc[:, 20:]

        ce_data.columns = ce_data.columns.str.replace(r"CE\.", "")
        ce_data = ce_data[columns].round(2)
        ce_data.columns = "CE_" + ce_data.columns

        # ce_data.columns = pd.MultiIndex.from_product([[options[0]],
        #                                               ce_data.columns.tolist()])
        ce_data.reset_index(inplace=True)

        # Put Options
        pe_data.columns = pe_data.columns.str.replace(r"PE\.", "")
        pe_data = pe_data[columns].round(2)
        pe_data.columns = "PE_" + pe_data.columns
        # pe_data.columns = pd.MultiIndex.from_product([[options[1]],
        #                                                pe_data.columns.tolist()])
        pe_data.reset_index(inplace=True)

        merged_call_put = pd.merge(ce_data, pe_data, on="strikePrice",
                                   how="left")
        # response.update({'OptionChain': merged_call_put.to_dict('records')})
        response.update(
            {
                "OptionChain": merged_call_put.round(2),
                "ResponseGenerateTime": datetime.now(tz=TIME_ZONE),
            }
        )
        end_time = round(perf_counter() - start_time, 2)

        return response


if __name__ == '__main__':
    obj = OptionChainAnalysis()

    response = obj.get_option_chain('NIFTY')
    print(response)
