from pandas import DataFrame, to_datetime, merge, concat
from datetime import datetime, date
from numpy import where
from typing import Any
from dateutil.relativedelta import relativedelta
from algo_trade.utils.meta.async_meta import AsyncLoggingMeta
from algo_trade.data_handler.source.nse import nse_web_map, nsedata_cache
from algo_trade.data_handler.network_tools import DownloadTools
from algo_trade.data_handler.source.constants import TRADEABLE_INDICES_NAME
from algo_trade.data_handler.calendar.constants import TODAY, DATE_FMT
from algo_trade.data_handler.source.data_utils import DataUtils, \
    merge_quote_option_chain


class NseDataConfig(DataUtils, metaclass=AsyncLoggingMeta):

    def __init__(self):
        super(NseDataConfig, self).__init__()
        self.nse_map = nse_web_map()
        self.cache = nsedata_cache()
        self.download_tools = DownloadTools()
        self.headers = self.nse_map.nse_headers_simple

    def nse_release_event(self, from_date: date, to_date: date,
                          symbol: str = None):
        """ Gets you release dates for all stocks. """
        
        url, dt_format = self.nse_map.nse_api_release_calendar

        from_date = from_date.strftime(dt_format)
        to_date = to_date.strftime(dt_format)
        if symbol is not None:
            url = url.format(from_date, to_date, symbol)
        else:
            url = url.format(from_date, to_date, '')

        data = self.download_tools.get_request_api(url, self.headers)

        data = DataFrame(data.json())
        return data

    @merge_quote_option_chain
    def nse_option_chain(self, key: str, symbol: str) -> dict:

        """
        Makes API request on the NSE website for index/equity
        option-chain and returns the data in dict format.
        """

        if key == 'equity':
            url = self.nse_map.nse_equity_option_chain(symbol)

        else:
            url = self.nse_map.nse_index_option_chain(symbol)

        data = self.download_tools.get_request_api(url, self.headers,
                                                   cookies)
        self.logger.debug(
            "NSE Option Chain: Key-{0}, Symbol-{1}".format(key, symbol))
        return data.json()

    def nse_symbol_info(self, key: str, symbol: str) -> dict:

        """ Gets you the quote for the select Stock or Option Ticker. """

        if key == 'equity':
            url = self.nse_map.nse_equity_quote(symbol)

        else:
            url = self.nse_map.nse_option_index(symbol)

        # cookies = self.download_tools.get_cookies(url, self.headers)
        data = self.download_tools.get_request_api(url,
                                                   self.nse_map.nse_headers_advanced,
                                                   cookies)

        self.logger.debug(
            "NSE Quote Symbol: Key-{0}, Symbol-{1}".format(key, symbol))

        return data.json()

    def nse_holiday(self, key: str = 'CM') -> DataFrame:
        """ Gets you Holidays from NSE API. Key for Cash Market. """

        if 'holidays' in self.cache.loaded_dict.keys():
            return self.cache['holidays']

        url = self.nse_map.nse_api_holiday
        # cookies = self.download_tools.get_cookies(url, self.headers)
        data = self.download_tools.get_request_api(url, self.headers)
        data = data.json()[key]
        self.logger.debug("NSE Holidays Generated. Key: {0}".format(key))

        result = DataFrame(data)
        result = result.rename(columns={'tradingDate': 'trade_day',
                                        'weekDay': 'week_day',
                                        'Sr_no': 'index'}).set_index('index')

        result["trade_day"] = to_datetime(result.trade_day).apply(lambda x:
                                                                  x.date())

        return result

    def nse_api_fii_dii_trade(self) -> DataFrame:

        """ NSE API to fetch FII/DII Trade details. """

        url = self.nse_map.nse_api_fii_dii_report

        # cookies = self.download_tools.get_cookies(url, self.headers)
        data = self.download_tools.get_request_api(url, self.headers)

        data = DataFrame(data.json())
        data.columns = ["category", "date", "buy", "sell", "net"]

        data["date"] = to_datetime(data.date).apply(lambda x: x.date())

        return data

    def nse_daily_bhavcopy(self, date_str: date, key: str = 'EQ') -> \
            DataFrame:

        """
        Gets you Downloaded Bhavcopy in the dataframe format.
        This method checks if the bhavcopy is already downloaded and cached
        in the pickle.
        If not the case, it downloads it and stores it in the cache with a
        timestamp to differentiate.
        """

        if key not in ('EQ', 'FO'):
            bhav_key = "fo_secban"
        else:
            bhav_key = "DailyBhav_{0}".format(key)

        dated = date_str
        day, month, year = dated.strftime("%d-%m-%Y").upper().split("-")

        if bhav_key in self.cache.loaded_dict.keys() \
                and self.cache[bhav_key]["time_stamp"] == date_str.strftime(
            DATE_FMT).upper():
            self.logger.debug("Since the data for {0} is already "
                              "cached, "
                              "returning the data from cached "
                              "source.".format(date_str))
            return self.cache[bhav_key]["data"]

        if key == 'EQ':
            url = self.nse_map.nse_download_eq_bhavcopy
            url = url.format(year, month, day, dated.strftime("%b").upper())

        elif key == 'FO':
            url = self.nse_map.nse_download_fo_bhavcopy.format(year,
                                                               dated.strftime(
                                                                   "%b").upper(),
                                                               day)
        else:
            url = self.nse_map.nse_download_fo_secban.format(day, month, year)

        data = self.download_tools.download_data(url, self.headers)

        self.cache[bhav_key] = {"data": data, "time_stamp": date_str}

        self.logger.debug("{0} Cached for {1}.".format(bhav_key, date_str))

        return data

    def nse_fo_mktlots(self) -> DataFrame:
        """Download FO Mktlots file."""
        url = self.nse_map.nse_download_fo_mktlots

        data = self.download_tools.download_data(url, self.headers)

        return data

    def update_fno_list(self, *, mk_start: int = 4):

        """ Method which triggers update of fno list with in the cache. """

        data = self.nse_fo_mktlots()

        cols = data.iloc[mk_start]

        index, fno = data.loc[:(mk_start - 1), :], data.loc[(mk_start + 1):, :]

        # Index dataframe manipulation
        index.columns = index.columns.str.strip()
        index.rename(columns={"UNDERLYING": "underlying",
                              "SYMBOL": "symbol"}, inplace=True)

        # FNO Dataframe manipulation.
        fno.columns = cols
        fno.columns = fno.columns.str.strip()
        fno = fno.iloc[:, [0, 1, 2, 3, 4]]
        fno.columns.values[0] = "underlying"
        fno.columns.values[1] = "symbol"
        fno["symbol"] = fno.symbol.str.strip()

        self.cache['fo_data'] = fno
        self.cache['index_lots'] = index

        self.logger.info("Stock F&O Data and Index F&O data updated.")

    def fno_curr_month_update(self, date_fmt: str) -> str:
        """ Support function which evaluates curr month in options. """

        curr_month = date.today().strftime(date_fmt).upper()
        fo_data = self.cache.fno_data

        try:
            value = fo_data[curr_month]

        except KeyError:

            try:
                curr_month = date.today() + relativedelta(months=1)
                curr_month = curr_month.strftime(date_fmt).upper()
                value = fo_data[curr_month]

            except KeyError:
                self.update_fno_list()

        return curr_month

    def make_nse_co_list(self, fo_date_fmt: str = "%b-%y") -> DataFrame:

        """
        This method updates the NSE List based on the FNO Lots,
        Market Cap list which is issued by NSE once every six months.
        """

        mcap_list = self.cache.mcap
        curr_month = self.fno_curr_month_update(fo_date_fmt)
        fo_data = self.cache.fno_data

        mcap_list["FNO"] = where(
            mcap_list.symbol.isin(fo_data.symbol.to_list()), "Yes", "No")

        mcap = merge(mcap_list, fo_data[["symbol", curr_month]],
                     on="symbol", how="left").rename(
            columns={curr_month: "lotsize"})

        sectors_data = self.cache.sectoral_data

        mcap = merge(
            mcap,
            sectors_data[["symbol", "industry", "sectoral_index"]],
            on="symbol",
            how="left",
        )

        mcap["lotsize"] = mcap.lotsize.fillna(0)
        mcap["industry"] = mcap.industry.fillna("-")
        mcap["sectoral_index"] = mcap.sectoral_index.fillna("-")

        self.cache['nse'] = mcap

        return mcap

    def update_nse_list(self) -> DataFrame:
        """
        Update NSE List, triggers creation of NSE list a fresh
        with updated details and scrips.
        """

        if self.next_day.month != self.prev_day.month:
            nse_list = self.make_nse_co_list()
            self.logger.info("NSE List refreshed on the on-set of new Month. ")
            return nse_list

        return self.cache.nse_list

    def get_top_n_stocks(self, nse_top: int = 1000) -> DataFrame:
        """ Gets you the top N number of stocks ordered by Market Cap. """
        data = self.update_nse_list()

        data = data.loc[data.index < nse_top, :]

        return data

    def get_all_indices(self) -> DataFrame:
        """
        Returns quotes for all the indices at one place.
        We return Keys - Thematic, sectoral, broad market ...
        indices - All the Indices  and the data.

        """

        url = self.nse_map.nse_indices
        # cookies = self.download_tools.get_cookies(url, self.headers)
        data = self.download_tools.get_request_api(url, self.headers, cookies)

        data = DataFrame(data.json()['data'])
        keys = data.key.unique().tolist()
        indices = data.indexSymbol.unique().tolist()

        return data, keys, indices

    def get_quote_index(self, symbol: str, key: str = 'metadata') -> DataFrame:

        """ Gets you the quote of the selected index.  """

        url = self.nse_map.nse_quote_indices.format(symbol.upper())
        # cookies = self.download_tools.get_cookies(url, self.headers)
        data = self.download_tools.get_request_api(url, self.headers, cookies)
        data = data.json()

        if key == 'metadata':

            df = DataFrame([data[key]])
            df = df.rename(columns={"indexName": "index",
                                    "previousClose": "prev_close",
                                    "percChange": "pct_change",
                                    "timeVal": "timestamp",
                                    "yearHigh": "year_high",
                                    "yearLow": "year_low",
                                    "last": "close"})
            df = df.iloc[:, range(0, len(df.columns.to_list()) - 3)]

            return df

        else:
            # Key here is mostly 'data'.

            data = DataFrame(data[key])

            return data

    def get_india_vix(self, from_date: str, to_date: str) -> DataFrame:

        """ Method to retrieve VIX data. """

        vix_url = self.nse_map.nse_india_vix.format(from_date, to_date)
        # cookies = self.download_tools.get_cookies(vix_url, self.headers)
        data = self.download_tools.get_request_api(vix_url, self.headers,
                                                   cookies)
        # Response data manipulation.
        data = DataFrame(data.json()['data'])
        data = data.iloc[:, range(1, 11)]
        data.columns = ["dated", "index", "open", "close", "high",
                        "low", "prev_close", "timestamp", "change",
                        "pct_change"]
        data["dated"] = to_datetime(data.dated).apply(lambda x:
                                                      x.date())
        data[["year_high", "year_low"]] = 0
        data = data[["index", "open", "high", "low", "close", "prev_close",
                     "change", "pct_change", "dated", "year_high", "year_low"]]
        data = data.rename(columns={"dated": "timestamp"})

        return data

    def run_quote_index(self, dt_fmt: str = "%d-%m-%Y"):

        data = [self.get_quote_index(i) for i in TRADEABLE_INDICES_NAME]

        from_date = self.prev_day

        to_date = self.next_day

        vix = self.get_india_vix(from_date.strftime(dt_fmt),
                                 to_date.strftime(dt_fmt))
        cols = data[0].columns.to_list()
        data.append(vix[cols])
        df = concat(data)

        return df

    def nse_cache_data(self, key: str, value: Any):

        """ Facilitator method to cache data. """

        self.cache[key] = value


if __name__ == '__main__':
    obj = NseDataConfig()
    data = obj.nse_option_chain('index', 'NIFTY')
    print(data)
