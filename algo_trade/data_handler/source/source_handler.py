import pandas as pd
from datetime import datetime, date, timedelta
from algo_trade.data_handler.calendar.constants import DATE_FMT
from algo_trade.data_handler.source.yfin.yf_utils import YFUtils
from algo_trade.data_handler.source.nse.nse_data_config import NseDataConfig
from algo_trade.data_handler.source.constants import \
    NSE_CM_BHAVCOPY_COL_RENAMED, NSE_FO_LIQUID_STOCKS, YFIN_INDEX_LIST, \
    TRADEABLE_INDICES_NAME
from algo_trade.data_handler.exc import InvalidTicker


class SourceHandler(NseDataConfig, YFUtils):

    def __init__(self):
        super(SourceHandler, self).__init__()

    def get_q_results_date(self, data: pd.DataFrame, date_str: date,
                           days: int = 3):
        """
        Gets you q_results in a delta period of days.
        :param data:
        :param date_str:
        :param days:
        :return:
        """

        date_ = date_str

        start_date = date_ - timedelta(days=days)
        start_date = start_date

        end_date = date_ + timedelta(days=days)
        end_date = end_date

        values = self.nse_release_event(start_date, end_date)
        values = values.rename(columns={"date": "release_date"})
        data = pd.merge(data, values[["symbol", "purpose", "bm_desc"]],
                        on="symbol", how="left")
        data[["purpose", "bm_desc"]] = data[["purpose", "bm_desc"]].fillna("-")

        return data

    def highlight_select_stocks(
            self, data: pd.DataFrame,
            select_stocks: list = NSE_FO_LIQUID_STOCKS
    ) -> pd.DataFrame:
        """ Meant to highlight select stocks. """

        data["priority_stocks"] = "-"
        data.loc[
            data.symbol.isin(select_stocks), "priority_stocks"] = "Priority"

        return data

    def processed_bhavcopy(self, top_n: int = 1000) -> pd.DataFrame:
        """
        Method to generate Processed BhavCopy.
        We first check if the processed bhavcopy exists within the cache.
        Else we process the data for the new day and
        """

        # 1. We check if the data exists in the cache.
        # If it exists we return the data from the cache itself.
        if "processed_nse_list" in self.cache.loaded_dict.keys() \
                and self.cache["processed_nse_list"]["dated"] == self.next_day:
            nse_list = self.cache["processed_nse_list"]["data"]
            nse_list = nse_list.loc[nse_list.index < top_n, :]
            return nse_list

        # 2. We retrive daily bhavcopy, which is either downloaded or cached.
        data = self.nse_daily_bhavcopy(self.prev_day)

        # 3. We manipulate filter data.
        data = data.loc[data.SctySrs == 'EQ', :]
        data = data.rename(columns=NSE_CM_BHAVCOPY_COL_RENAMED)
        data = data[list(NSE_CM_BHAVCOPY_COL_RENAMED.values())]

        # 4. We get updated/fresh NSE list.
        nse_list = self.get_top_n_stocks()
        nse_list = pd.merge(nse_list, data, on="symbol", how="left")

        # 5. Remove quotes with NA and calculate % change.
        nse_list = self.data_remove_quotes_with_na(nse_list)
        data = self.calculate_pct_change(nse_list, 'close', 'prev_close',
                                         'pct_change')

        # 6. Release events for a given stock.
        nse_list = self.get_q_results_date(nse_list, self.next_day)

        # 7. Highlight Select stocks.
        nse_list = self.highlight_select_stocks(nse_list)

        # 8. Processed NSE list being stored in the cache.
        self.cache["processed_nse_list"] = {"data": nse_list,
                                            "dated": self.next_day}

        # 9. Return the top_n stocks.
        return nse_list.loc[nse_list.index < top_n, :]

    def yfin_index_data(self, index: str, period='1y', interval='1d'):

        try:
            data = self.get_period_data(YFIN_INDEX_LIST[index], period,
                                        interval, index=True)

        except KeyError:
            msg = "Invalid Ticker: {0}".format(index)
            InvalidTicker(msg)

        data["symbol"] = index
        return data

    def yf_run_all_indices(self):

        keys = list(YFIN_INDEX_LIST.keys())

        data = [self.yfin_index_data(i) for i in keys]

        data = pd.concat(data)

        return data


if __name__ == '__main__':
    obj = SourceHandler()
    obj.yf_run_all_indices()
