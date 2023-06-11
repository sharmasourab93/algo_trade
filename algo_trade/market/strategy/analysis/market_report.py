from typing import Dict, List
from algo_trade.data_handler import DataHandler
from algo_trade.utils.meta import AsyncLoggingMeta
from algo_trade.market.strategy.analysis.spot_index_analysis import \
    DailyIntradayIndicesReport
from algo_trade.market.strategy.analysis.option_chain_analysis import \
    OptionChainAnalysis


class MarketReport(metaclass=AsyncLoggingMeta):
    """
    We follow a standard template to
    provide us with an overall texture of the market
    and the day that went by.
    """

    def __init__(self):
        self.data_handler = DataHandler()
        self.next_day = self.data_handler.next_day
        self.prev_day = self.data_handler.prev_day

    def verify_fii_dii_trades(self) -> dict:
        """
        Brief summary of FII & DII Trades.
        Sourced from NSE Website.
        :return:
        """
        data = self.data_handler.nse_api_fii_dii_trade()

        return data.to_dict(orient='split')

    def identify_fno_sec_ban(self) -> str:
        """
        Identify stocks under Security Ban by NSE.
        :return:
        """
        data = self.data_handler.nse_daily_bhavcopy(self.next_day,
                                                    'FnoSec Ban')
        headers = data.columns.to_list()[0]
        if 0 in data.shape:
            value = '[]'

        else:
            value = ', '.join(data[headers].values.tolist())

        return headers + value

    def top_gainers_loosers(self) -> Dict[str, list]:
        """
        Top 5 Gainers & Top 5 Loosers
        :return:
        """

        index_movers = 100
        data = self.data_handler.processed_bhavcopy(100)
        data = data[["symbol", "pct_change", "purpose"]].round(2)
        top_5 = data.sort_values(by=["pct_change"], ascending=False).head(5)
        top_5["flag"] = top_5.purpose.apply(lambda x: -1 if x != "-" else 1)
        top_5 = top_5[["symbol", "pct_change", "flag"]]
        top_5 = top_5.to_dict(orient='records')

        bottom_5 = data.sort_values(by=["pct_change"]).head(5)
        bottom_5["flag"] = bottom_5.purpose.apply(lambda x: -1 if x != "-"
        else 1)
        bottom_5 = bottom_5[["symbol", "pct_change", "flag"]]
        bottom_5 = bottom_5.to_dict(orient='records')

        result = {"top_5": top_5,
                  "bottom_5": bottom_5}

        return result

    def get_indices(self) -> List[list]:
        """
        Runs you through a summary chief tradeable indices.
        :return:
        """
        indices = self.data_handler.yf_run_all_indices()
        indices = indices[["symbol", "close", "pct_change"]].round(2)

        return indices.to_dict(orient='split')['data']

    def day_closing_report(self) -> dict:
        """
        Day Closing Report.
        :return:
        """
        indices_report = self.get_indices()
        gainer_looser = self.top_gainers_loosers()

        report = {"index_report": indices_report,
                  "top_5": gainer_looser['top_5'],
                  "bottom_5": gainer_looser['bottom_5']
                  }

        return report

    def post_market_report(self):
        """ Post Market Report with Analysis. """

        index_data = self.post_market_indices_report()

        return index_data

    def day_close_in_text(self) -> str:

        report = self.day_closing_report()

        title = "Trade Like a Yogi: Market Closing Report for {0}".format(
            self.prev_day)
        indices_text = '\n'.join(['{0} {2}%({1})'.format(*i) for i in
                                  report["index_report"]])
        indices_text = "\n\nIndex Snapshot:\n" + indices_text
        top_5 = ['{0} {1}{2}'.format(i['symbol'], i['pct_change'],
                                     '' if i['flag'] == 1 else '*')
                 for i in report['top_5']]
        top_5 = "\n\n Top 5 Gainers: \n" + '\n'.join(top_5)
        bottom_5 = ['{0} {1}{2}'.format(i['symbol'], i['pct_change'],
                                        '' if i['flag'] == 1 else '*')
                    for i in report['bottom_5']]
        bottom_5 = "\n\n Top 5 Loosers: \n" + '\n'.join(bottom_5)

        text = title + indices_text + top_5 + bottom_5 + "\n\n\n * Event led."

        return text

    def post_market_indices_report(self):
        """
        Indices report with Day, week and month High Low range and CPR
        analysis.
        """

        indices_report = DailyIntradayIndicesReport()
        option_chain = OptionChainAnalysis()
        indices = indices_report.indices_report()
        indices = indices.to_dict(orient='split')['data']

        post_market_analysis = option_chain.all_indices_analysis()
        post_market_analysis.insert(1, '')

        analysis = list(zip(indices, post_market_analysis))
        text = '{0} {1} {2}%, ' \
               '\nCPR: {3} {4}, ' \
               '\nDay Range High/Low: {5}/{6}, ' \
               '\nWeek Range High/Low: {7}/{8}, ' \
               '\nMonthly Range High/Low: {9}/{10}. ' \
               '\n{11}'

        text_list = [text.format(*i, j) for i, j in analysis]

        text = '*Post Market Analysis: {0}*\n\n'.format(
            self.prev_day.strftime("%d-%b-%Y"))
        text += '\n'.join(text_list)

        fii_dii = self.verify_fii_dii_trades()

        fii_title = "\n*FII/DII*:\n" \
                    "{0}  {1} {2} - {3} = {4}\n".format(*fii_dii['columns'])
        dii_columns = "{0}  {1}  {2} {3} = {4}".format(*fii_dii['data'][0])
        fii_columns = "\n{0}  {1}  {2} {3} = {4}".format(*fii_dii['data'][1])

        fii_dii = fii_title + dii_columns + fii_columns

        fno_secban = self.identify_fno_sec_ban()

        text += fii_dii + "\n"
        # + fno_secban

        return text


if __name__ == '__main__':
    obj = MarketReport()
    report = obj.post_market_indices_report()
    print(report)
