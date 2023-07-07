from pandas import concat
from datetime import date
import re
import os
from typing import Dict, List
from algo_trade.data_handler import DataHandler
from algo_trade.utils.meta import AsyncLoggingMeta
from algo_trade.market.strategy.analysis.spot_index_analysis import \
    DailyIntradayIndicesReport
from algo_trade.market.strategy.analysis.option_chain import \
    OptionChainAnalysis
from algo_trade.utils.constants import ROOT_DIR


class MarketReport(metaclass=AsyncLoggingMeta):
    """
    We follow a standard template to
    provide us with an overall texture of the market
    and the day that went by.
    """
    
    def write_to_text(self, analysis: str, date_: date):
        file_path = os.path.join(ROOT_DIR, "data",
                                 "post_market_analysis_{0}.txt".format(date_))
        with open(file_path, "w+") as file:
            file.write(analysis)
    
    def __init__(self):
        self.data_handler = DataHandler()
        self.next_day = self.data_handler.next_day
        self.prev_day = self.data_handler.prev_day
    
    def verify_fii_dii_trades(self) -> dict:
        data = self.data_handler.nse_api_fii_dii_trade()
        
        return data.to_dict(orient='split')
    
    def identify_fno_sec_ban(self) -> str:
        data = self.data_handler.nse_daily_bhavcopy(self.next_day,
                                                    'FnoSec Ban')
        return re.sub(r"\n\d+,", ", ", data)
    
    def sectoral_view(self, head: int = 5):
        """ Gets you top 5 performing sectoral View for the day. """
        
        data = self.data_handler.get_nse_all_indices()
        columns = ["index", "percentChange"]
        filters = ["SECTORAL INDICES"]
        symbols = ["NIFTY 50", "NIFTY 100", "NIFTY 500"]
        
        # Index Filters.
        data_1 = data.loc[data["index"].isin(symbols), :]
        data_2 = data.loc[data.key.isin(filters), :]
        data = concat([data_1, data_2])
        data = data[columns]
        
        # Sort by %.
        data = data.sort_values(by=["percentChange"], ascending=False)
        data = data.head(head)
        data[columns[-1]] = data.percentChange.astype(str) + "%"
        
        data = data.to_dict(orient="split")['data']
        data = [" ".join(i) for i in data]
        
        return '\n'.join(data)
    
    def advance_decline(self):
        data = self.data_handler.nse_daily_bhavcopy(self.prev_day)
        data = data.loc[data.SctySrs == 'EQ', :]
        data["pctChange"] = ((data["ClsPric"] - data["PrvsClsgPric"]) / data[
            "PrvsClsgPric"])
        data["adv_dec"] = data.pctChange.apply(lambda x:1 if x>=0 else 0)
        
        data = "*Advance/Decline Ratio:*" + '/'. \
            join(map(str, data.adv_dec.value_counts().to_list()))
        
        return data
    
    def top_gainers_loosers(self) -> Dict[str, list]:
        """
        Top 5 Gainers & Top 5 Loosers
        :return:
        """
        
        index_movers = 100
        data = self.data_handler.processed_bhavcopy(100)
        data = data[["symbol", "pct_change", "purpose"]].round(2)
        top_5 = data.sort_values(by=["pct_change"], ascending=False).head(5)
        top_5["flag"] = top_5.purpose.apply(lambda x:-1 if x != "-" else 1)
        top_5 = top_5[["symbol", "pct_change", "flag"]]
        top_5 = top_5.to_dict(orient='records')
        
        bottom_5 = data.sort_values(by=["pct_change"]).head(5)
        bottom_5["flag"] = bottom_5.purpose.apply(lambda x:-1 if x != "-"
        else 1)
        bottom_5 = bottom_5[["symbol", "pct_change", "flag"]]
        bottom_5 = bottom_5.to_dict(orient='records')
        
        result = {"top_5":top_5,
                  "bottom_5":bottom_5}
        
        return result
    
    def get_indices(self) -> List[list]:
        """
        Runs you through a summary chief tradeable indices.
        :return:
        """
        indices = self.data_handler.yf_run_all_indices()
        indices = indices[["symbol", "close", "pct_change"]].round(2)
        
        return indices.to_dict(orient='split')['data']
    
    def day_closing_report_dict(self) -> dict:
        indices_report = self.get_indices()
        gainer_looser = self.top_gainers_loosers()
        
        return {"index_report":indices_report,
                "top_5":gainer_looser['top_5'],
                "bottom_5":gainer_looser['bottom_5']
                }
    
    def post_market_report_dict(self):
        """ Post Market Report with Analysis. """
        # TODO: Create post market report in dict format.
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
    
    def post_market_indices_report(self) -> str:
        """
        A complete post market Analysis in text.
        1. Nifty, Bank Nifty & Finnifty View with Option Chain Analysis
        2. Advance Decline Ration
        3. Top 5 Sectoral Performers
        4. FII / DII
        5. Stocks on Ban.

        Meant to handle all the technical analysis.
        """
        
        indices_report = DailyIntradayIndicesReport()
        option_chain = OptionChainAnalysis()
        indices = indices_report.indices_report()
        indices = indices.to_dict(orient='split')['data']
        
        # Option Chain Analysis.
        post_market_analysis = option_chain.all_indices_analysis()
        post_market_analysis.insert(1, '')
        analysis = list(zip(indices, post_market_analysis))
        text = '*{0} {1} {2}%*, ' \
               '\nCPR: {3} {4}, ' \
               '\nProbable Day Range High/Low: {5}/{6}, ' \
               '\nProbable Week Range High/Low: {7}/{8}, ' \
               '\nProbable Monthly Range High/Low: {9}/{10}. ' \
               '\n{11}'
        text_list = [text.format(*i, j) for i, j in analysis]
        
        text = '*Post Market Analysis: {0}*\n\n'.format(
            self.prev_day.strftime("%d-%b-%Y"))
        text += '\n'.join(text_list)
        
        # FII/DII, Advance Decline, Sectoral Performers and Sec bans.
        fii_dii = self.verify_fii_dii_trades()
        adv_dec = self.advance_decline()
        sec = "\n*Top 5 Sectoral Performers*:\n" + self.sectoral_view()
        fii_title = "\n*FII/DII*:\n" \
                    "{0}  {2} - {3} = {4}\n".format(*[i.capitalize() for i
                                                      in fii_dii['columns']])
        dii_columns = "{0}  {2} {3} = {4}".format(*fii_dii['data'][0])
        fii_columns = "\n{0}  {2} {3} = {4}".format(*fii_dii['data'][1])
        fii_dii = fii_title + dii_columns + fii_columns
        fno_secban = self.identify_fno_sec_ban()
        text += '\n' + adv_dec + '\n' + sec + "\n" + fii_dii + \
                "\n" + "\n" + fno_secban
        
        self.write_to_text(text, self.prev_day)
        self.telegram.send_message(text)
        
        return text


if __name__ == '__main__':
    obj = MarketReport()
    report = obj.post_market_indices_report()
    print(report)
