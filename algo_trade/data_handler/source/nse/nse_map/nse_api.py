import abc
from algo_trade.data_handler.source.constants import TRADEABLE_INDICES


class NSEApiMap(abc.ABC):
    """
    Abstract class which serves as a differentiator
    for extracting all api related urls/configs.
    """

    @property
    def nse_api_release_calendar(self):
        return (
            self.loaded_dict["Market"]["NSE"]["RELEASE"]["url"],
            self.loaded_dict["Market"]["NSE"]["RELEASE"]["DATE-FMT"]
        )

    @property
    def nse_api_block_deal(self):
        return (
                self.nse_domain
                + self.loaded_dict["Market"]["NSE"]["API"]["BLOCK-DEAL"]
        )

    @property
    def nse_api_derivative_master_list(self):
        return (
                self.nse_domain
                + self.loaded_dict["Market"]["NSE"]["API"]["DERIVATIVE"][
                    "MASTER-LIST"
                ]
        )

    @property
    def nse_api_derivative_option_chain_equity(self):
        return (
                self.nse_domain
                + self.loaded_dict["Market"]["NSE"]["API"]["DERIVATIVE"][
                    "OPTION-CHAIN"
                ]
        )

    def nse_equity_option_chain(self, symbol: str):
        data = self.nse_api_derivative_option_chain_equity.format(
            symbol.upper())

        return data

    @property
    def nse_api_derivative_option_chain_index(self):
        return (
                self.nse_domain
                + self.loaded_dict["Market"]["NSE"]["API"]["DERIVATIVE"][
                    "OPTION-INDEX"
                ]
        )

    def nse_index_option_chain(self, symbol: str):
        data = self.nse_api_derivative_option_chain_index.format(
            symbol.upper())

        return data

    @property
    def nse_api_derivative_option_index(self):
        return (
                self.nse_domain
                + self.loaded_dict["Market"]["NSE"]["API"]["DERIVATIVE"][
                    "QUOTE"]
        )

    def nse_option_index(self, symbol: str):
        symbol = symbol.upper()
        if symbol in TRADEABLE_INDICES:
            return self.nse_api_derivative_option_index.format(symbol.upper())

        raise KeyError(
            "{0} not in {1}".format(symbol, ', '.join(TRADEABLE_INDICES)))

    @property
    def nse_api_equity_meta(self):
        return (
                self.nse_domain
                + self.loaded_dict["Market"]["NSE"]["API"]["EQUITY"]["META"]
        )

    def nse_equity_meta_info(self, symbol: str):
        data = self.nse_api_equity_meta.format(symbol.upper())

        return data

    @property
    def nse_api_equity_quote(self):
        return (
                self.nse_domain
                + self.loaded_dict["Market"]["NSE"]["API"]["EQUITY"]["QUOTE"]
        )

    def nse_equity_quote(self, symbol: str):
        data = self.nse_api_equity_quote.format(symbol.upper())

        return data

    @property
    def nse_api_equity_trade(self):
        return (
                self.nse_domain
                + self.loaded_dict["Market"]["NSE"]["API"]["EQUITY"]["TRADE"]
        )

    def nse_equity_trade_info(self, symbol: str):
        data = self.nse_api_equity_trade.format(symbol.upper())

        return data

    @property
    def nse_api_etf_all(self):
        return (
                self.nse_domain
                + self.loaded_dict["Market"]["NSE"]["API"]["ETF"]["ETF-ALL"]
        )

    @property
    def nse_etf_quote(self):
        return (
                self.nse_domain
                + self.loaded_dict["Market"]["NSE"]["API"]["ETF"]["ETF-QUOTE"]
        )

    def nse_etf_quote_method(self, symbol: str):
        return self.nse_etf_quote.format(symbol.upper())

    @property
    def nse_api_holiday(self):
        return (
                self.nse_domain
                + self.loaded_dict["Market"]["NSE"]["API"]["HOLIDAY"][
                    "TRADING"]
        )

    @property
    def nse_equity_master(self):
        return (
                self.nse_domain
                + self.loaded_dict["Market"]["NSE"]["API"]["INDEX"][
                    "EQUITY-MASTER"
                ]
        )

    @property
    def nse_index_names(self):
        return (
                self.nse_domain
                + self.loaded_dict["Market"]["NSE"]["API"]["INDEX"][
                    "INDEX-NAMES"]
        )

    @property
    def nse_indices(self):
        return (
                self.nse_domain
                + self.loaded_dict["Market"]["NSE"]["API"]["INDEX"]["INDICES"]
        )

    @property
    def nse_quote_indices(self):
        return (
                self.nse_domain
                + self.loaded_dict["Market"]["NSE"]["API"]["INDEX"][
                    "QUOTE-INDEX"]
        )

    @property
    def nse_ipo_issues_all(self):
        return (
                self.nse_domain
                + self.loaded_dict["Market"]["NSE"]["API"]["ISSUES"]["IPO"]
        )

    @property
    def nse_ipo_past_issues_all(self):
        return (
                self.nse_domain
                + self.loaded_dict["Market"]["NSE"]["API"]["ISSUES"][
                    "PAST-ISSUES"])

    def nse_ipo_issues_past(self):
        url = self.nse_ipo_past_issues_all

        data = self.download_tools.get_request_api(url, self.headers)

        return data

    @property
    def nse_api_fii_dii_report(self):
        return (
                self.nse_domain
                + self.loaded_dict["Market"]["NSE"]["API"]["FII-DII-REPORT"]
        )

    @property
    def nse_india_vix(self):
        return self.nse_domain + self.loaded_dict["Market"]["NSE"]["API"][
            "INDEX"]["VIX"]
