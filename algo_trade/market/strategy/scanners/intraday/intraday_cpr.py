import pandas as pd
import asyncio
from algo_trade.utils.meta import AsyncLoggingMeta
from algo_trade.market.strategy.scanners.scanner_generics.cpr_strategy \
    import CPRAbstract
from algo_trade.market.strategy.constants import CPR_STRATEGY_COLUMNS


class IntradayStockCPRStrategy(CPRAbstract, metaclass=AsyncLoggingMeta):

    def cpr_strategy_output(self):
        """
        `Daily CPR Strategy Scanner Output`
        """
        self.logger.debug("Initiated Process to get Processed BhavCopy for "
                          "{0}.".format(self.CPR_FREQUENCY))
        data = self.yf_utils.processed_bhavcopy(self.nse_n)

        self.logger.debug(
            "Processed BhavCopy Received. Shape: {0}".format(data.shape))
        self.logger.debug(
            "Processed Data containing Columns: [{0}]".format(
                ", ".join(data.columns.to_list())
            )
        )

        data = self.plot_pivots_with_cpr(data)

        # To identify Consolidation Range.
        if self.consolidation:
            tickers = data.symbol.values.tolist()

            cpr_columns = CPR_STRATEGY_COLUMNS

            consol_count = self.consol_range_obj.get_time_consolidation(
                tickers)

            data = pd.merge(data, consol_count, on="symbol", how="left")

        else:
            cpr_columns.remove("con_range")

        # Ordering by columns.
        if self.order_by is None:
            self.order_by = ["cpr_width"]

        data = data[cpr_columns].sort_values(by=list(self.order_by),
                                             ascending=self.asc)

        # If VCPR toggle is True.
        if self.vcpr_analysis:
            vcpr_symbols = data.loc[
                data[
                    "cpr_width"] <= self.vcpr_cpr_cutoff, "symbol"].values.tolist()
            vcpr_symbols = asyncio.run(self.identify_ticker_with_vcpr(
                vcpr_symbols))
            vcpr_data = pd.DataFrame(vcpr_symbols).transpose().reset_index()
            vcpr_data.columns = ["symbol", "long_vcpr", "short_vcpr"]

            data = pd.merge(data, vcpr_data, on="symbol", how="left").fillna(
                "-")

        data["frequency"] = self.CPR_FREQUENCY

        self.logger.info(
            "Execution of Daily CPR Strategy Scanner Complete. "
            "Data Shape: {0}.".format(data.shape)
        )

        return data.round(2)


if __name__ == '__main__':
    obj = IntradayStockCPRStrategy()

    result = obj.cpr_strategy_output()
