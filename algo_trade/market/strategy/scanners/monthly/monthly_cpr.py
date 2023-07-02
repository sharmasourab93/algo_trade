import pandas as pd
from datetime import datetime
from algo_trade.market.strategy.scanners.scanner_generics.cpr_strategy \
    import CPRAbstract
from algo_trade.utils.meta import AsyncLoggingMeta
from algo_trade.market.strategy.constants import CPR_STRATEGY_COLUMNS
from algo_trade.data_handler.calendar import MarketCalendarTools
from algo_trade.data_handler.calendar.constants import TODAY, DATE_FMT


class MonthlyCprStrategy(CPRAbstract, metaclass=AsyncLoggingMeta):
    CPR_FREQUENCY = "MONTHLY"

    def cpr_strategy_output(self) -> pd.DataFrame:

        self.logger.info("Initialized Monthly CPR Process")

        market_expiry = datetime.strptime(next(
            MarketCalendarTools.get_monthly_expiry()), DATE_FMT).date()

        # On the Previous day month and current day month not being the same
        # Monthly CPR is executed.

        if self.yf_utils.prev_day.month != self.yf_utils.next_day.month:

            data = self.yf_utils.processed_timeframe_bhavcopy(self.nse_n,
                                                              "month")

            self.logger.info("Monthly Quote DataFrame receivied. "
                             "Shape: {0}".format(data.shape))
            data = self.plot_pivots_with_cpr(data)
            self.logger.info("Monthly CPR plotted.")
            cpr_columns = CPR_STRATEGY_COLUMNS

            if self.order_by is not None:
                self.order_by = ["cpr_width"]

            try:
                data = data[cpr_columns].sort_values(by=self.order_by,
                                                     ascending=self.asc)

            except KeyError:
                cpr_columns.remove("time_con_range")
                cpr_columns.remove("purpose")
                data = data[cpr_columns].sort_values(by=self.order_by,
                                                     ascending=self.asc)

            data["frequency"] = self.CPR_FREQUENCY

            self.logger.info(
                "Execution of Weekly CPR Strategy Scanner Complete. "
                "Data Shape: {0}.".format(data.shape)
            )
            data = data.loc[~(data.priority_stocks == '-'), :].sort_values(by=['cpr_width'])
            return data.round(2)

        return pd.DataFrame()


if __name__ == '__main__':
    obj = MonthlyCprStrategy()
    obj.cpr_strategy_output()
