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

        self.logger.debug("Initialized Monthly CPR Process")

        market_expiry = datetime.strptime(next(
            MarketCalendarTools.get_monthly_expiry()), DATE_FMT).date()

        # On the day when the prev day falls on the market expiry
        # and the next day is greater than the market expiry, we execute
        # the monthly cpr strategy process.

        if self.yf_utils.prev_day <= market_expiry < self.yf_utils.next_day:

            data = self.yf_utils.processed_timeframe_bhavcopy(self.nse_n,
                                                              "month")

            data = self.plot_pivots_with_cpr(data)

            cpr_columns = CPR_STRATEGY_COLUMNS

            if self.order_by is not None:
                self.order_by = ["cpr_width"]

            try:
                data = data[cpr_columns].sort_values(by=self.order_by,
                                                     ascending=self.asc)

            except KeyError:
                cpr_columns.remove("con_range")
                cpr_columns.remove("purpose")
                data = data[cpr_columns].sort_values(by=self.order_by,
                                                     ascending=self.asc)

            data["Frequency"] = self.CPR_FREQUENCY

            self.logger.debug(
                "Execution of Weekly CPR Strategy Scanner Complete. "
                "Data Shape: {0}.".format(data.shape)
            )

            return data.round(2)

        return pd.DataFrame()


if __name__ == '__main__':
    obj = MonthlyCprStrategy()
    obj.cpr_strategy_output()
