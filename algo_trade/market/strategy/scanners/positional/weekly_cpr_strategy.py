import pandas as pd
from algo_trade.utils.meta import AsyncLoggingMeta
from algo_trade.market.strategy.scanners.scanner_generics.cpr_strategy \
    import CPRAbstract
from datetime import date, timedelta
from algo_trade.market.strategy.constants import CPR_STRATEGY_COLUMNS
from algo_trade.data_handler.calendar.constants import TODAY, DATE_FMT


class WeeklyCPRStrategy(CPRAbstract, metaclass=AsyncLoggingMeta):
    CPR_FREQUENCY = "WEEKLY"

    def cpr_strategy_output(self) -> pd.DataFrame:

        self.logger.debug("Initialized Weekly CPR Process")

        if self.yf_utils.next_day.isocalendar()[-1] == 1 or \
                self.yf_utils.prev_day.isocalendar()[-1] == 5:

            data = self.yf_utils.processed_timeframe_bhavcopy(self.nse_n)

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
    obj = WeeklyCPRStrategy()
    result = obj.cpr_strategy_output()
    print(result)
