import pandas as pd
from algo_trade.utils.meta import AsyncLoggingMeta
from algo_trade.market.strategy.scanners.scanner_generics.cpr_strategy \
    import CPRAbstract
from datetime import date, timedelta
from algo_trade.market.strategy.constants import CPR_STRATEGY_COLUMNS
from algo_trade.data_handler.calendar.constants import TODAY, DATE_FMT


class WeeklyCPRStrategy(CPRAbstract, metaclass=AsyncLoggingMeta):
    CPR_FREQUENCY = "WEEKLY"
    STRATEGY_NAME = "WeeklyCPRStrategy"
    
    def cpr_strategy_output(self) -> pd.DataFrame:
        
        week = "Week-{0}".format(self.yf_utils.next_day.isocalendar().week)
        self.logger.info("Initialized Weekly CPR Process")
        
        data = self.retrieve_outputs(week)
        
        if data is not None:
            return data
        
        if self.yf_utils.prev_day.isocalendar().week != self.yf_utils.next_day.isocalendar().week:
            
            data = self.yf_utils.processed_timeframe_bhavcopy(self.nse_n)
            
            data = self.plot_pivots_with_cpr(data)
            
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
            
            self.logger.debug(
                "Execution of Weekly CPR Strategy Scanner Complete. "
                "Data Shape: {0}.".format(data.shape)
                )
            
            data = data.sort_values(by=['cpr_width'])
            self.storing_outputs(data, week)
            
            return data.round(2)
        
        return pd.DataFrame()
    
    def weekly_narrow_cpr(self):
        
        result = self.cpr_strategy_output()
        
        result = result.loc[(result.cpr == "Narrow CPR")
                            &(result["index"]<=250), :].sort_values(by=["cpr_width"],
                                                                    ascending=True).head(20)
        
        self.telegram.send_message(result, cols=["symbol", "industry"], additional_text="Weekly Narrow CPR")
        
        return result


if __name__ == '__main__':
    obj = WeeklyCPRStrategy()
    result = obj.weekly_narrow_cpr()
    print(result)
