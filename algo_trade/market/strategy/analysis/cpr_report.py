import pandas as pd
from algo_trade.market.strategy.scanners.intraday.intraday_cpr import \
    IntradayStockCPRStrategy
from algo_trade.market.strategy.scanners.positional.weekly_cpr_strategy \
    import WeeklyCPRStrategy
from algo_trade.market.strategy.scanners.monthly.monthly_cpr import \
    MonthlyCprStrategy


def cpr_report():
    cprs = (IntradayStockCPRStrategy(), WeeklyCPRStrategy(),
            MonthlyCprStrategy())
    data = list()
    for i in cprs:
        data.append(i.cpr_strategy_output())
        i.logger.info("CPR Strategy for {0} completed.".format(i))

    data = pd.concat(data)

    return data


if __name__ == '__main__':
    result = cpr_report()
