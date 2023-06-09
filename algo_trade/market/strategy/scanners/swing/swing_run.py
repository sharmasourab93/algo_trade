import sys
from inspect import isclass, getmembers
from algo_trade.utils import write_df_to_file
from algo_trade.data_handler.calendar.constants import DATE_FMT

from algo_trade.market.strategy.scanners.scanner_generics.swing_generic import store_swing_output, retrieve_swing_output
from algo_trade.market.strategy.scanners.swing.swing_buying_pullback import \
    SwingBuyingPullBackScanner
from algo_trade.market.strategy.scanners.swing.swing_volume_scanner import \
    SwingVolumeScanner
from algo_trade.market.strategy.scanners.swing.swing_ema_crossover_scanner \
    import SwingMovingAverageCrossOver


def swing_result():
    class_list = list()
    data = retrieve_swing_output()
    if data is not None:
        return data

    for name, obj in getmembers(sys.modules[__name__]):
        if isclass(obj):
            _ = obj()
            class_list.append(_)

    class_list = [i.generate_swing_output() for i in class_list]

    data = _.get_swing_result()
    store_swing_output(data)
    return data


if __name__ == '__main__':
    result = swing_result()

    from datetime import datetime

    dated_ = datetime.today().strftime(DATE_FMT)

    write_df_to_file(result, "{0}_{1}".format("SWINGRUN", dated_))
