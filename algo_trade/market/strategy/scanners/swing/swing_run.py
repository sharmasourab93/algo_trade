import sys
from inspect import isclass, getmembers

from algo_trade.market.strategy.scanners.swing.swing_buying_pullback import \
    SwingBuyingPullBackScanner
from algo_trade.market.strategy.scanners.swing.swing_volume_scanner import \
    SwingVolumeScanner


def swing_result():
    class_list = list()
    for name, obj in getmembers(sys.modules[__name__]):
        if isclass(obj):
            _ = obj()
            class_list.append(_)

    class_list = [i.generate_swing_output() for i in class_list]

    return _.get_swing_result()


if __name__ == '__main__':
    result = swing_result()

    print(result)
