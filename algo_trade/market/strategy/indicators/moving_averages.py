import pandas as pd


class MovingAverages:
    
    def add_moving_averages(
            self,
            data: pd.DataFrame,
            column: str = 'close',
            ma: str = "EMA",
            ma_range: tuple = (20, 50, 200)
            ) -> pd.DataFrame:
        """
        Method to Add Moving Averages.
        Currently, includes only,
            i.  Exponential moving Averages & Data
            ii. Simple/Daily Moving Averages
        :param data:
        :param ma: EMA| SMA | DMA
        :param ma_range: Range of Moving Averages
        :return:
        """
        
        if ma == "SMA" or ma == "DMA":
            for i in ma_range:
                col = ma + "{0}".format(i)
                data[col] = data[column].rolling(i).mean()
        
        elif ma == "EMA":
            for i in ma_range:
                col = ma + "{0}".format(i)
                data[col] = data.sort_values(by=["date"])[column]. \
                                ewm(span=i,
                                    adjust=False).mean().reindex(). \
                                iloc[::-1].round(2)
        else:
            
            raise KeyError("Invalid Key passed for moving average. "
                           "Moving Average received: {0}".format(ma))
        return data
    
    # def moving_average_crossover(self,
    #                              data: pd.DataFrame,
    #                              ma1: str, ma2: str) -> pd.DataFrame:
    #     """
    #     Method to identify Moving Average Crossovers.
    #     :param data:
    #     :param ma1:
    #     :param ma2:
    #
    #     :returns: Dataframe with a column on Crossovers.
    #     """
    #
    #     prev_1 = data[ma1].shift(1)
    #     prev_2 = data[ma2].shift(1)
    #
    #     cross1 = ((data[ma1] <= data[ma2]) & (prev_1 >= prev_2))
    #     cross2 = ((data[ma1] >= data[ma2]) & (prev_1 <= prev_2))
    #
    #     data[ma1 + 'X' + ma2] = cross1
    #     data[ma2 + 'X' + ma1] = cross2
    #
    #     return data
    
    def moving_average_crossover(
            self, data: pd.DataFrame, ma1: str, ma2: str, ma3: str
            ) -> pd.DataFrame:
        """
        Method to identify position of a higher moving average, and
        it's crossover & it's position in percentile.

        :param data:
        :param ma1:
        :param ma2:
        :param ma3:
        :returns: Data Frame with Crossover
        """
        
        prev_1 = data[ma1].shift(1)
        prev_2 = data[ma2].shift(1)
        prev_3 = data[ma3].shift(1)
        
        cross1 = (data[ma1]<=data[ma2])&(prev_1>=prev_2)
        cross2 = (data[ma1]>=data[ma2])&(prev_1<=prev_2)
        
        data[ma1 + "X" + ma2] = cross1
        data.loc[:, [ma1 + "X" + ma2]] = data[ma1 + "X" + ma2].apply(
            lambda x:"Short")
        data[ma2 + "X" + ma1] = cross2
        
        cross_1_3 = (data[ma1]<=data[ma3])&(prev_1>=prev_3)
        cross_3_1 = (data[ma1]>=data[ma3])&(prev_1<=prev_3)
        
        data[ma1 + "X" + ma3] = cross_1_3
        data[ma3 + "X" + ma1] = cross_3_1
        
        cross_2_3 = (data[ma2]<=data[ma3])&(prev_2>=prev_3)
        cross_3_2 = (data[ma2]>=data[ma3])&(prev_2<=prev_3)
        
        data[ma2 + "X" + ma3] = cross_2_3
        data[ma3 + "X" + ma2] = cross_3_2
        
        return data
