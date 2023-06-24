# from src.indicators.indicators import Indicators
import pandas as pd
import numpy as np


class PivotPoints:
    def cpr_calculator(
            self, pivot: float, tcpr: float, bcpr: float, round_to: int = 2
            ) -> float:
        """
        Simple CPR Calculator
        :param pivot: Pivot Point
        :param tcpr: Top CPR
        :param bcpr: Bottom CPR
        :param round_to: int 2 (default)
        :return: CPR Condition using the arguments passed.
        """
        
        return round((abs(tcpr - bcpr) / pivot) * 100, round_to)
    
    def pivot_support_calculator(
            self, pivot: float, high: float, low: float, close: float,
            round_to: int = 2
            ) -> tuple:
        """
        Pivot Support Calculator
        :param pivot: Pivot Points
        :param high: High
        :param low: Low
        :param close: Close
        :param round_to: int 2 (default)
        :return:
        """
        
        s1 = round((pivot * 2) - high, round_to)
        s2 = round(pivot - (high - low), round_to)
        s3 = round(low - (2 * (high - pivot)), round_to)
        
        return s1, s2, s3
    
    def pivot_resistance_calculator(
            self, pivot: float, high: float, low: float, close: float,
            round_to: int = 2
            ) -> tuple:
        """
        Pivot Resistance Calculator
        :param pivot: float (Pivot Point)
        :param high: High
        :param low: low
        :param close: close
        :param round_to: 2 int (default)
        :return: Resistance 3, 2, 1, in that order
        """
        r1 = (pivot * 2) - low
        r2 = pivot + (high - low)
        r3 = high + (2 * (pivot - low))
        
        return r3, r2, r1
    
    def pivot_points_calculator(
            self, open: float, high: float, low: float, close: float,
            round_to: int = 2
            ) -> tuple:
        """
        Simple Pivot Pivot point calculator
        :param open:
        :param high:
        :param low:
        :param close:
        :param round_to:
        :return:
        """
        
        pivot = round(sum([high, low, close]) / 3, round_to)
        bcpr = round(sum([high, low]) / 2, round_to)
        tcpr = round((pivot - bcpr) + pivot, round_to)
        
        values = [pivot, bcpr, tcpr]
        
        return values
    
    def plot_central_pivots(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Method to Plot Pivot, Bottom CPR, Top CPR,
        Calculation of CPR_Width.
        :param data:
        :return:
        """
        data["pivotpt"] = (data.high + data.low + data.close) / 3
        data["bcpr"] = (data.high + data.low) / 2
        data["tcpr"] = (data.pivotpt - data.bcpr) + data.pivotpt
        
        return data
    
    def plot_support_resistance(self, data: pd.DataFrame):
        """
        Method to add Support & Resistance
        :param data:
            Dataframe composing of PIVOT, TCPR & BCPR,
            which also implies that Data should contain
            Open, High, Low, Close.
        :return: Dataframe having Supports S1, S2, S3, S4 &
                 Resistances R1, R2, R3, R4
        """
        
        # Building Resisitance Levels
        data["r1"] = (data.pivotpt * 2) - data.low
        data["r2"] = data.pivotpt + (data.high - data.low)
        data["r3"] = data.high + (2 * (data.pivotpt - data.low))
        
        # Support Levels
        data["s1"] = (data.pivotpt * 2) - data.high
        data["s2"] = data.pivotpt - (data.high - data.low)
        data["s3"] = data.low - (2 * (data.high - data.pivotpt))
        
        return data
    
    def calculate_x_factor(self, price: float) -> int:
        
        for j in range(1, 8):
            
            if price<(2500 * (2**(j - 1))):
                return j - 1
    
    def plot_cpr_width(self, data: pd.DataFrame, classify_cpr: bool = True):
        """
        Method to Calculate CPR & Classify kinds of CPR
        """
        
        x_factor = data.close.apply(self.calculate_x_factor)
        
        data["cpr_width"] = round(
            (abs(data.tcpr - data.bcpr) / data.pivotpt) * 100, 2)
        
        data["cpr_width"] = data.cpr_width * x_factor
        
        if classify_cpr:
            condition = [
                (data.cpr_width<=0.25),
                ((data.cpr_width>0.25)&(data.cpr_width<=0.5)),
                ((data.cpr_width>0.5)&(data.cpr_width<=0.75)),
                ((data.cpr_width>0.75)&(data.cpr_width<=0.9)),
                (data.cpr_width>0.9)
                ]
            cprs = ["Narrow CPR", "Compact CPR", "Mid CPR", "Wide CPR",
                    "Very Wide CPR"]
            
            data["cpr"] = np.select(condition, cprs)
        
        return data
    
    def plot_pivots_with_cpr(self, data):
        """Wrapping Plotting of Pivots, Supports, Resistances, CPR width
        & CPR Classification
        """
        
        data = self.plot_central_pivots(data)
        data = self.plot_support_resistance(data)
        data = self.plot_cpr_width(data)
        
        return data
