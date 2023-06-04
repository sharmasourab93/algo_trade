from sqlalchemy import Column, String, Float, Integer, Date

from algo_trade.utils.db_utils.declared_tables.base import Base


class IndexReport(Base):
    __tablename__ = "index_cpr_report"
    
    id = Column("id", Integer, primary_key=True, autoincrement=True)
    Symbol = Column("Symbol", String(100))
    Index_Key = Column("Index_Key", String(50))
    Timestamp = Column("Timestamp", Date)
    Open = Column("Open", Float)
    High = Column("High", Float)
    Low = Column("Low", Float)
    Close = Column("Close", Float)
    Pct_Change = Column("Pct_Change", Float)
    Change = Column("Change", Float)
    YearlyHigh = Column("YearlyHigh", Float)
    YearlyLow = Column("YearlyLow", Float)
    Ad_dec = Column("Ad_dec", String(12))
    Prevclose = Column("Prevclose", Float)
    CPR_Width = Column("CPR_Width", Float)
    CPR = Column("CPR", String(20))
    R3 = Column("R3", Float)
    R2 = Column("R2", Float)
    R1 = Column("R1", Float)
    TCPR = Column("TCPR", Float)
    Pivot = Column("Pivot", Float)
    BCPR = Column("BCPR", Float)
    S1 = Column("S1", Float)
    S2 = Column("S2", Float)
    S3 = Column("S3", Float)
    
    DayHigherRange = Column("DayHigherRange", Float)
    DayLowerRange = Column("DayLowerRange", Float)
    WeekHigherRange = Column("WeekHigherRange", Float)
    WeekLowerRange = Column("WeekLowerRange", Float)
    MonthHigherRange = Column("MonthHigherRange", Float)
    MonthLowerRange = Column("MonthLowerRange", Float)
