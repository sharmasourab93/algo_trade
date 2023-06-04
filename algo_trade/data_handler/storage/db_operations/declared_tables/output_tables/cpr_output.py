from sqlalchemy import Column, String, Float, Integer, ForeignKey, Date

from algo_trade.utils.db_utils.declared_tables.base import Base


class CPROutput(Base):
    __tablename__ = "cpr_output"
    
    id = Column("id", Integer, primary_key=True, autoincrement=True)
    Symbol = Column("Symbol", String(50), ForeignKey("mcap.Symbol"))
    Open = Column("Open", Float)
    High = Column("High", Float)
    Low = Column("Low", Float)
    Close = Column("Close", Float)
    Timestamp = Column("Timestamp", Date)
    Pct_Change = Column("Pct_Change", Float)
    con_range = Column("con_range", Integer)
    FNOLots = Column("FNOLots", Integer, default=0)
    Release = Column("Release", String(12))
    R3 = Column("R3", Float)
    R2 = Column("R2", Float)
    R1 = Column("R1", Float)
    TCPR = Column("TCPR", Float)
    Pivot = Column("Pivot", Float)
    BCPR = Column("BCPR", Float)
    S1 = Column("S1", Float)
    S2 = Column("S2", Float)
    S3 = Column("S3", Float)
    
    CPR_Width = Column("CPR_Width", Float)
    CPR = Column("CPR", String(20))
    Priority = Column("Priority", String(20))
    Frequency = Column("Frequency", String(10))
    Long_VCPR = Column("Long_VCPR", String(100), default="")
    Short_VCPR = Column("Short_VCPR", String(100), default="")
