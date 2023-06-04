from sqlalchemy import Column, String, Integer, Float, Date, ForeignKey, ForeignKeyConstraint

from algo_trade.utils.db_utils.declared_tables.base import Base


class DailyBhavCopy(Base):
    __tablename__ = 'dailybhav'
    
    id = Column(Integer, autoincrement=True, primary_key=True)
    isin = Column("isin", String(50))
    Symbol = Column("Symbol", String(50), ForeignKey("mcap.Symbol"))
    Series = Column("Series", String(4))
    Open = Column("Open", Float)
    High = Column("High", Float)
    Low = Column("Low", Float)
    Close = Column("Close", Float)
    Prevclose = Column("Prevclose", Float)
    Pct_Change = Column("Pct_Change", Float)
    Volume = Column("Volume", Integer)
    Timestamp = Column("Timestamp", Date)
    FNO_Lots = Column("FNO_Lots", Integer)
    ReleaseDate = Column("ReleaseDate", String(20), default="-")
    Priority = Column("Prority", String(10), default="-")
    
    ForeignKeyConstraint(["Symbol"], ["mcap.Symbol"])
