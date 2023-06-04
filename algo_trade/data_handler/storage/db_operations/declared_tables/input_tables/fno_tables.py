from sqlalchemy import Column, Integer, ForeignKey, String, Date, ForeignKeyConstraint

from algo_trade.utils.db_utils.declared_tables.base import Base


class StockFNOLots(Base):
    __tablename__ = "fnolots"
    id = Column(Integer, autoincrement=True, primary_key=True)
    Underlying = Column("Underlying", String(200))
    Symbol = Column(String(200), ForeignKey("mcap.Symbol"))
    # Month in %b-%y Format i.e., MAY-23, JUN-23 etc.
    month = Column("month", String(6))
    lotsize = Column("lotsize", Integer)
    
    ForeignKeyConstraint(["Symbol"], ["mcap.Symbol"])


class IndexFNOLots(Base):
    __tablename__ = "indexfnolots"
    
    id = Column(Integer, autoincrement=True, primary_key=True)
    Symbol = Column("Symbol", String(50), ForeignKey("tradeindex.Symbol"))
    month = Column("month", String(6))
    lotsize = Column("lotsize", Integer)
    
    ForeignKeyConstraint(["Symbol"], ["tradeindex.Symbol"])


class FNOSecBan(Base):
    __tablename__ = "fnosecban"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    Dated = Column("Dated", Date)
    Securities = Column("Securities", String(100), ForeignKey("mcap.Symbol"))
    
    ForeignKeyConstraint(["Securities"], ["mcap.Symbol"])
