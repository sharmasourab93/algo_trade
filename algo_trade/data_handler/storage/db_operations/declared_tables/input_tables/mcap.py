from sqlalchemy import Integer, Column, String, Float

from algo_trade.utils.db_utils.declared_tables.base import Base


class MarketCapList(Base):
    __tablename__ = "mcap"
    
    index = Column("index", Integer, primary_key=True)
    Symbol = Column("Symbol", String, unique=True, nullable=False)
    Company = Column("Company", String(200), nullable=False)
    Mcap = Column("Mcap", Float)


class TradeIndex(Base):
    __tablename__ = "tradeindex"
    
    index = Column("index", Integer, autoincrement=True, primary_key=True)
    Symbol = Column("Symbol", String(10), unique=True)
    Underlying = Column("Underlying", String(50))
