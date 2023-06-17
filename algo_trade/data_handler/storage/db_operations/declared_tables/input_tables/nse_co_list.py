from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    ForeignKey,
    ForeignKeyConstraint
    )

from algo_trade.utils.db_utils.declared_tables.base import Base


class NSECOList(Base):
    __tablename__ = "nseco_list"
    # MarketCapList
    index = Column("index", Integer, nullable=False, primary_key=True)
    Symbol = Column(String(200), ForeignKey("mcap.Symbol", ondelete='cascade'))
    mcap = Column("mcap", Float)
    company = Column("company", String(200))
    fno = Column("fno", String)
    lotsize = Column("lotsize", Integer)
    industry = Column("industry", String)
    sector = Column("sector", String)
    
    ForeignKeyConstraint(["Symbol"], ["mcap.Symbol"])
