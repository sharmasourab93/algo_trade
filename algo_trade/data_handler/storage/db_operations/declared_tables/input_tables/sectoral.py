from sqlalchemy import Column, ForeignKey, String, ForeignKeyConstraint, Integer

from algo_trade.utils.db_utils.declared_tables.base import Base


class SectoralIndex(Base):
    __tablename__ = 'sectoralindex'
    
    id = Column("id", Integer, autoincrement=True, primary_key=True)
    company = Column("company", String(200))
    Industry = Column("Industry", String(200))
    Symbol = Column("Symbol", ForeignKey("mcap.Symbol"))
    Series = Column("Series", String(2))
    isin_code = Column("isin_code", String(200))
    sectoral_index = Column("sectoral_index", String(200))
    
    ForeignKeyConstraint(["Symbol"], ["mcap.Symbol"])
