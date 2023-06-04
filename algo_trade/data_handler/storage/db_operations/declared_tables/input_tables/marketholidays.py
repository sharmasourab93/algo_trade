from sqlalchemy import Column, String, Date, Integer

from algo_trade.utils.db_utils.declared_tables.base import Base


class MarketHolidays(Base):
    __tablename__ = "marketholidays"
    id = Column(Integer, autoincrement=True, primary_key=True)
    Holiday = Column("Holiday", String(50))
    Date = Column("Date", Date, unique=True)
    Day = Column("Day", String(10))
