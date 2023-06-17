from sqlalchemy import Column, Integer, String, Boolean, DateTime

from algo_trade.utils.db_utils.declared_tables.base import Base


class DefaultTable(Base):
    __tablename__ = "logtable"
    
    id = Column(Integer, autoincrement=True, primary_key=True)
    table_name = Column(String(50), nullable=False)
    created = Column(Boolean, default=False)
    updated_on = Column(DateTime)
    created_on = Column(DateTime, nullable=False)
    
    def __repr__(self):
        return (
            "<{0}(table_name={1}, created={2}, "
            "created_on={3}, last_updated={4}>".format(
                self.table_name, self.created, self.created_on, self.updated_on
                )
        )
