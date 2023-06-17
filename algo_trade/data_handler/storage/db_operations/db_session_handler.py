from datetime import datetime
from functools import wraps

from algo_trade.utils.db_utils.declared_tables import DefaultTable


class SessionHandler:
    
    @staticmethod
    def session_update_logtable(operations):
        @wraps(operations)
        def handle_session(self, *args, **kwargs):
            result = operations(self, *args, **kwargs)
            #TODO: Perform updation of table name.
            # query = update(DefaultTable).filter_by(table_name=result).values({"updated_on":datetime.now()})
            query = self.session.query(DefaultTable).filter(DefaultTable.table_name == result).with_for_update().one()
            query.updated_on = datetime.now()
            self.session.add(query)
            self.session.commit()
        
        return handle_session
    
    @staticmethod
    def session_handled_insert_operation(operations):
        @wraps(operations)
        def handle_session(self, *args, **kwargs):
            result, table_name = operations(self, *args, **kwargs)
            
            if result:
                self.session.add_all(result)
                self.session.commit()
        
        return handle_session
