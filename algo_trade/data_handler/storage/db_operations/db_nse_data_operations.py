from datetime import datetime

from pandas import to_datetime, DataFrame
from sqlalchemy.orm import scoped_session

from algo_trade.utils.constants import DATE_FMT
from algo_trade.utils.db_utils.db_constants import DAILY_BHAV_RENAME, DAILY_BHAV_COL_ORDER, INDEX_REPORT_RENAME, \
    CPR_REPORT_RENAME, INDEX_COL_ORDER, CPR_COL_ORDER, DB_INSERTION_EXCLUSIONS
from algo_trade.utils.db_utils.db_session_handler import SessionHandler
from algo_trade.utils.db_utils.declared_tables.input_tables import DailyBhavCopy, MarketCapList, FNOSecBan
from algo_trade.utils.db_utils.declared_tables.output_tables import CPROutput, IndexReport
from algo_trade.utils.nse_utils.nse_processor import NSEProcessor


#TODO: Add date validation to not overwrite to output reports or updated bhavcopy.
# TODO: Find a way to not make multiple api calls for the same data.

class NSEDataUpdater:
    
    def __init__(self, session: scoped_session, *args, **kwargs):
        self.session = session
        self.nse_processor = NSEProcessor(*args, **kwargs)
    
    def retrieve_data(self, table_name, *args, **kwargs):
        pass
    
    @SessionHandler.session_handled_insert_operation
    def create_fno_sec_ban(self):
        result = self.nse_processor.get_fno_secban_list()
        result = [self.session.query(MarketCapList).filter_by(Symbol=x).first() for x in result]
        result = [x.Symbol for x in result]
        dated = [datetime.strptime(self.nse_processor.next_bday, DATE_FMT).date()] * len(result)
        result = list(zip(result, dated))
        
        if not result:
            return list(), FNOSecBan.__tablename__
        
        result = [FNOSecBan(Securities=symbol, Dated=dated_) for symbol, dated_ in result]
        
        return result, FNOSecBan.__tablename__
    
    @SessionHandler.session_handled_insert_operation
    def daily_bhavcopy_update(self, nse_top: int = 1500, fno: bool = False, return_df: bool = False,
                              delete_file_downloads: bool = False, orient: str = 'records'):
        data = self.nse_processor.cm_data_to_processed_df(nse_top, fno, return_df, delete_file_downloads)
        
        # DF Renaming of columns and Ordering.
        data = data.rename(columns=DAILY_BHAV_RENAME)
        data = data[DAILY_BHAV_COL_ORDER]
        
        # Manipulating FNO_Lots.
        data["FNO_Lots"] = data.FNO_Lots.apply(lambda x:int(x) if x is not None else 0)
        
        # Date format
        data["Timestamp"] = to_datetime(data.Timestamp).apply(lambda x:x.date())
        
        # Symbol
        data = data.loc[~(data.Series != 'EQ'), :]
        data["Symbol"] = data.Symbol.apply(
            lambda x:self.session.query(MarketCapList).filter_by(Symbol=x).first())
        data["Symbol"] = data.Symbol.apply(lambda x:x.Symbol)
        
        result = [DailyBhavCopy(**i) for i in data.to_dict(orient=orient)]
        
        return result, DailyBhavCopy.__tablename__
    
    @SessionHandler.session_handled_insert_operation
    def insert_index_report(self, data: DataFrame, table: type = IndexReport, orient: str = 'records'):
        data = data.rename(columns=INDEX_REPORT_RENAME)
        data = data[INDEX_COL_ORDER]
        
        result = [table(**i) for i in data.to_dict(orient=orient)]
        
        return result, table.__tablename__
    
    @SessionHandler.session_handled_insert_operation
    def insert_cpr_report(self, data: DataFrame, table: type = CPROutput, orient: str = 'records'):
        data = data.rename(columns=CPR_REPORT_RENAME)
        
        if 'Long_VCPR' not in data.columns and 'Short_VCPR' not in data.columns:
            data[["Long_VCPR", "Short_VCPR"]] = str(), str()
        
        if 'Release' not in data.columns and 'con_range' not in data.columns:
            data[['Release', 'con_range']] = str(), 0
        data = data.loc[~data.Symbol.isin(DB_INSERTION_EXCLUSIONS), :]
        
        data = data[CPR_COL_ORDER]
        
        # Symbol Foreign Key association.
        data["Symbol"] = data.Symbol.apply(lambda x:self.session.query(MarketCapList).filter_by(Symbol=x).first())
        data["Symbol"] = data.Symbol.apply(lambda x:x.Symbol)
        
        result = [table(**i) for i in data.to_dict(orient=orient)]
        
        return result, table.__tablename__


if __name__ == '__main__':
    obj = NSEDataUpdater()
