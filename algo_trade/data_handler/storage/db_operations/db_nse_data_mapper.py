from datetime import datetime

import pandas as pd
from sqlalchemy.orm import scoped_session

from algo_trade.utils.constants import MCAP_PATH, DATE_FMT
from algo_trade.utils.db_utils.db_constants import MCAP_DF_RENAME, FNO_MTLOTS_FILE_PATH, NSE_CO_LIST_RENAME, \
    SECTOR_INDEX_RENAME
from algo_trade.utils.db_utils.db_session_handler import SessionHandler
from algo_trade.utils.db_utils.declared_tables import DefaultTable
from algo_trade.utils.db_utils.declared_tables.input_tables import MarketCapList, MarketHolidays, TradeIndex, \
    FNOSecBan, \
    StockFNOLots, IndexFNOLots, SectoralIndex, NSECOList
from algo_trade.utils.market_calendar import MarketHolidays as MarketCalendar
from algo_trade.utils.nse_utils import NSEProcessor


class NSEDataMapper(NSEProcessor):
    
    def __init__(self, session: scoped_session, *args, orient: str = 'records', **kwargs):
        super(NSEProcessor, self).__init__(*args, **kwargs)
        self.session = session
        self.orient = orient
    
    def prepare_init_mcap_data(self, file_reference: str = MCAP_PATH):
        mcap_data = pd.read_excel(MCAP_PATH)
        mcap_data = mcap_data.loc[:, ~mcap_data.columns.str.match(r"^Unnamed")]
        mcap_data.rename(columns=MCAP_DF_RENAME, inplace=True)
        mcap_data.loc[:, "Mcap"] = mcap_data.Mcap.apply(lambda x:x if type(x) == float else 0)
        mcap_data = mcap_data.loc[
                    (
                        (mcap_data.index.isin(mcap_data.index.values.tolist()[:-2]))
                    ),
                    :,
                    ]
        mcap_data.drop_duplicates("Symbol", inplace=True)
        mcap_data = mcap_data.drop(columns=["index"]).reset_index()
        mcap_data["index"] = mcap_data.index + 1
        
        return mcap_data
    
    @SessionHandler.session_handled_insert_operation
    def create_mcap_data(self, file_reference: str = MCAP_PATH, orient: str = 'records'):
        mcap_data = self.prepare_init_mcap_data()
        
        mapped_table = [MarketCapList(**i) for i in mcap_data.to_dict(orient=self.orient)]
        
        return mapped_table, MarketCapList.__tablename__
    
    @SessionHandler.session_handled_insert_operation
    def make_market_holidays_data(self):
        result = MarketCalendar().get_market_holidays_with_description()
        
        result = [MarketHolidays(**i) for i in result.to_dict(orient=self.orient)]
        
        return result, MarketHolidays.__tablename__
    
    def _tables(self):
        tables = [MarketCapList, MarketHolidays, TradeIndex, FNOSecBan, StockFNOLots, IndexFNOLots, SectoralIndex,
                  NSECOList]
        return tables
    
    @SessionHandler.session_handled_insert_operation
    def create_log(self):
        tables = self._tables()
        creation_time = datetime.now()
        # 1. Mcap Data creation
        result = [DefaultTable(table_name=i.__tablename__, created=True, created_on=creation_time) for i in tables]
        
        return result, DefaultTable.__tablename__
    
    @SessionHandler.session_handled_insert_operation
    def create_nse_co_list_data(self):
        data = self.update_nse_list(return_df=True)
        data["Lot Size"] = data["Lot Size"].fillna(0).apply(lambda x:int(x))
        data["Industry"] = data.Industry.fillna("-")
        data["Sectoral Index"] = data["Sectoral Index"].fillna("-")
        NSE_CO_LIST_RENAME.update({data.columns[3]:"mcap"})
        data = data.rename(columns=NSE_CO_LIST_RENAME)
        data.loc[:, "mcap"] = data.mcap.apply(lambda x:x if type(x) == float else 0)
        data = data.loc[data.index.isin(data.index.values.tolist()[:-3]), :]
        data["Symbol"] = data.Symbol.apply(
            lambda x:self.session.query(MarketCapList).filter_by(Symbol=x).first())
        data["Symbol"] = data.Symbol.apply(lambda x:x.Symbol)
        data = [NSECOList(**i) for i in data.to_dict(orient=self.orient)]
        
        return data, NSECOList.__tablename__
    
    @SessionHandler.session_handled_insert_operation
    def make_fno_stock_file(self):
        today = datetime.today().strftime(DATE_FMT)
        
        stock_fno, index_fno = self.make_index_fno_lots_file(path=FNO_MTLOTS_FILE_PATH,
                                                             return_df=True)
        stock_fno = stock_fno.rename(columns={"DerivativesonIndividualSecurities":"Underlying"})
        stock_fno["Symbol"] = stock_fno.Symbol.str.strip()
        stock_fno["Underlying"] = stock_fno.Underlying.str.strip()
        stock_fno = stock_fno.melt(["Underlying", "Symbol"],
                                   var_name='month',
                                   value_name="lotsize")
        stock_fno["lotsize"] = stock_fno.lotsize.apply(lambda x:x.strip()).apply(lambda x:int(x) if x != '' else 0)
        
        symbols = stock_fno.Symbol.apply(
            lambda x:self.session.query(MarketCapList).filter_by(Symbol=x).first())
        stock_fno["Symbol"] = symbols.apply(lambda x:x.Symbol)
        missing_symbol_df = stock_fno.loc[stock_fno.Symbol.isna(), :]
        missing_symbols = set(missing_symbol_df.Underlying.values.tolist())
        if len(missing_symbols) != 0:
            msg = "Missing Symbols from FNO Lots are: {0}".format(missing_symbols)
            raise MissingNSESymbolsException(msg)
        
        stock_fno = [StockFNOLots(**i) for i in stock_fno.to_dict(orient=self.orient)]
        
        return stock_fno, StockFNOLots.__tablename__
    
    @SessionHandler.session_handled_insert_operation
    def create_index_table_data(self, index_df: pd.DataFrame):
        return [TradeIndex(**i) for i in index_df.to_dict(orient=self.orient)], TradeIndex.__tablename__
    
    @SessionHandler.session_handled_insert_operation
    def create_index_lots_tables_data(self):
        _, index_lots = self.make_index_fno_lots_file(path=FNO_MTLOTS_FILE_PATH, return_df=True)
        index_lots.columns = index_lots.columns.str.strip()
        index_lots = index_lots.rename(columns={index_lots.columns[0]:index_lots.columns[0].capitalize(),
                                                index_lots.columns[1]:index_lots.columns[1].capitalize(), })
        trade_index = index_lots[["Underlying", "Symbol"]]
        
        self.create_index_table_data(trade_index)
        
        index_lots = index_lots.melt(["Underlying", "Symbol"],
                                     var_name='month',
                                     value_name="lotsize")
        index_lots["lotsize"] = index_lots.lotsize.apply(lambda x:x.strip()).apply(
            lambda x:int(x) if x != '' else 0)
        index_lots["Symbol"] = index_lots.Symbol.apply(
            lambda x:self.session.query(TradeIndex).filter_by(Symbol=x).first())
        index_lots["Symbol"] = index_lots.Symbol.apply(lambda x:x.Symbol)
        index_lots.drop(columns=["Underlying"], inplace=True)
        
        return [IndexFNOLots(**i) for i in index_lots.to_dict(orient=self.orient)], IndexFNOLots.__tablename__
    
    @SessionHandler.session_handled_insert_operation
    def create_sector_index(self, orient: str = 'records'):
        result_1 = self.get_nifty_sectoral_list_downloads(get_raw=True)
        result_1 = result_1.rename(columns=SECTOR_INDEX_RENAME)
        
        result_1 = [SectoralIndex(**i) for i in result_1.to_dict(orient=self.orient)]
        return result_1, SectoralIndex.__tablename__
    
    def create_initial_data_mappings(self):
        # 0. Log Table creates on top of every other table.
        # 1. Mcap Data is to be uploaded
        # 2. Underlying Symbol Reference
        # 3. Stock FNO Index
        # 4. Market Holidays data.
        # 5. NSE Co List.
        # 6. FNO Sec Ban
        
        self.create_log()
        self.create_mcap_data()
        self.make_market_holidays_data()
        self.make_fno_stock_file()
        self.create_nse_co_list_data()
        self.create_sector_index()
        self.create_index_lots_tables_data()
