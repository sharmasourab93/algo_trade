from inspect import getmembers, isclass

from sqlalchemy import create_engine, MetaData, inspect
from sqlalchemy.exc import (
    OperationalError,
    NoSuchModuleError,
    )
from sqlalchemy.orm import sessionmaker, scoped_session

from algo_trade.utils.db_utils import (
    InvalidSQLUrlException,
    UnavailableDatabaseException
    )
from algo_trade.utils.db_utils import declared_tables
from algo_trade.utils.db_utils.db_constants import (
    ECHO_POOL,
    MAX_OVERFLOW,
    POOL_PRE_PING,
    POOL_SIZE,
    POOL_RECYCLE
    )
from algo_trade.utils.db_utils.db_nse_data_mapper import NSEDataMapper
from algo_trade.utils.db_utils.db_nse_data_operations import NSEDataUpdater
from algo_trade.utils.db_utils.declared_tables import Base


class NSEDatabaseProcessor:
    """
    This class is being built to achive the following:
    1. A Database Schema designer for Stock Market Setup.
    2. Designed to auto setup default mappings, inputs and outputs
       on consuming a Valid DB Url and few optional parameters.
    3. Designed to switch and shift between various commercially available Databases.

    """
    
    DATABASE_NAME = "stockmarket"
    LOG_TABLE = "logtable"
    
    # @event.listens_for(mapper, 'init')
    # def auto_add(self, target, *args, **kwargs):
    #     self.session.add(target)
    
    def __init__(
            self,
            db_url: str,
            database: str,
            db_engine_echo: bool = False,
            create_db_if_not_exists: bool = False,
            *args,
            **kwargs
            ):
        super(NSEDatabaseProcessor, self).__init__(*args, **kwargs)
        self.engine = self.make_connection(db_url, database, db_engine_echo)
        self.session = scoped_session(sessionmaker(self.engine))
        
        if self.validation_process(create_db_if_not_exists) is False:
            # If Validation Process is True,
            # it means that the database is setup by default.
            self.data_mapper = NSEDataMapper(self.session)
            self.init_creation_process()
            self.data_mapper.create_initial_data_mappings()
        
        self.data_operator = NSEDataUpdater(self.session)
        self.data_operator.daily_bhavcopy_update()
        self.data_operator.create_fno_sec_ban()
    
    def get_metadata(self):
        return MetaData(self.engine)
    
    def init_creation_process(self):
        Base.metadata.create_all(self.engine)
    
    def validation_process(self, db_create_bool: bool) -> bool:
        # We validate if a certain table exists
        
        try:
            inspector = inspect(self.engine)
            
            if inspector.has_table(NSEDatabaseProcessor.LOG_TABLE) is False:
                return False
            
            return True
        
        except OperationalError as e:
            if db_create_bool is False:
                raise UnavailableDatabaseException(e)
        
        # TODO: Since creating a database poses different set of challenges,
        #  we shall be dealing with the creation of database in a different iteration.
    
    def make_connection(
            self,
            db_url: str,
            database: str,
            echo: bool,
            echo_pool: bool = ECHO_POOL,
            max_overflow: int = MAX_OVERFLOW,
            pool_pre_ping: bool = POOL_PRE_PING,
            pool_size: int = POOL_SIZE,
            pool_recycle: int = POOL_RECYCLE,
            ):
        """Method to create connection with the sql server.

        :param db_url: A valid SQL URL.

        :param database: A Valid Database name.

        :param echo: Boolean value

        :param echo_pool: (False) – if True,
        the connection pool will log informational output such as
        when connections are invalidated as well as when connections
        are recycled to the default log handler,
        which defaults to sys.stdout for output.
        If set to the string "debug", the logging will include pool checkouts
        and checkins. Direct control of logging is also available
        using the standard Python logging module.

        :param max_overflow: (10) – the number of connections to allow
        in connection pool “overflow”, that is connections that can be opened above
        and beyond the pool_size setting, which defaults to five.
        this is only used with QueuePool.

        :param pool_pre_ping – boolean, if True will enable the connection pool “pre-ping”
        feature that tests connections for liveness upon each checkout.

        :param pool_size=5 – the number of connections to keep open
        inside the connection pool.
        This used with QueuePool as well as SingletonThreadPool.
        With QueuePool, a pool_size setting of 0 indicates no limit;
        to disable pooling, set poolclass to NullPool instead.

        :param pool_recycle: (-1) - this setting causes the pool to recycle connections
        after the given number of seconds has passed.
        It defaults to -1, or no timeout.
        For example, setting to 3600 means connections will be recycled after one hour.
        Note that MySQL in particular will disconnect automatically
        if no activity is detected on a connection for eight hours
        (although this is configurable with the MySQLDB connection itself
        and the server configuration as well).

        """
        
        url = db_url + database
        
        try:
            engine = create_engine(
                url,
                echo=echo,
                echo_pool=echo_pool,
                max_overflow=max_overflow,
                pool_pre_ping=pool_pre_ping,
                pool_size=pool_size,
                pool_recycle=pool_recycle,
                )
        
        except NoSuchModuleError as e:
            raise InvalidSQLUrlException(e)
        
        return engine
    
    def get_all_table_names(self):
        tables = self.get_all_tables()
        
        tables = [j.__tablename__ for i, j in tables.items()]
        
        return tables
    
    def get_all_tables(self) -> dict:
        tables = dict(getmembers(declared_tables, isclass))
        
        return tables


if __name__ == "__main__":
    con_string = "postgresql://marshall:sourab@192.168.0.105:5432/"
    wcon_string = "post://marshall:sourab@192.168.0.101:5432/"
    wdb_name = "marketmarshall"
    db_name = "stockmarket"
    db_source = NSEDatabaseProcessor(con_string, db_name, True)
