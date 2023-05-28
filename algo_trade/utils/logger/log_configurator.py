import os
import os.path as path
from os import getenv
from os.path import exists, join, abspath, dirname
import logging
from logging import config as log_conf
import warnings
from datetime import date
from zoneinfo import ZoneInfo
from datetime import datetime
from pytz import timezone
from algo_trade.utils.constants import UTILS, LOGS_FOLDER_PATH
from algo_trade.utils.logger.logging import LOG_MAP


TIME_ZONE = ZoneInfo("Asia/Kolkata")
TZ = timezone("Asia/Kolkata")
DATE = date.today()
TODAY = datetime.now(tz=TIME_ZONE).date()


DATE_FMT = "%Y%m%d"
IST = TIME_ZONE
CONF_PATH = r"src/config/"

LOG_PATH = LOGS_FOLDER_PATH
LOG_CONFIG_FILE = os.path.join(UTILS, "logging.yml")


class LogConfig:
    """
    LogConfig Module to Configure logger.
    This class enables you to log by allowing you dynamically provide
    1. Optionally providing a country name to set logs specific to region.
    2. level of logging
    3. Config path.
    4. Optionally setting log_path.
    5. Disabling writing to log
    6. Disabling writing to console.
    7. Lets you set environment variable LOG_CFG to pick config file from.

    In Logging `level` can be set to the different value as set below:

            Level        |  Numeric Value
       ------------------|-----------------
            NOTSET       |    0
            DEBUG        |    10
            INFO         |    20
            WARNING      |    30
            ERROR        |    40
            CRITICAL     |    50

    Based on the value set to level, logging will let you log
    logs for level greater than or equal to in
    ascending order of the numeric value.

    Methods in logger are as follows:

    S.No   Loggeer method       Description
    --------------------------------------------------------------
    1.      Logger.info(msg)    log with level INFO on this logger.
    2.      logger.warning(msg) Log with level WARNING
    3.      logger.error(msg)   Log with level ERROR
    4.      logger.critical(msg)
    5.      logger.log(lvl, msg)
    6.      logger.exception(msg, exc_info=True)
    7.      logger.debug(msg)

    Based on the default config,
    you can set to_log=False to not log to a log file.
    Based on default config,
    you can set to_console=False to disable STDOUT.

    Ways to call this method are as follows:
    Example 1:
    />>>from src.main.utilities.logging import LogConfig
    # Imports the LogConfig Module.
    />>>LogConfig.setup_logging("zambia", 10)
    # Setting up logging.
    />>>logger = LogConfig.get_logger(__name__)
    # Calling logging into logger.
    # The setting enabled does this following:
    #   1. config_path will be default
    #   2. level = 10 // debug level set to debug.
    #   3. country = "zambia"
    #   // Based on path validation will write a
    #   // log file to /src/main/logs/zambia/zambia_<timestamp>.log
    #   4. Since to_log is not set to false and
    #      to_console is not set to false
    #   Logs will be on STDOUT/console and
    #   on the log file zambia_<timestamp>.log
    />>>logger.info("INFO Logged")
    # "[INFO]: Timestamp: Module __main__: INFO Logged"
    # Similarly for other logger methods.

    """

    @staticmethod
    def setup_logging(
        country="",
        level=logging.INFO,
        *,
        config_path=LOG_CONFIG_FILE,
        log_path=None,
        to_log=True,
        to_console=True,
        env_key="LOG_CFG"
    ):
        """
         The method `setup_logging` will try to configure
         the logging in this flow:

         1. Check if LOG_CFG is set in the environment:
            - LOG_CFG is expected to be a file path in *.yml format.
            - The Configuration will be read from that path.

         2. If LOG_CFG is not set,
            default `config_path` will be configured.
            - the default config_path i.e.
               LOG_CONFIG_FILE = "src/main/conf/logging.yml"

         3. By default log_path is set to None and assigned to
            - If Country Value is not "" and
              If the country value's path exists:
                - src/main/logs/{country}_YYYYMMDD.log
            - Else:
              - src/main/logs/log_YYYYMMDD.log

         4. Config file is read
            - Filename is set to file_handler
            - Log level is set to root based on level value.
            - If to_log value is set to False
                - file_handler is removed from the debug_options.

         5. Logger config is set.


        :param country: ""(default) // Assign Values based on valid path.
        :param level: logging.INFO
        :param config_path: set to src/main/conf/logging.yml (Default)
        :param log_path: None (By Default) // Assign valid path for others.
        :param to_log: True(Default) Set false to disable logging to file.
        :param to_console: True, Set false to turn off stdout.
        :param env_key: LOG_CFG(Default) // Environment Variable LOG_CFG
        """

        value, config = getenv(env_key, None), None
        time = datetime.now(tz=IST).strftime(DATE_FMT)

        if value and exists(value):
            config_path = value

        if not log_path or not exists(abspath(log_path)):

            # logging.debug("Log file path not found. "
            #               "Setting to default path.")

            if exists(join(abspath(LOG_PATH), country.lower())) and country != "":
                log_path = join(
                    abspath(LOG_PATH), country, "{0}_{1}.log".format(country, time)
                )
            else:
                string = "log_{0}.log".format(time)
                log_path = join(abspath(LOG_PATH), string)

        # logging.debug("Log configuration file found: " + config_path)

        # with open(config_path, 'rt') as f:
        #     config = yaml.safe_load(f.read())
        config = LOG_MAP

        config["handlers"]["file_handler"]["filename"] = log_path
        config["root"]["level"] = level

        if not to_log:
            config["root"]["handlers"].pop(-1)

        if not to_console:
            config["root"]["handlers"].pop(0)

        if not (to_log and to_console):
            config["root"]["handlers"].clear()

        log_conf.dictConfig(config)

    @staticmethod
    def get_logger(name):
        """Method to seek Logger.
        Called after setup_logging.
        """
        return logging.getLogger(name)
