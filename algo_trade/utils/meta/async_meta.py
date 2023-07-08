import concurrent.futures
from os import getenv
import asyncio
import pandas as pd
from time import perf_counter
from functools import wraps
from contextlib import contextmanager

from algo_trade.utils.logger.log_configurator import LogConfig
from algo_trade.utils.telegram import TelegramBot

ENABLE_LOGGING = getenv('LOG_ON', 'True') == 'True'
ENABLE_TELEGRAM = not getenv('ENABLE_TELEGRAM', 'False') == 'False'
LOG_LEVEL = getenv('LOG_LEVEL', 'INFO')
MAKE_ASYNC = getenv('MAKE_ASYNC', 'False') == 'True'
TIME_COMP = getenv('TIME_COMP', 'True') == 'True'


def compute_execution_time(method):
    if not asyncio.iscoroutinefunction(method):
        @wraps(method)
        def sync_wrapper(self, *args, **kwargs):
            start = perf_counter()
            execution = method(self, *args, **kwargs)
            end = perf_counter()
            elapsed_time = round(end - start, 2)
            self.logger.info("Execution time for sync method {0}: {1}s".format(
                method.__name__, elapsed_time))
            
            return execution
        
        return sync_wrapper
    
    else:
        @wraps(method)
        async def async_wrapper(self, *args, **kwargs):
            
            start = perf_counter()
            result = await method(self, *args, **kwargs)
            end = perf_counter()
            elapsed_time = round(end - start, 2)
            self.logger.info("Execution time for async method {0}: {1}s".format(
                method.__name__, elapsed_time))
            return result
        
        return async_wrapper


def make_async(method):
    if not asyncio.iscoroutinefunction(method):
        
        return method
    
    else:
        def execute_method(self, *args, **kwargs):
            async def inner_async():
                result = await method(self, *args, **kwargs)
            
            return inner_async()
        
        return execute_method


class AsyncLoggingMeta(type):
    
    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)
    
    def __new__(mcs, name, bases, namespace):
        
        enable_async = bool(MAKE_ASYNC)
        enable_logging = bool(ENABLE_LOGGING)
        enable_time = bool(TIME_COMP)
        enable_telegram = bool(ENABLE_TELEGRAM)
        
        pd.set_option('chained_assignment', None)
        pd.set_option('copy_on_write', True)
        
        if enable_logging:
            LogConfig.setup_logging("", LOG_LEVEL)
        
        logger = LogConfig.get_logger(name)
        namespace["logger"] = logger
        
        for attr_name, attr_value, in namespace.items():
            
            if callable(attr_value) and not attr_name.startswith("__"):
                
                # if enable_async:
                #     # TODO: Find a way to make this method async here.
                #     attr_value = make_async(attr_value)
                
                if enable_time:
                    attr_value = compute_execution_time(attr_value)
                
                namespace[attr_name] = attr_value
        
        telegram_obj = TelegramBot(enable_telegram)
        namespace["telegram"] = telegram_obj
        
        return super().__new__(mcs, name, bases, namespace)
