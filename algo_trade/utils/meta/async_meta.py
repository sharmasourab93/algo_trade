from os import getenv
import asyncio
from time import perf_counter
from functools import wraps
from algo_trade.utils.logger.log_configurator import LogConfig
from contextlib import contextmanager

ENABLE_LOGGING = getenv('LOG_ON', True)
LOG_LEVEL = getenv('LOG_LEVEL', 'DEBUG')
MAKE_ASYNC = getenv('MAKE_ASYNC', True)
TIME_COMP = getenv('TIME_COMP', True)


def compute_execution_time(method):
    def execute(self, *args, **kwargs):
        start = perf_counter()
        execution = method(self, *args, **kwargs)
        end = perf_counter()
        elapsed_time = round(end - start, 2)
        self.logger.debug("Execution time for {0}: {1}s".format(
            method.__name__, elapsed_time))

        return execution

    return execute


def make_async(method):
    @wraps(method)
    async def execute(*args, **kwargs):
        return method(*args, **kwargs)

    return execute


class AsyncLoggingMeta(type):

    def __new__(mcs, name, bases, namespace):

        enable_async = bool(MAKE_ASYNC)
        enable_logging = bool(ENABLE_LOGGING)
        enable_time = bool(TIME_COMP)

        if enable_logging:
            LogConfig.setup_logging("", LOG_LEVEL)

        logger = LogConfig.get_logger(name)
        namespace["logger"] = logger

        for attr_name, attr_value, in namespace.items():

            if callable(attr_value) and not attr_name.startswith("__"):

                if enable_time:
                    attr_value = compute_execution_time(attr_value)

                if enable_async:
                    # TODO: Find a way to make this method async here.
                    attr_value = attr_value

                namespace[attr_name] = attr_value

        return super().__new__(mcs, name, bases, namespace)
