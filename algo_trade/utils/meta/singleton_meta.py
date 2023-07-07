from os import getenv

from algo_trade.utils.logger import LogConfig

ENABLE_LOGGING = getenv('LOG_ON', True)
LOG_LEVEL = getenv('LOG_LEVEL', 'INFO')


class SingletonMeta(type):
    _instances = {}
    
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        
        return cls._instances[cls]
