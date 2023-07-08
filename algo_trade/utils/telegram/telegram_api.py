from algo_trade.utils.meta.singleton_meta import SingletonMeta
from os import getenv
from pandas import DataFrame
from functools import cache, wraps
import asyncio
import telegram
from datetime import datetime
import time
from typing import Union, Dict, List
from re import sub, compile
from tabulate import tabulate

MARKDOWN_SEQUENCE = r'(\.\\\+\*\?\[\^\]\$\(\)\{\}\!\<\>\|\:\-)'


def textualize_data(telegram_method):
    def execute_method(self, data: Union[DataFrame, str], *,
                       additional_text: str = None,
                       order: str = "Top",
                       cols: list = None,
                       index: bool = False,
                       tablefmt: str = 'rst',
                       stralign: str = 'center',
                       date_format: str = "%d-%b-%Y %H:%M"):
        
        if isinstance(data, DataFrame):
            if cols is None:
                cols = data.columns.tolist()
            
            data = tabulate(data[cols], headers='keys', showindex=index, tablefmt=tablefmt, stralign=stralign)
            
            if additional_text is not None:
                
                additional_text = "**" + additional_text + "**"
                
                if order.capitalize() == "Top":
                    data = additional_text + "\n" + data
                
                else:
                    data += "\n" + additional_text
        
        telegram_method(self, data, date_format)
    
    return execute_method


class TelegramBot(metaclass=SingletonMeta):
    """
    TelegramBot method is built on the Singleton Pattern
    wherein at any given point we do not replicate creation of
    TelegramBot objects. This helps in using the same object
    over and over again in an optimized manner.
    """
    
    def __init__(self, telegram_bot_enabled: bool = False):
        self.telegram_enabled = telegram_bot_enabled
        if self.telegram_enabled:
            
            self.telegram_token = getenv('TELEGRAM_TOKEN', None)
            
            if self.telegram_token is None:
                logger_msg = "TELEGRAM_TOKEN Key not set."
                raise KeyError(logger_msg)
            
            self.bot = telegram.Bot(self.telegram_token)
    
    @cache
    async def get_chat_id(self) -> int:
        
        """
        Method to get the chat_id of the group to which
        both has to send message to.
        This method is cached in order to reuse the exisiting chat box
        since we are only dealing with posting messages to
        one Telegram group.
        """
        
        bot_updates = await self.bot.get_updates()
        chat_id = bot_updates[1].effective_chat
        return chat_id.id
    
    @textualize_data
    def send_message(self, text: str, date_format: str = "%d-%b-%Y %H:%M"):
        """
        If the Telegram_enabled bool is True, we send a message
        else do nothing.
        
        :param text:
        :param date_format:
        
        :return:
        """
        
        if self.telegram_enabled:
            chat_id = asyncio.run(self.get_chat_id())
            text += "\n Generated on {0}." \
                    "\n~TradeYogi".format(datetime.now().strftime(date_format))
            asyncio.run(self.bot.send_message(text=text, chat_id=chat_id))
