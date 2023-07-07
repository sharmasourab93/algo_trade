from algo_trade.utils.meta.singleton_meta import SingletonMeta
from os import getenv
from functools import cache, wraps
import asyncio
import telegram
from datetime import datetime
import time


class TelegramBot(metaclass=SingletonMeta):
    
    def __init__(self, telegram_bot_enabled: bool = False):
        self.telegram_enabled = telegram_bot_enabled
        if self.telegram_enabled:
            
            self.telegram_token = getenv('TELEGRAM_TOKEN', None)
            
            if self.telegram_token is None:
                logger_msg = "TELEGRAM_TOKEN Key not set."
                # self.logger.info(logger_msg)
                raise KeyError(logger_msg)
            
            self.bot = telegram.Bot(self.telegram_token)
    
    @cache
    async def get_chat_id(self) -> int:
        
        bot_updates = await self.bot.get_updates()
        chat_id = bot_updates[1].effective_chat
        return chat_id.id
    
    def send_message(self, text: str, date_format: str = "%d-%b-%Y %H:%M"):
        
        chat_id = asyncio.run(self.get_chat_id())
        text += "\n Generated on {0}." \
                "\n~TradeYogi".format(datetime.now().strftime(date_format))
        asyncio.run(self.bot.send_message(text=text, chat_id=chat_id))
