import os
import json
import pickle

FILE_DIR = os.path.dirname(os.path.abspath(__file__))
NSE_PKL_CACHE = os.path.join(FILE_DIR, "nse_cache.pkl")
INDICES_MAP = os.path.join(FILE_DIR, "index.json")
NIFTY_INDICES_DOMAIN = "https://niftyindices.com/IndexConstituent/"


class NSEDataCacheManager:
    """
    NSEDataCacheManager is the pickle file manager for
    all the initial cached data required for any analysis or
    stock/index quote or reference.
    """

    def __init__(self, file_name: str = NSE_PKL_CACHE):
        self.file_name = file_name
        self.loaded_dict: dict = self.load()

    def save(self, mode: str = 'wb'):
        with open(self.file_name, mode) as file:
            pickle.dump(self.loaded_dict, file)

    def load(self, mode: str = 'rb') -> dict:
        with open(self.file_name, mode) as file:
            return pickle.load(file)

    def __enter__(self):
        return self

    def __getitem__(self, key: str):
        return self.loaded_dict[key]

    def __setitem__(self, key, value):
        self.loaded_dict[key] = value
        self.save()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.save()

    def __delitem__(self, key):
        del self.loaded_dict[key]
        self.save()

    @property
    def mcap(self):
        return self.loaded_dict["mcap"]

    @property
    def fno_data(self):
        return self.loaded_dict["fo_data"]

    @property
    def indices(self):
        if 'indices' in self.loaded_dict.keys():
            return self.loaded_dict['indices']

        self.loaded_dict['indices'] = list(
            NSEDataCacheManager.index_map().keys())

        return self.loaded_dict

    @property
    def index_lots(self):
        return self.loaded_dict["index_lots"]

    @property
    def nse_list(self):
        return self.loaded_dict["nse"]

    @property
    def sectoral_data(self):
        return self.loaded_dict["sectoral"]

    @staticmethod
    def index_map(file_name: str = INDICES_MAP, mode: str = 'rb'):
        with open(file_name, mode) as json_file:
            return json.load(json_file)

    @property
    def market_holidays(self):
        return self.loaded_dict['market_holidays']


def nsedata_cache() -> NSEDataCacheManager:
    """
    Method which handles context management of the
    NSEDataCacheManager Class which in
    turn returns reference to the class itself.
    """

    with NSEDataCacheManager() as file:
        return file
