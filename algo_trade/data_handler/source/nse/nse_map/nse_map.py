import json
import os

from algo_trade.data_handler.source.nse.nse_map.nse_api import NSEApiMap
from algo_trade.data_handler.source.nse.nse_map.nse_downloads import \
    NSEDownloadMap

FILE_DIR = os.path.dirname(os.path.abspath(__file__))
NSE_JSON = os.path.join(FILE_DIR, "nse.json")


class NSEWebMap(NSEDownloadMap, NSEApiMap):
    """
    Class which manages the `nse.json`(which stores the NSE API Map) and
    which helps in operating/referencing objects from this class through
    tailor-made instance methods/properties.
    """

    def __init__(self, file_name: str = NSE_JSON):
        self.file_name = file_name
        self.loaded_dict: dict = self.load()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __getitem__(self, key: str):
        return self.loaded_dict[key]

    def save(self, mode: str = "wb"):
        with open(self.file_name, mode) as file:
            json.dump(self.loaded_dict, file)

    def load(self, mode: str = "rb") -> dict:
        with open(self.file_name, mode) as file:
            return json.load(file)

    @property
    def industry(self):
        return self.loaded_dict["INDUSTRY"]

    @property
    def nse_domain(self):
        return self.loaded_dict["Market"]["NSE"]["domain"]

    @property
    def nse_date_fmt(self):
        return self.loaded_dict["Market"]["NSE"]["DATE-FMT"]

    @property
    def nse_download_domain(self):
        return self.loaded_dict["Market"]["NSE"]["DOWNLOADS"]["domain"]

    @property
    def nse_headers_advanced(self):
        return self.loaded_dict["Market"]["NSE"]["headers"]["advanced"]

    @property
    def nse_headers_intermediate(self):
        return self.loaded_dict["Market"]["NSE"]["headers"]["intermediate"]

    @property
    def nse_headers_simple(self):
        return self.loaded_dict["Market"]["NSE"]["headers"]["simple"]


def nse_web_map() -> NSEWebMap:
    """
    Method which handles context management of the
    NSEWebMap Class which in
    turn return reference to the class itself.
    """

    with NSEWebMap() as file:
        return file
