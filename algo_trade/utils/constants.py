import os
from datetime import datetime, date
from os.path import dirname, abspath
from zoneinfo import ZoneInfo

from pytz import timezone

ROOT_DIR = os.environ.get("ROOT_DIR", None)
if ROOT_DIR is None:
    ROOT_DIR = dirname(dirname(abspath(__file__)))

UTILS = dirname(abspath(__file__))

LOGS_FOLDER_PATH = os.path.join(ROOT_DIR, "logs/")
if not os.path.exists(LOGS_FOLDER_PATH):
    os.mkdir(LOGS_FOLDER_PATH)

LOG_LEVEL = os.environ.get("LOG_LEVEL", None)
if LOG_LEVEL is None:
    LOG_LEVEL = 10

CHUNK_SIZE = 1024

# Constants Used for MarketHolidays
LEAVES = ("Market", "NSE", "API", "HOLIDAY", "TRADING")
LEAVES_DOMAIN = ("Market", "NSE", "domain")

# NIFTY INDICES LIST

NIFTY_INDICES_DOMAIN = "https://niftyindices.com/IndexConstituent/"

NIFTY_INDICES = (
    "NIFTY AUTO",
    "NIFTY BANK",
    "NIFTY COMMODITIES",
    "NIFTY CONSR DURBL",
    "NIFTY CONSUMPTION",
    "NIFTY ENERGY",
    "NIFTY FIN SERVICE",
    "NIFTY FMCG",
    "NIFTY HEALTHCARE",
    "NIFTY HOUSING",
    "NIFTY INFRA",
    "NIFTY IT",
    "NIFTY INDIA MANUFACTURING",
    "NIFTY MEDIA",
    "NIFTY METAL",
    "NIFTY MIDSML FINSRV",
    "NIFTY MIDSML IT AND TELECOM",
    "NIFTY MIDSML HEALTHCARE",
    "NIFTY OIL AND GAS",
    "NIFTY PHARMA",
    "NIFTY PSU BANK",
    "NIFTY PVT BANK",
    "NIFTY REALTY",
)

NIFTY_INDICES_CONSTITUENTS = (
    "ind_niftyautolist",
    "ind_niftybanklist",
    "ind_niftycommoditieslist",
    "ind_niftyconsumerdurableslist",
    "ind_nifty_consumptionlist",
    "ind_niftyenergylist",
    "ind_niftyfinancelist",
    "ind_niftyfmcglist",
    "ind_niftyhealthcarelist",
    "ind_niftyhousing_list",
    "ind_niftyinfralist",
    "ind_niftyitlist",
    "ind_niftyindiamanufacturing_list",
    "ind_niftymedialist",
    "ind_niftymetallist",
    "ind_niftymidsmallfinancialservice_list",
    "ind_niftymidsmallitAndtelecom_list",
    "ind_niftymidsmallhealthcare_list",
    "ind_niftyoilgaslist",
    "ind_niftypharmalist",
    "ind_niftypsubanklist",
    "ind_nifty_privatebanklist",
    "ind_niftyrealtylist",
)

NIFTY_INDICES_DOWNLOAD_MAP = dict(
    zip(NIFTY_INDICES, NIFTY_INDICES_CONSTITUENTS))

# YFUTILS Symbol Change Exception List

YF_UTILS_EXCEPTION_LIST = {
    "MOTHERSUMI.NS": "MOTHERSON",
    "RUCHI.NS": "PATANJALI",
    "MINDAIND.NS": "UNOMINDA",
    "LTI.NSE": "LTIM"
}
STOCK_MODIFICATION = {"MOTHERSUMI": "MOTHERSON"}

# All Indices related columns
ALL_INDICES_COLUMN_RENAME = {
    "last": "Close",
    "open": "Open",
    "high": "High",
    "low": "Low",
    "yearHigh": "52Wk High",
    "yearLow": "52Wk Low",
    "variation": "Change",
    "previousClose": "PrevClose",
    "percentChange": "% Change",
    "indexSymbol": "Symbol",
    "key": "Index Key",
}

ALL_INDICES_COLUMNS_ORDER = [
    "Symbol",
    "Index Key",
    "TimeStamp",
    "Open",
    "High",
    "Low",
    "Close",
    "% Change",
    "Change",
    "52Wk High",
    "52Wk Low",
    "Advance:Decline",
    "PrevClose",
    "CPR Width",
    "CPR",
    "R3",
    "R2",
    "R1",
    "TCPR",
    "Pivot",
    "BCPR",
    "S1",
    "S2",
    "S3",
]

ALL_INDICES_COLUMNS_INCLUDE_SYMBOL = [
    "NIFTY 50",
    "NIFTY NEXT 50",
    "INDIA VIX",
    "NIFTY BANK",
    "NIFTY AUTO",
    "NIFTY FIN SERVICE",
    "NIFTY FMCG",
    "NIFTY IT",
    "NIFTY METAL",
    "NIFTY MEDIA",
    "NIFTY PHARMA",
    "NIFTY REALTY",
    "NIFTY OIL AND GAS",
    "NIFTY CONSR DURBL",
    "NIFTY ENERGY",
    "NIFTY INFRA",
    "NIFTY HEALTHCARE",
    "NIFTY MIDCAP 150",
    "NIFTY SMLCAP 250",
    "NIFTY 100",
    "NIFTY 500",
]

# CPR Strategy Columns

CPR_STRATEGY_COLUMNS = [
    "Sno",
    "Symbol",
    "Open",
    "High",
    "Low",
    "Close",
    "Timestamp",
    "% Change",
    "Con. Range",
    "FNO Lots",
    "Release Date",
    "R3",
    "R2",
    "R1",
    "BCPR",
    "Pivot",
    "TCPR",
    "S1",
    "S2",
    "S3",
    "CPR Width",
    "CPR",
    "Priority Stocks",
]

NSE_FO_LIQUID_STOCKS = [
    "ASHOKLEY",
    "AXISBANK",
    "ABCAPITAL",
    "ABFRL",
    "ADANIPORTS",
    "APOLLOTYRE",
    "BHEL",
    "BANKBARODA",
    "BHARTIARTL",
    "BIOCON",
    "BPCL",
    "BSOFT",
    "CANBK",
    "COALINDIA",
    "CHAMBLFERT",
    "CHOLAFIN",
    "DABUR",
    "DELTACORP",
    "EXIDEIND",
    "FEDERALBNK",
    "FSL",
    "GAIL",
    "HINDALCO",
    "HDFCLIFE",
    "HINDCOPPER",
    "HINDPETRO",
    "ICICIBANK",
    "IOC",
    "ITC",
    "INDIACEM",
    "INDHOTEL",
    "IGL",
    "IBULHSGFIN",
    "INFY",
    "JSWSTEEL",
    "JUBLFOOD",
    "LICHSGFIN",
    "L&TFH",
    "M&MFIN",
    "M&M",
    "MOTHERSUMI",
    "MANAPPURAM",
    "NTPC",
    "NMDC",
    "POWERGRID",
    "PETRONET",
    "PFC",
    "PNB",
    "RAIN",
    "RECLTD",
    "SBIN",
    "SUNPHARMA",
    "SAIL",
    "TATAMOTORS",
    "TATASTEEL",
    "TATAPOWER",
    "TVSMOTORS",
    "UPL",
    "VEDL",
    "WIPRO",
    "ZEEL",
]

# Swing Trading Section
SWING_NSE_LIMIT = 1000
SWING_FNO_FILTER = False
SWING_EARNINGS_DELTA = 15
SWING_MIN_SHARE_PRICE = 300.0
SWING_MIN_PCT_CHANGE = 5.0

# Strategy 1 - BIGBANG
SWING_VOLUME_DIFF = 2.5
SWING_VOLUME_BIGBANG_ORDER = ["VolumeDiff"]

# Strategy 3 - Buying Pull backs
SWING_BUYING_PULLBACKS = [""]
