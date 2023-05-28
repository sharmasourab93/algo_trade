from os import environ
from os.path import dirname, abspath

ROOT_DIR = environ.get("ROOT_DIR", None)
if ROOT_DIR is None:
    ROOT_DIR = dirname(dirname(dirname(abspath(__file__))))

NSE_CM_BHAVCOPY_COL_RENAMED = {
    "TckrSymb": "symbol",
    "SctySrs": "series",
    "OpnPric": "open",
    "HghPric": "high",
    "LwPric": "low",
    "ClsPric": "close",
    "TradDt": "timestamp",
    "PrvsClsgPric": "prev_close",
    "TtlTradgVol": "volume",
}

YF_UTILS_EXCEPTION_LIST = {
    "MOTHERSUMI.NS": "MOTHERSON",
    "RUCHI.NS": "PATANJALI",
    "MINDAIND.NS": "UNOMINDA",
}

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

TRADEABLE_INDICES = ('NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY')
TRADEABLE_INDICES_NAME = ('NIFTY 50', 'NIFTY BANK', 'NIFTY FIN SERVICE',
                          'NIFTY MID SELECT')

YFIN_INDEX_LIST = {"NIFTY": "^NSEI",
                   "NIFTY BANK": "^NSEBANK",
                   "NIFTY FIN SERVICE": "NIFTY_FIN_SERVICE.NS",
                   "NIFTY MID SELECT": "NIFTY_MID_SELECT.NS"
                   }
