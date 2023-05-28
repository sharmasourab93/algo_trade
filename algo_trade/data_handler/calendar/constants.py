from zoneinfo import ZoneInfo
from datetime import date, datetime
from pytz import timezone

# Time related Constants used along with Market Calendar Module.
TIME_ZONE = ZoneInfo("Asia/Kolkata")
TZ = timezone("Asia/Kolkata")
DATE = date.today()
TODAY = datetime.now(tz=TIME_ZONE).date()
TIME_OFFSET = "1800"
TIME_OFFSET_FMT = "%H%M"
TIME_CUTOFF = datetime.strptime(TIME_OFFSET, TIME_OFFSET_FMT).time()
DATE_FMT = "%d-%b-%Y"

# Fixed Market Start Time and End time for any market day.
MARKET_START = "0915"
MARKET_CLOSE = "1530"
MARKET_AMO = ("1540", "1600")
TIME_STRF = "%H%M"

# Market Start Time and Close Time.
MARKET_START_TIME = datetime.strptime(MARKET_START, TIME_STRF).time()
MARKET_CLOSE_TIME = datetime.strptime(MARKET_CLOSE, TIME_STRF).time()
MARKET_AMO_TIME = tuple(
    list(datetime.strptime(i, TIME_STRF).time() for i in MARKET_AMO)
)
