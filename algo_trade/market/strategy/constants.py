CPR_STRATEGY_COLUMNS = [
    "index",
    "symbol",
    "lotsize",
    "industry",
    "sectoral_index",
    "open",
    "high",
    "low",
    "close",
    "pct_change",
    "volume",
    "cpr_width",
    "cpr",
    "con_range",
    "purpose",
    "priority_stocks",
    "timestamp"
]

# All Indices related columns
ALL_INDICES_COLUMN_RENAME = {
    "last": "close",
    "open": "open",
    "high": "high",
    "low": "low",
    "yearHigh": "52wk_high",
    "yearLow": "52wk_low",
    "variation": "change",
    "previousClose": "prev_close",
    "percentChange": "pct_change",
    "indexSymbol": "symbol"
}

ALL_INDICES_COLUMNS_ORDER = [
    "symbol",
    "key",
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "pct_change",
    "change",
    "52wk_high",
    "52wk_low",
    "Advance:Decline",
    "prev_close",
    "cpr_width",
    "cpr",
    "r3",
    "r2",
    "r1",
    "tcpr",
    "pivotpt",
    "bcpr",
    "s1",
    "s2",
    "s3",
]

ALL_INDICES_COLUMNS_INCLUDE_SYMBOL = [
    "NIFTY 50",
    "NIFTY BANK",
    "NIFTY FIN SERVICE",
    # "NIFTY MIDCAP 150",
    "INDIA VIX"
]

SELECT_COLUMNS_FOR_INDEX_REPORT = [
    "symbol",
    "timestamp",
    "pct_change",
    "cpr_width",
    "cpr",
    "day_higher_range",
    "day_lower_range",
    "weekly_higher_range",
    "weekly_lower_range",
    "monthly_higher_range",
    "monthly_lower_range"
]

# Swing Trading Section
SWING_NSE_LIMIT = 1000
SWING_FNO_FILTER = False
SWING_EARNINGS_DELTA = 15
SWING_MIN_SHARE_PRICE = 300.0
SWING_MIN_PCT_CHANGE = 5.0

# Strategy 1 - BIGBANG
SWING_VOLUME_DIFF = 10
SWING_VOLUME_BIGBANG_ORDER = ["volume_diff"]

# Strategy 3 - Buying Pull backs
SWING_BUYING_PULLBACKS = [""]

STOCK_MODIFICATION = {"MOTHERSUMI": "MOTHERSON"}
