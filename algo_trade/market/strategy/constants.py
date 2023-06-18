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
    "close",
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

PCR_VERDICT_RANGE = {(0, 0.4): "Over Sold",
                     (0.4, 0.6): "Very Bearish",
                     (0.6, 0.8): "Bearish",
                     (0.8, 1.0): "Mildly Bullish",
                     (1.0, 1.2): "Bullish",
                     (1.2, 1.5): "Very Bullish",
                     (1.5, float('inf')): "Over Bought"
                     }

OPTION_CHAIN_STRIKE_MULTIPLES = {
    (0, 200): 1,
    (200, 400): 2.5,
    (400, 500): 5,
}

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
SWING_BUYING_PULLBACKS = ["close"]
SWING_BUYING_PULLBACKS_VOLUME = 10000000
SWING_BUYINGPULLBACKS_PCT = (20, 30, 50)
SWING_BUYING_PULLBACK_COLUMNS = ["symbol", "close",
                                 "prev_close", "volume", "pct_change",
                                 "date"]
SWING_BUYING_PULLBACK_MIN_PCT = 3
STOCK_MODIFICATION = {"MOTHERSUMI": "MOTHERSON"}
