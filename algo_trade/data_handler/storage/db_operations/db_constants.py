DAILY_BHAV_RENAME = {"Priority Stocks":"Priority",
                     "Release Date":"ReleaseDate",
                     "FNO Lots":"FNO_Lots",
                     "% Change":"Pct_Change",
                     "Isin":"isin"}
DAILY_BHAV_COL_ORDER = ["isin", "Symbol", "Series", "Open", "High", "Low", "Close",
                        "Prevclose", "Volume", "Timestamp", "Pct_Change", "FNO_Lots", "ReleaseDate", "Priority"]
MCAP_DF_RENAME = {"Sr. No.":"index",
                  "Sr No":"index",
                  "Company Name":"Company",
                  'Market capitalization as on March 31, 2023\n(Rs in Lakhs)':"Mcap",
                  'Market capitalization as on March 31, 2022\n(Rs in Lakhs)':"Mcap"}
SECTOR_INDEX_RENAME = {"Company Name":"company",
                       "ISIN Code":"isin_code",
                       "Sectoral Index":"sectoral_index"}
NSE_CO_LIST_RENAME = {"Sr. No.":"index",
                      "Lot Size":"lotsize",
                      "Company Name":"company",
                      "Industry":"industry",
                      "Sectoral Index":"sector",
                      "FNO":"fno"}

INDEX_REPORT_RENAME = {"Index Key":"Index_Key",
                       "TimeStamp":"Timestamp",
                       "% Change":"Pct_Change",
                       "52Wk High":"YearlyHigh",
                       "52Wk Low":"YearlyLow",
                       "Advance:Decline":"Ad_dec",
                       "PrevClose":"Prevclose",
                       "CPR Width":"CPR_Width",
                       "Higher Range":"DayHigherRange",
                       "Lower Range":"DayLowerRange",
                       "Weekly Higher Range":"WeekHigherRange",
                       "Weekly Lower Range":"WeekLowerRange",
                       "Monthly Higher Range":"MonthHigherRange",
                       "Monthly Lower Range":"MonthLowerRange"}
INDEX_COL_ORDER = ["Symbol", "Index_Key",
                   "Timestamp", "Open",
                   "High", "Low",
                   "Close", "Pct_Change",
                   "Change", "YearlyHigh",
                   "YearlyLow", "Ad_dec",
                   "Prevclose", "CPR_Width",
                   "CPR", "R3",
                   "R2", "R1",
                   "TCPR", "Pivot",
                   "BCPR", "S1",
                   "S2", "S3",
                   "DayHigherRange", "DayLowerRange",
                   "WeekHigherRange", "WeekLowerRange",
                   "MonthHigherRange", "MonthLowerRange"]

CPR_REPORT_RENAME = {"% Change":"Pct_Change",
                     "Con. Range":"con_range",
                     "FNO Lots":"FNOLots",
                     "CPR Width":"CPR_Width",
                     "Priority Stocks":"Priority",
                     "Long VCPR":"Long_VCPR",
                     "Short VCPR":"Short_VCPR",
                     "Release Date":"Release"}

CPR_COL_ORDER = ["Symbol", "Timestamp",
                 "Open", "High",
                 "Low", "Close",
                 "Pct_Change", "con_range",
                 "FNOLots", "Release",
                 "R3", "R2",
                 "R1", "TCPR",
                 "Pivot", "BCPR",
                 "S1", "S2",
                 "S3", "CPR_Width",
                 "CPR", "Priority",
                 "Long_VCPR", "Short_VCPR"]
DB_INSERTION_EXCLUSIONS = ["LTI", "MINDTREE",
                           "MOTHERSUMI", "SRTRANSFIN",
                           "RUCHI", "MINDAIND"]
FNO_MTLOTS_FILE_PATH = r"data/input/downloaded/fo/fo_mktlots.csv"

# Constants with respect to create_engine connection.
ECHO_POOL = False
MAX_OVERFLOW = 10
POOL_PRE_PING = False
POOL_SIZE = 5
POOL_RECYCLE = -1
