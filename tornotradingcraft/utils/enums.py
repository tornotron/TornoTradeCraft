from enum import Enum

class YahooPeroid(Enum):
    ONE_DAY = "1d"
    FIVE_DAY = "5d"
    ONE_MON = "1mo"
    THREE_MON = "3mo"
    SIX_MON = "6mo"
    ONE_YEAR = "1yr"
    TWO_YEAR = "2yr"
    FIVE_YEAR = "5yr"
    TEN_YEAR = "10yr"
    YTD = "ytd"
    MAX = "max"


class YahooInterval(Enum):
    ONE_MIN = "1m"
    TWO_MIN = "2m"
    FIVE_MIN = "5m"
    FIFTEEN_MIN = "15m"
    THIRTY_MIN = "30m"
    SIXTY_MIN = "60m"
    NINTEY_MIN = "90m"
    ONE_HOUR = "1h"
    ONE_DAY = "1d"
    FIVE_DAY = "5d"
    ONE_WEEK = "1wk"
    ONE_MONTH = "1mo"
    THREE_MONTH = "3mo"
