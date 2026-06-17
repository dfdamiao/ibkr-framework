"""
Order type constants and algorithm parameters.

Reference: the IBKR TWS API documentation Sections 4-5, Appendix B
"""

# --- Order Actions ---
BUY = "BUY"
SELL = "SELL"

# --- Security Types ---
STK = "STK"
OPT = "OPT"
FUT = "FUT"
CASH = "CASH"

# --- Order Types ---
MKT = "MKT"
LMT = "LMT"
STP = "STP"
STP_LMT = "STP LMT"
REL = "REL"
MIDPRICE = "MIDPRICE"  # NOT used — fails outside RTH (Error 321)
TRAIL = "TRAIL"
TRAIL_LIMIT = "TRAIL LIMIT"
MOC = "MOC"

# --- Time in Force ---
DAY = "DAY"
GTC = "GTC"
IOC = "IOC"

# --- Adaptive Algorithm Priorities ---
PATIENT = "Patient"
NORMAL = "Normal"
URGENT = "Urgent"

# --- Market Data Types ---
LIVE = 1
FROZEN = 2
DELAYED = 3
DELAYED_FROZEN = 4

# --- Trigger Methods ---
TRIGGER_DEFAULT = 0
TRIGGER_DOUBLE_BID_ASK = 1
TRIGGER_LAST = 2
TRIGGER_DOUBLE_LAST = 3
TRIGGER_BID_ASK = 4
TRIGGER_LAST_OR_BID_ASK = 7
TRIGGER_MIDPOINT = 8
