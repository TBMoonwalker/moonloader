import re

from config import Config
from market import Market
from database import Database
from logger import LoggerFactory
from indicators import Indicators
from quart import Quart


######################################################
#                       Config                       #
######################################################

# load configuration file
attributes = Config()

# Set logging facility
if attributes.get("debug", False):
    loglevel = "DEBUG"
else:
    loglevel = "INFO"

logging = LoggerFactory.get_logger("logs/moonloader.log", "main", log_level=loglevel)

######################################################
#                        Init                        #
######################################################

# Initialize database
database = Database(
    "moonloader.sqlite", loglevel, attributes.get("housekeeping_interval", 1)
)

# Initialize Indicators
indicators = Indicators(loglevel=loglevel)

# Initialize Market module
market = Market(
    exchange=attributes.get("exchange"),
    key=attributes.get("key"),
    secret=attributes.get("secret"),
    password=attributes.get("password", None),
    currency=attributes.get("currency", "USDT"),
    market=attributes.get("market", "spot"),
    loglevel=loglevel,
    timeframe=attributes.get("timeframe", "1m"),
    history_data=attributes.get("history_data", None),
)

# Initialize app
app = Quart(__name__)


######################################################
#                     Main methods                   #
######################################################


@app.route("/api/v1/symbol/add/<symbol>", methods=["GET"])
async def add_symbol(symbol):
    symbol = symbol.split(attributes.get("currency", "USDT"))[0]
    symbol = f"{symbol}/{attributes.get('currency', 'USDT')}"
    response = await market.add_symbol(symbol)
    if not response:
        response = {"result": ""}
    else:
        response = {"result": "ok"}

    return response


@app.route("/api/v1/symbol/remove/<symbol>", methods=["GET"])
async def remove_symbol(symbol):
    symbol = symbol.split(attributes.get("currency", "USDT"))[0]
    symbol = f"{symbol}/{attributes.get('currency', 'USDT')}"
    status = await market.remove_symbol(symbol)
    if not status:
        response = {"result": ""}
    else:
        response = {"result": "ok"}

    return response


@app.route("/api/v1/symbol/list", methods=["GET"])
async def status_symbol():
    symbol_list = await market.status_symbols()
    if not symbol_list:
        response = {"result": ""}
    else:
        response = '{"result": ' + str(symbol_list) + "}"

    return response


@app.route("/api/v1/indicators/rsi/<symbol>/<timerange>", methods=["GET"])
async def rsi(symbol, timerange):
    response = await indicators.calculate_rsi(symbol, timerange)

    return response


@app.route("/api/v1/indicators/btc_pulse/<timerange>", methods=["GET"])
async def btc_pulse(timerange):
    response = await indicators.calculate_btc_pulse(timerange)

    return response


@app.route("/api/v1/indicators/ema_cross/<symbol>/<timerange>", methods=["GET"])
async def ema_cross(symbol, timerange):
    response = await indicators.calculate_ema_cross(symbol, timerange)

    return response


@app.route("/api/v1/indicators/ema/<symbol>/<timerange>/<length>", methods=["GET"])
async def ema(symbol, timerange, length):
    response = await indicators.calculate_ema(symbol, timerange, length)

    return response


@app.route("/api/v1/indicators/sma/<symbol>/<timerange>", methods=["GET"])
async def sma(symbol, timerange):
    response = await indicators.calculate_sma(symbol, timerange)

    return response


@app.route("/api/v1/indicators/sma_slope/<symbol>/<timerange>", methods=["GET"])
async def sma_slope(symbol, timerange):
    response = await indicators.categorize_sma_slope(symbol, timerange)

    return response


@app.route(
    "/api/v1/indicators/support/<symbol>/<timerange>/<numlevels>", methods=["GET"]
)
async def support_level(symbol, timerange, numlevels):
    response = await indicators.detect_support_levels(symbol, timerange, int(numlevels))

    return response


@app.before_serving
async def startup():
    await database.init()

    app.add_background_task(database.cleanup)
    app.add_background_task(market.watch_tickers)


@app.after_serving
async def shutdown():
    await market.shutdown()
    await database.shutdown()


######################################################
#                     Main                           #
######################################################

if __name__ == "__main__":
    app.run(host="0.0.0.0", port="9130")
