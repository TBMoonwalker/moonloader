import os
import asyncio

from config import Config
from market import Market
from data import Data
from database import Database
from cmc import Cmc
from logger import LoggerFactory
from indicators import Indicators
from quart import Quart
from quart_cors import route_cors


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

# Create db and logs directories if they don't exist already
try:
    os.makedirs("logs", exist_ok=True)
    os.makedirs("db", exist_ok=True)
except:
    print(
        "Error creating 'db' and 'logs' directory - please create them manually and report it as a bug!"
    )
    exit(1)

logging = LoggerFactory.get_logger("logs/moonloader.log", "main", log_level=loglevel)

######################################################
#                        Init                        #
######################################################

# Initialize database
database = Database(
    "moonloader.sqlite", loglevel, attributes.get("housekeeping_interval", 1)
)

# Initialize Indicators
indicators = Indicators(
    loglevel=loglevel,
    currency=attributes.get("currency", "USDT"),
    timeframe=attributes.get("timeframe", "1m"),
)

# Initialize Data
data = Data(loglevel=loglevel)

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

# Initialize Global module
cmc = Cmc(cmc_api_key=attributes.get("cmc_api_key"), loglevel=loglevel)

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


@app.route("/api/v1/indicators/rsi/<symbol>/<timerange>/<length>", methods=["GET"])
async def rsi(symbol, timerange, length):
    df = None
    response = await indicators.calculate_rsi(df, symbol, timerange, int(length))

    return response


@app.route("/api/v1/indicators/btc_pulse/<timerange>", methods=["GET"])
async def btc_pulse(timerange):
    response = await indicators.calculate_btc_pulse(timerange)

    return response


@app.route("/api/v1/indicators/ema_cross/<symbol>/<timerange>", methods=["GET"])
async def ema_cross(symbol, timerange):
    df = None
    response = await indicators.calculate_ema_cross(df, symbol, timerange)

    return response


@app.route("/api/v1/indicators/ema/<symbol>/<timerange>/<length>", methods=["GET"])
async def ema(symbol, timerange, length):
    df = None
    response = await indicators.calculate_ema(df, symbol, timerange, int(length))

    return response


@app.route(
    "/api/v1/indicators/ema_slope/<symbol>/<timerange>/<length>", methods=["GET"]
)
async def ema_slope(symbol, timerange, length):
    df = None
    response = await indicators.calculate_ema_slope(df, symbol, timerange, int(length))

    return response


@app.route(
    "/api/v1/indicators/ema_distance/<symbol>/<timerange>/<length>", methods=["GET"]
)
async def ema_distance(symbol, timerange, length):
    df = None
    response = await indicators.calculate_ema_distance(
        df, symbol, timerange, int(length)
    )

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
    "/api/v1/indicators/rsi_slope/<symbol>/<timerange>/<length>", methods=["GET"]
)
async def rsi_slope(symbol, timerange, length):
    df = None
    response = await indicators.calculate_rsi_slope(df, symbol, timerange, int(length))

    return response


@app.route(
    "/api/v1/indicators/support/<symbol>/<timerange>/<numlevels>", methods=["GET"]
)
async def support_level(symbol, timerange, numlevels):
    response = await indicators.detect_support_levels(symbol, timerange, int(numlevels))

    return response


@app.route("/api/v1/indicators/marketstate/stablecoin_dominance", methods=["GET"])
async def stablecoin_dominance():
    response = await indicators.get_stablecoin_dominance()

    return response


@app.route("/api/v1/indicators/buy_signal/<symbol>/<timerange>", methods=["GET"])
async def buy_signal(symbol, timerange):
    response = await indicators.find_optimal_buy_level(symbol, timerange)

    return response


@app.route(
    "/api/v1/data/ohlcv/<symbol>/<timerange>/<timestamp_start>/<offset>",
    methods=["GET"],
)
@route_cors(allow_origin="*")
async def get_ohlcv(symbol, timerange, timestamp_start, offset):
    response = await data.get_ohlcv_for_pair(symbol, timerange, timestamp_start, offset)

    return response


@app.before_serving
async def startup():
    await database.init()

    app.add_background_task(database.cleanup)
    app.add_background_task(market.watch_tickers)
    app.add_background_task(cmc.get_global_data)
    app.add_background_task(data.data_sanity_check)


@app.after_serving
async def shutdown():
    await data.shutdown()
    await cmc.shutdown()
    await market.shutdown()
    await database.shutdown()


######################################################
#                     Main                           #
######################################################

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=attributes.get("port", "9130"))
