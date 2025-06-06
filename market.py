import ccxt.pro as ccxtpro
import ccxt as ccxt
import asyncio
import json

from logger import LoggerFactory
from models import Tickers, Symbols
from data import Data


class Market:
    def __init__(
        self,
        exchange,
        key,
        secret,
        password,
        currency,
        market,
        loglevel,
        timeframe,
        history_data,
    ):
        self.currency = currency
        self.timeframe = timeframe
        self.history_data = history_data
        self.data = Data(loglevel)
        Market.exchange_class = getattr(ccxtpro, exchange)
        Market.exchange = Market.exchange_class(
            {
                "apiKey": key,
                "secret": secret,
                "options": {
                    "defaultType": market,
                },
            },
        )

        # Class variables
        Market.status = True
        Market.symbols = []
        Market.logging = LoggerFactory.get_logger(
            "logs/market.log", "market", log_level=loglevel
        )
        Market.logging.info("Initialized")

    def __convert_symbols(self, symbols: list) -> list:
        """Add the configured timestamp to the Symbol array

        Parameters
        ----------
        symbols: list
            List of symbols - has to be in format ["symbol"/currency"] (Example: [BTC/USDT])

        Returns
        -------
        list
            List of symbols with timestamps
        """
        symbol_list = []
        if symbols:
            for symbol in symbols:
                symbol_list.append([symbol, self.timeframe])
        else:
            Market.logging.error("Symbol list is empty!")

        return symbol_list

    async def __get_historical_data(self, symbol):
        ohlcv = []
        try:
            from_ts = self.exchange.parse8601(self.history_data)
            ohlcv_data = await self.exchange.fetch_ohlcv(
                symbol, self.timeframe, since=from_ts, limit=1000
            )
            while True:
                from_ts = ohlcv_data[-1][0]
                new_ohlcv = await self.exchange.fetch_ohlcv(
                    symbol, self.timeframe, since=from_ts, limit=1000
                )
                ohlcv_data.extend(new_ohlcv)
                if len(new_ohlcv) != 1000:
                    break
            symbol, market = symbol.split("/")

            for ticker in ohlcv_data:
                ticker = Tickers(
                    timestamp=ticker[0],
                    symbol=symbol + market,
                    open=ticker[1],
                    high=ticker[2],
                    low=ticker[3],
                    close=ticker[4],
                    volume=ticker[5],
                )
                ohlcv.append(ticker)
            return ohlcv
        except ccxt.NetworkError as e:
            Market.logging.error(
                f"Error fetching historical data from Exchange due to a network error: {e}"
            )
        except ccxt.ExchangeError as e:
            Market.logging.error(
                f"Error fetching historical data from Exchange due to an exchange error: {e}"
            )
        except Exception as e:
            Market.logging.error(
                f"Error fetching historical data from Exchange. Cause: {e}"
            )

    async def add_symbol(self, symbol) -> bool:
        """Adding new symbol to the ticker list."""
        symbol_list = []
        symbols = await self.data.get_symbols()
        ohlcv = []

        if symbols:
            for ticker in symbols:
                symbol_list.append(ticker)

        if symbol not in symbol_list:
            try:
                # add initial historic data into database
                ohlcv = await self.__get_historical_data(symbol)
                # Write history data to database
                await self.__process_data(ohlcv, bulk=True)
                # Add symbol to symbol table
                await Symbols.create(symbol=symbol)
                Market.logging.info(f"Added Symbol {symbol}.")
                # Add symbol to websocket subscription
                symbol_list.append(symbol)
                Market.symbols = self.__convert_symbols(symbol_list)
                return True
            except Exception as e:
                Market.logging.error(f"Error writing ticker data in to db: {e}")
                return False
        else:
            Market.logging.info("Symbol already on the list.")
            return False

    async def status_symbols(self):
        symbol_list = []
        for symbol, timerange in Market.symbols:
            symbol, market = symbol.split("/")
            symbol_list.append(f"{symbol}{market}@{timerange}")

        return json.dumps(symbol_list)

    async def remove_symbol(self, symbol):
        """Remove new symbol to the ticker list."""
        symbols = await self.data.get_symbols()
        symbol_list = []

        if symbols:
            for ticker in symbols:
                symbol_list.append(ticker)

            if symbol in symbol_list:
                symbol_list.remove(symbol)
                Market.symbols = self.__convert_symbols(symbol_list)
                try:
                    query = await Symbols.filter(symbol=symbol).delete()
                    symbol, currency = symbol.split("/")
                    symbol = symbol + currency
                    query = await Tickers.filter(symbol=symbol).delete()
                    Market.logging.info(
                        f"Start removing symbol. Deleted {query} entries for {symbol}"
                    )
                    return True
                except Exception as e:
                    Market.logging.error(f"Error removing symbol from database: {e}")
                    return False
            else:
                Market.logging.info("Symbol not on the list.")

            return False
        else:
            Market.logging.info("No initial Symbols yet - please add one.")

            return False

    async def __process_data(self, ohlcv, bulk=False):
        try:
            if bulk:
                await Tickers.bulk_create(ohlcv)
            else:
                symbol, market = ohlcv["symbol"].split("/")
                await Tickers.create(
                    timestamp=ohlcv["timestamp"],
                    symbol=symbol + market,
                    open=ohlcv["open"],
                    high=ohlcv["high"],
                    low=ohlcv["low"],
                    close=ohlcv["close"],
                    volume=ohlcv["volume"],
                )
        except Exception as e:
            Market.logging.error(f"Error writing ticker data in to db: {e}")

    async def watch_tickers(self):
        last_candles = {}

        # Initial list for symbols in database
        symbols = await self.data.get_symbols()

        if symbols:
            Market.symbols = self.__convert_symbols(symbols)
            # Track the last candle for each symbol
            last_candles = {symbol: None for symbol in symbols}

        actual_symbols = Market.symbols
        while Market.status:
            if Market.symbols:
                # Reload on symbol list change
                if Market.symbols == actual_symbols:
                    ohlcvs = None
                    try:
                        ohlcvs = await Market.exchange.watch_ohlcv_for_symbols(
                            Market.symbols
                        )
                    except ccxt.NetworkError as e:
                        Market.logging.error(
                            f"Error watching websocket data from Exchange due to a network error: {e}"
                        )
                        continue
                    except ccxt.ExchangeError as e:
                        Market.logging.error(
                            f"Error watching websocket data from Exchange due to an exchange error: {e}"
                        )
                        continue
                    except Exception as e:
                        Market.logging.error(f"CCXT websocket error. Cause: {e}")
                        continue
                    if ohlcvs:
                        for symbol, timeframes in ohlcvs.items():
                            for tf, ohlcv_list in timeframes.items():
                                # Process the last candle from the list for the given symbol and timeframe
                                if ohlcv_list:
                                    current_candle = ohlcv_list[-1]
                                    timestamp, open, high, low, close, volume = (
                                        current_candle
                                    )
                                    # Check if it's a new candle for the current symbol
                                    if symbol in last_candles:
                                        if (
                                            last_candles[symbol] is None
                                            or last_candles[symbol][0] < timestamp
                                        ):
                                            # The previous candle for this symbol has closed; write it to the database
                                            if last_candles[symbol]:
                                                (
                                                    timestamp,
                                                    open,
                                                    high,
                                                    low,
                                                    close,
                                                    volume,
                                                ) = last_candles[symbol]
                                                ohlcv = {
                                                    "timestamp": timestamp,
                                                    "symbol": symbol,
                                                    "open": open,
                                                    "high": high,
                                                    "low": low,
                                                    "close": close,
                                                    "volume": volume,
                                                }
                                                await self.__process_data(ohlcv)
                                                Market.logging.debug(ohlcv)

                                            last_candles[symbol] = current_candle
                                    # Add new initial symbol for candle
                                    else:
                                        last_candles[symbol] = None
                    else:
                        self.logging.error("OHLCV data empty")
                else:
                    actual_symbols = Market.symbols
                    Market.logging.info(f"Actual symbol list: {actual_symbols}")
                    continue
            else:
                await asyncio.sleep(5)

    async def shutdown(self):
        Market.status = False
        await Market.exchange.close()
