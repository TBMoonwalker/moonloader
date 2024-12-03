import ccxt.pro as ccxtpro
import asyncio
import json
import re

from datetime import datetime
from logger import LoggerFactory
from models import Tickers, Symbols


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
        self.market = market
        self.timeframe = timeframe
        self.history_data = history_data
        self.exchange_id = exchange
        self.exchange_class = getattr(ccxtpro, self.exchange_id)
        Market.exchange = self.exchange_class(
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

    async def __get_symbols(self):
        """Get actual list of symbols from the database."""
        tickers = None
        try:
            tickers = await Symbols.all().distinct().values_list("symbol", flat=True)
        except Exception as e:
            Market.logging.error(f"Error fetching actual symbol list. Cause: {e}")

        return tickers

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
            # print(ohlcv)
            symbol, market = symbol.split("/")

            for ticker in ohlcv_data:
                timestamp = datetime.fromtimestamp(ticker[0] / 1000.0)
                ticker = Tickers(
                    timestamp=timestamp,
                    symbol=symbol + market,
                    open=ticker[1],
                    high=ticker[2],
                    low=ticker[3],
                    close=ticker[4],
                    volume=ticker[5],
                )
                ohlcv.append(ticker)
        except Exception as e:
            Market.logging.error(
                f"Error fetching historical data from Exchange. Cause: {e}"
            )
            # Remove symbol if it cannot be retrieved by websocket
            match = re.search(r"\b[A-Z]+/[A-Z]+\b", e)
            if match:
                await self.remove_symbol(match.group())
                Market.logging.error(
                    f"Removed symbol, because it doesn't exist or is too new to catch"
                )

        return ohlcv

    async def add_symbol(self, symbol) -> bool:
        """Adding new symbol to the ticker list."""
        symbol_list = []
        symbols = await self.__get_symbols()

        if symbols:
            for ticker in symbols:
                symbol_list.append(ticker)

        if symbol not in symbol_list:
            symbol_list.append(symbol)
            Market.symbols = self.__convert_symbols(symbol_list)
            try:
                await Symbols.create(symbol=symbol)

                # add initial historic data into database
                ohlcv = await self.__get_historical_data(symbol)
                await self.__process_data(ohlcv, bulk=True)

                Market.logging.info(f"Added Symbol {symbol}.")
            except Exception as e:
                Market.logging.error(f"Error writing ticker data in to db: {e}")
                return False

            return True
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
        symbols = await self.__get_symbols()
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
                timestamp = datetime.fromtimestamp(ohlcv["timestamp"] / 1000.0)
                await Tickers.create(
                    timestamp=timestamp,
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
        last_price = None

        # Initial list for symbols in database
        symbols = await self.__get_symbols()

        if symbols:
            Market.symbols = self.__convert_symbols(symbols)

        actual_symbols = Market.symbols
        while Market.status:
            if Market.symbols:
                # Reload on symbol list change
                if Market.symbols == actual_symbols:
                    try:
                        tickers = await Market.exchange.watch_ohlcv_for_symbols(
                            Market.symbols
                        )
                        for symbol in tickers:
                            for ticker in tickers[symbol]:
                                timestamp = float(tickers[symbol][ticker][0][0])
                                open = float(tickers[symbol][ticker][0][1])
                                high = float(tickers[symbol][ticker][0][2])
                                low = float(tickers[symbol][ticker][0][3])
                                close = float(tickers[symbol][ticker][0][4])
                                volume = float(tickers[symbol][ticker][0][5])

                                if last_price:
                                    # Only write to database on price change
                                    if float(close) != float(last_price):
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
                                        last_price = close
                                        Market.logging.debug(ohlcv)
                                # First value on init
                                else:
                                    last_price = close
                    except Exception as e:
                        Market.logging.error(f"CCXT websocket error. Cause: {e}")
                        # Remove symbol if it cannot be retrieved by websocket
                        match = re.search(r"\b[A-Z]+/[A-Z]+\b", str(e))
                        if match:
                            await self.remove_symbol(match.group())
                            Market.logging.error(
                                f"Removed symbol, because it doesn't exist or is too new to catch"
                            )
                        pass
                else:
                    actual_symbols = Market.symbols
                    Market.logging.info(f"Actual symbol list: {actual_symbols}")
                    continue
            else:
                await asyncio.sleep(5)

    async def shutdown(self):
        Market.status = False
        await Market.exchange.close()
