import pandas as pd
import asyncio

from datetime import datetime, timedelta, UTC
from logger import LoggerFactory
from models import Symbols, Tickers
from scipy.stats import linregress


class Data:
    def __init__(self, loglevel):

        # Class variables
        Data.status = True
        Data.logging = LoggerFactory.get_logger(
            "logs/data.log", "data", log_level=loglevel
        )
        Data.logging.info("Initialized")

    async def data_sanity_check(self):
        while Data.status:
            data = []
            symbols = await self.get_symbols()
            for symbol in symbols:
                try:
                    symbol, market = symbol.split("/")
                    symbol = symbol + market
                    # Get Dataframe
                    df = await self.get_data_for_pair(symbol, "15min", 1)
                    # Convert unix timestamp to datetime object
                    df["timestamp"] = pd.to_datetime(
                        df["timestamp"].astype(float),
                        utc=True,
                        origin="unix",
                        unit="ms",
                    )
                    actual_date = datetime.now(UTC)
                    last_candle_date = df["timestamp"].dropna().iloc[-1]
                    time_difference = actual_date - last_candle_date
                    if time_difference > timedelta(minutes=30):
                        Data.logging.error(
                            f"Old data found for {symbol}, Actual date: {actual_date}, Latest candle date: {last_candle_date} - Check websocket subscription"
                        )
                except Exception as e:
                    Data.logging.error(
                        f"No data available yet for {symbol} or data is to old. If this message exceeds 30 minutes check the websocket connection. Waiting for data ... {e}"
                    )

            await asyncio.sleep(60)

    async def get_symbols(self):
        """Get actual list of symbols from the database."""
        tickers = None
        try:
            tickers = await Symbols.all().distinct().values_list("symbol", flat=True)
        except Exception as e:
            Data.logging.error(f"Error fetching actual symbol list. Cause: {e}")

        return tickers

    def __calculate_min_date(self, timerange, length):
        # Convert timerange with buffer
        match timerange:
            case "1d":
                length_minutes = 2880
            case "4h":
                length_minutes = 480
            case "1h":
                length_minutes = 120
            case "15min":
                length_minutes = 30
            case "10min":
                length_minutes = 20
            case "5min":
                length_minutes = 10

            # If an exact match is not confirmed, this last case will be used if provided
            case _:
                length_minutes = 30

        # Input parameters
        num_candles = length  # Number of candles with buffer
        end_time = datetime.now()

        # Calculate the total look-back duration
        lookback_duration = timedelta(minutes=length_minutes * num_candles)

        # Calculate the minimum date
        min_date = end_time - lookback_duration

        return datetime.timestamp(min_date)

    async def get_ohlcv_for_pair(self, pair, timerange, timestamp_start, offset):
        # 600000 --> 60 minutes in milliseconds before
        # start_date = datetime.fromtimestamp(((float(timestamp_start) - 600000) / 1000.0),UTC,)
        start_timestamp = float(timestamp_start) - 60000
        ohlcv = {}
        query = (
            await Tickers.filter(symbol=pair)
            .filter(timestamp__gt=start_timestamp)
            .values("timestamp", "open", "high", "low", "close")
        )

        if query:
            df = pd.DataFrame(query)
            df["time"] = df["timestamp"].astype(int) / 1000
            df["time"] = df["time"] + 60 * int(offset)
            df.drop_duplicates(subset=["time"], inplace=True)
            df.rename(
                columns={
                    "open": "open",
                    "high": "high",
                    "low": "low",
                    "close": "close",
                },
                inplace=True,
            )
            ohlcv = df.to_json(orient="records")

        return ohlcv

    async def get_data_for_pair(self, pair, timerange, length):
        start_date = self.__calculate_min_date(timerange, length)
        query = (
            await Tickers.filter(symbol=pair).filter(timestamp__gt=start_date).values()
        )

        if query:
            df = pd.DataFrame(query)

            df.dropna(inplace=True)
        else:
            df = None

        return df

    def resample_data(self, ohlcv, timerange):
        df = pd.DataFrame(ohlcv)
        if not df.empty:

            # Convert unix timestamp to datetime object
            df["timestamp"] = pd.to_datetime(
                df["timestamp"].astype(float), utc=True, origin="unix", unit="ms"
            )
            # Set datetime index
            df = df.set_index("timestamp")

            # Resample to the configured timerange
            df_resample = df.resample(timerange).agg(
                {
                    "open": "first",
                    "high": "max",
                    "close": "last",
                    "low": "min",
                    "volume": "sum",
                }
            )

            # Reset index after resample
            df_resample.reset_index(inplace=True)

            # Clear empty values
            df_resample.dropna(inplace=True)

            return df_resample
        else:
            Data.logging.error("No historic data available yet for symbol")

            return None

    async def shutdown(self):
        Data.status = False
