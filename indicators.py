import pandas as pd
import pandas_ta as ta
import numpy as np

from logger import LoggerFactory
from models import Tickers


class Indicators:
    def __init__(self, loglevel):
        Indicators.logging = LoggerFactory.get_logger(
            "logs/indicators.log", "indicator", log_level=loglevel
        )
        Indicators.logging.info("Initialized")

    async def __get_ticker_from_symbol(self, symbol):
        query = await Tickers.filter(symbol=symbol).values()

        if query:
            df = pd.DataFrame(query)

            df.dropna(inplace=True)
        else:
            df = None

        return df

    def __resample_data(self, ohlcv, timerange):
        df = pd.DataFrame(ohlcv)
        if not df.empty:

            # Set datetime index
            df = df.set_index("timestamp")

            # Resample to the configured timerange
            df_resample = df.resample(timerange).agg(
                {
                    "open": "first",
                    "high": "max",
                    "close": "last",
                    "low": "min",
                    "volume": "max",
                }
            )

            # Reset index after resample
            df_resample.reset_index(inplace=True)

            # Clear empty values
            df_resample.dropna(inplace=True)

            return df_resample
        else:
            Indicators.logging.error("No data available for symbol")

            return None

    async def calculate_rsi(self, symbol, timerange):
        rsi = 0
        df = await self.__get_ticker_from_symbol(symbol)
        df_resample = self.__resample_data(df, timerange)

        try:
            rsi = df_resample.ta.rsi(length=14).dropna().iloc[-1]
        except:
            rsi = ""

        return {"status": rsi}

    async def calculate_price_action(self, symbol, timerange, length):
        price_action = 0
        df = await self.__get_ticker_from_symbol(symbol)
        df_resample = self.__resample_data(df, timerange)

        try:
            price_action = (np.log(df_resample["close"].pct_change(length) + 1)) * 100
            price_action = price_action.dropna().iloc[-1]
        except:
            price_action = ""

        return {"status": price_action}

    async def calculate_ema_cross(self, symbol, timerange):
        ema_20 = await self.calculate_ema(symbol, timerange, 20)
        ema_50 = await self.calculate_ema(symbol, timerange, 50)

        status = ""

        Indicators.logging.debug(f"EMA20: {ema_20}, EMA50: {ema_50}")

        try:
            if ema_20["status"] > ema_50["status"]:
                status = "up"
            elif ema_20["status"] < ema_50["status"]:
                status = "down"
        except:
            Indicators.logging.info(
                "EMA Cross cannot be calculated, because we don't have enough history data."
            )

        ema_cross = {"status": status}

        return ema_cross

    async def calculate_ema(self, symbol, timerange, length):
        ema = 0
        df = await self.__get_ticker_from_symbol(symbol)
        df_resample = self.__resample_data(df, timerange)

        try:
            ema = df_resample.ta.ema(length=length)
            ema = ema.dropna().iloc[-1]
        except:
            ema = ""

        return {"status": ema}

    async def calculate_btc_pulse(self, timerange):
        btc_pulse = ""
        ema9 = await self.calculate_ema("BTCUSDT", timerange, 9)
        ema50 = await self.calculate_ema("BTCUSDT", timerange, 50)
        price_action = await self.calculate_price_action("BTCUSDT", timerange, 3)

        try:
            if (price_action["status"] < -1) or (ema50["status"] > ema9["status"]):
                Indicators.logging.info("BTC-Pulse signaling downtrend")
                btc_pulse = "downtrend"
            else:
                btc_pulse = "uptrend"
        except:
            Indicators.logging.info(
                "BTC Pulse cannot be calculated, because we don't have enough history data."
            )

        return {"status": btc_pulse}

    async def __calculate_sma_slope(self, symbol, timerange):
        sma_slope = 0
        df = await self.__get_ticker_from_symbol(symbol)
        df_resample = self.__resample_data(df, timerange)

        try:
            # Calculate the SMA
            df_resample["sma"] = df_resample.ta.sma(length=20)

            # Calculate the SMA slope
            df_resample["sma_slope"] = df_resample[
                "sma"
            ].diff()  # Calculate the difference between consecutive SMA values

            sma_slope = df_resample["sma_slope"].dropna().iloc[-1]

            Indicators.logging.debug(f"SMA Slope: {sma_slope}")
        except:
            sma_slope = ""

        return sma_slope

    async def categorize_sma_slope(self, symbol, timerange):
        slope = await self.__calculate_sma_slope(symbol, timerange)
        categories = ""
        if slope:
            if slope > 0:
                categories = "upward"
            elif slope < 0:
                categories = "downward"
            else:
                categories = "flat"

        sma_slope = {"status": categories}

        return sma_slope

    async def calculate_sma(self, symbol, timerange):
        df = await self.__get_ticker_from_symbol(symbol)
        df_resample = self.__resample_data(df, timerange)

        sma = df_resample.ta.sma(length=20).dropna().iloc[-1]
        return {"status": sma}

    async def detect_support_levels(
        self,
        symbol,
        timerange,
        num_levels=5,
        lookback=5,
        tolerance=0.005,
        merge_tolerance=0.025,
    ):
        """
        Detects support levels based on local minima in the Low column of an OHLCV DataFrame.

        Parameters:
            lookback (int): The number of candles to look back to determine local minima.
            tolerance (float): The percentage tolerance to consider a price as being on a support level.

        Returns:
            pd.DataFrame: The original DataFrame with a 'Support' column indicating if it's near a support level.
        """

        actual_df = await self.__get_ticker_from_symbol(symbol)
        df_resample = self.__resample_data(actual_df, timerange)

        df = df_resample.copy()

        # Identify local minima over the specified lookback window
        df["Support_Level"] = df["low"][
            (df["low"] == df["low"].rolling(window=lookback, center=True).min())
        ]

        # Extract unique support levels and sort them
        support_levels = sorted(df["Support_Level"].dropna().unique())

        # Merge nearby support levels
        merged_support_levels = []
        if support_levels:
            group = [support_levels[0]]  # Start with the first level
            for level in support_levels[1:]:
                # Check if the current level is within merge tolerance of the last level in the group
                if level <= group[-1] * (1 + merge_tolerance):
                    group.append(level)
                else:
                    # Add the average of the group to the merged levels
                    merged_support_levels.append(sum(group) / len(group))
                    group = [level]  # Start a new group
            # Add the last group
            merged_support_levels.append(sum(group) / len(group))

        # Limit to the most recent `num_levels` merged support levels
        merged_support_levels = merged_support_levels[-num_levels:]

        # Get the last price
        last_price = actual_df["close"].iloc[-1]

        # Check if the last price is within tolerance of the most recent support level
        is_near_support = False
        for lvl in merged_support_levels:
            lower_bound = lvl * (1 - tolerance)
            upper_bound = lvl * (1 + tolerance)
            Indicators.logging.debug(
                f"Symbol: {symbol}, Merged Support Level: {lvl}, Range: {lower_bound} - {upper_bound}, Last Price: {last_price}"
            )
            if lower_bound <= last_price <= upper_bound:
                is_near_support = True

        return {"status": f"{is_near_support}"}
