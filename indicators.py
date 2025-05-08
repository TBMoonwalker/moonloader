import pandas as pd
import numpy as np
import talib

from data import Data
from datetime import datetime, timedelta
from logger import LoggerFactory
from models import Global
from scipy.stats import linregress


class Indicators:
    def __init__(self, loglevel, currency, timeframe):
        self.currency = currency
        self.data = Data(loglevel)
        self.timeframe = timeframe

        Indicators.logging = LoggerFactory.get_logger(
            "logs/indicators.log", "indicator", log_level=loglevel
        )
        Indicators.logging.info("Initialized")

    async def calculate_24h_volume_data(self, df, symbol, timerange, length):
        try:
            if df is None:
                df_raw = await self.data.get_data_for_pair(symbol, timerange, length)
            else:
                df_raw = df
            df = self.data.resample_data(df_raw, timerange)
            df_1d = df.tail(24)
            quote_volume = df_1d.apply(
                lambda row: (row["close"] * row["volume"]), axis=1
            )
            result = quote_volume.sum()
        except Exception as e:
            result = ""
            Indicators.logging.error(f"Error getting 24h volume data. Cause: {e}")
        return {"status": result}

    async def calculate_ema_distance(self, df, symbol, timerange, length):
        result = False
        try:
            if df is None:
                df_raw = await self.data.get_data_for_pair(symbol, timerange, length)
            else:
                df_raw = df
            df = self.data.resample_data(df_raw, timerange)
            ema = talib.EMA(df["close"], timeperiod=length)
            ema = ema.dropna().iloc[-1]
            close_price = df["close"].dropna().iloc[-1]
            percentage_diff = abs(close_price - ema) / ema * 100
            Indicators.logging.debug(
                f"close_price: {close_price}, ema: {ema}, percentage diff: {percentage_diff}"
            )
            if percentage_diff < 2:
                result = True
        except Exception as e:
            Indicators.logging.info(
                f"EMA Distance cannot be calculated, because we don't have enough history data: {e}"
            )
        return {"status": result}

    async def calculate_ema_slope(self, df, symbol, timerange, length):
        try:
            if df is None:
                df_raw = await self.data.get_data_for_pair(symbol, timerange, length)
            else:
                df_raw = df
            df = self.data.resample_data(df_raw, timerange)
            ema = talib.EMA(df["close"], timeperiod=length)
            ema_slope = ema.diff()
            ema_last_slope = ema_slope.dropna().iloc[-1]
            if ema_last_slope:
                if ema_last_slope > 0:
                    categories = "upward"
                elif ema_last_slope < 0:
                    categories = "downward"
                else:
                    categories = "flat"
            result = categories
        except Exception as e:
            result = ""
            Indicators.logging.info(
                f"EMA SLOPE cannot be calculated, because we don't have enough history data: {e}"
            )
        return {"status": result}

    async def calculate_rsi_slope(self, df, symbol, timerange, length):
        try:
            if df is None:
                df = await self.data.get_data_for_pair(symbol, timerange, length)
            rsi = talib.RSI(df["close"], timeperiod=length)
            rsi_slope = rsi.diff()
            rsi_last_slope = rsi_slope.dropna().iloc[-1]
            categories = "flat"
            if rsi_last_slope:
                if rsi_last_slope > 0:
                    categories = "upward"
                elif rsi_last_slope < 0:
                    categories = "downward"
            result = categories
        except Exception as e:
            result = ""
            Indicators.logging.info(
                f"RSI SLOPE cannot be calculated, because we don't have enough history data: {e}"
            )
        return {"status": result}

    async def calculate_rsi(self, df, symbol, timerange, length):
        try:
            if df is None:
                df_raw = await self.data.get_data_for_pair(symbol, timerange, length)
            else:
                df_raw = df
            df = self.data.resample_data(df_raw, timerange)
            rsi = talib.RSI(df["close"], timeperiod=length).dropna().iloc[-1]
        except:
            rsi = ""
        return {"status": rsi}

    async def calculate_price_action(self, symbol, timerange, length):
        price_action = 0
        df = await self.data.get_data_for_pair(symbol, timerange, length)
        df_resample = self.data.resample_data(df, timerange)

        try:
            price_action = (np.log(df_resample["close"].pct_change(length) + 1)) * 100
            price_action = price_action.dropna().iloc[-1]
        except:
            price_action = ""

        return {"status": price_action}

    async def calculate_ema_cross(self, symbol, timerange):
        result = None
        df_raw = await self.data.get_data_for_pair(symbol, timerange, 21)
        df = self.data.resample_data(df_raw, timerange)
        df["ema_short"] = talib.EMA(df["close"], timeperiod=9)
        df["ema_long"] = talib.EMA(df["close"], timeperiod=21)
        df.dropna(subset=["ema_short", "ema_long"], inplace=True)

        try:

            if (
                df.iloc[-2]["ema_short"] <= df.iloc[-2]["ema_long"]
                and df.iloc[-1]["ema_short"] >= df.iloc[-1]["ema_long"]
            ):
                result = "up"
            elif (
                df.iloc[-2]["ema_short"] >= df.iloc[-2]["ema_long"]
                and df.iloc[-1]["ema_short"] <= df.iloc[-1]["ema_long"]
            ):
                result = "down"
            else:
                result = "none"

        except Exception as e:
            Indicators.logging.error(
                f"EMA Cross cannot be calculated for {symbol}. Cause: {e}"
            )

        return {"status": result}

    async def calculate_ema(self, df, symbol, timerange, length):
        if df is None:
            df_raw = await self.data.get_data_for_pair(symbol, timerange, length)
        else:
            df_raw = df
        df = self.data.resample_data(df_raw, timerange)

        try:
            ema = talib.EMA(df["close"], timeperiod=length)
            ema = ema.dropna().iloc[-1]
        except:
            ema = ""

        return {"status": ema}

    async def calculate_btc_pulse(self, timerange):
        df = None
        btc_pulse = ""
        ema9 = await self.calculate_ema(df, f"BTC{self.currency}", timerange, 9)
        ema50 = await self.calculate_ema(df, f"BTC{self.currency}", timerange, 50)
        price_action = await self.calculate_price_action(
            f"BTC{self.currency}", timerange, 3
        )

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
        df = await self.data.get_data_for_pair(symbol, timerange, 20)
        df_resample = self.data.resample_data(df, timerange)

        try:
            # Calculate the SMA
            df_resample["sma"] = talib.SMA(df_resample, length=20)

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
        df = await self.data.get_data_for_pair(symbol, timerange, 20)
        df_resample = self.data.resample_data(df, timerange)

        sma = talib.SMA(df_resample, length=20).dropna().iloc[-1]
        return {"status": sma}

    async def get_stablecoin_dominance(self):
        begin_week = (
            datetime.now() + timedelta(days=(0 - datetime.now().weekday()))
        ).date()

        # Stablecoin dominance
        try:
            global_data = await Global.filter(
                date__gt=begin_week, indicator="stablecoin_dominance"
            ).values_list("value", flat=True)
            if len(global_data) >= 7:
                days = np.arange(1, len(global_data) + 1)

                # Linear Regression
                slope, intercept, r_value, p_value, std_err = linregress(
                    days, global_data
                )
                trend = (
                    "uptrend" if slope > 0 else "downtrend" if slope < 0 else "neutral"
                )

                return {"status": trend}
            else:
                Indicators.logging.debug(f"Available stablecoin data: {global_data}")
                Indicators.logging.error(
                    "Week data for Stablecoin dominance indicator not reached yet."
                )
                return {"status": "not enough data"}
        except Exception as e:
            Indicators.logging.error(
                f"Error getting stablecoin dominance for the week: {e}"
            )

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

        # TODO - Calculate the length exactly for support levels
        actual_df = await self.data.get_data_for_pair(symbol, timerange, 120)
        df_resample = self.data.resample_data(actual_df, timerange)

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
