import pandas as pd
import pandas_ta as ta
import numpy as np

from data import Data
from datetime import datetime, timedelta
from logger import LoggerFactory
from models import Global
from scipy.stats import linregress


class Indicators:
    def __init__(self, loglevel, currency):
        self.currency = currency
        self.data = Data(loglevel)

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
            ema = df.ta.ema(length=length)
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
            ema = df.ta.ema(length=length)
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
            rsi = df.ta.rsi(length=length)
            rsi_slope = rsi.diff()
            rsi_last_slope = rsi_slope.dropna().iloc[-1]
            if rsi_last_slope:
                if rsi_last_slope > 0:
                    categories = "upward"
                elif rsi_last_slope < 0:
                    categories = "downward"
                else:
                    categories = "flat"
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
            rsi = df.ta.rsi(length=length).dropna().iloc[-1]
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

    async def calculate_ema_cross(self, df, symbol, timerange):
        ema_9 = await self.calculate_ema(df, symbol, timerange, 9)
        ema_50 = await self.calculate_ema(df, symbol, timerange, 50)

        status = ""

        Indicators.logging.debug(f"EMA20: {ema_9}, EMA50: {ema_50}")

        try:
            if ema_9["status"] > ema_50["status"]:
                status = "up"
            elif ema_9["status"] < ema_50["status"]:
                status = "down"
        except:
            Indicators.logging.info(
                "EMA Cross cannot be calculated, because we don't have enough history data."
            )

        ema_cross = {"status": status}

        return ema_cross

    async def calculate_ema(self, df, symbol, timerange, length):
        if df is None:
            df_raw = await self.data.get_data_for_pair(symbol, timerange, length)
        else:
            df_raw = df
        df = self.data.resample_data(df_raw, timerange)

        try:
            ema = df.ta.ema(length=length)
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
        df = await self.data.get_data_for_pair(symbol, timerange, 20)
        df_resample = self.data.resample_data(df, timerange)

        sma = df_resample.ta.sma(length=20).dropna().iloc[-1]
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

    async def __detect_bullish_engulfing(self, symbol, timerange):
        df = await self.data.get_data_for_pair(symbol, "15min", 200)
        df_resample = self.data.resample_data(df, "15min")

        df_resample.rename(
            columns={
                "timestamp": "Date",
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
            },
            inplace=True,
        )

        # Apply the 'cdl_pattern' method for ENGULFING
        # df["Engulfing"] = ta.cdl_pattern(df_resample, name="engulfing")
        df_resample["Engulfing"] = ta.cdl_pattern(
            open_=df_resample["Open"],
            high=df_resample["High"],
            low=df_resample["Low"],
            close=df_resample["Close"],
            name="engulfing",
        )

        # Interpretation:
        # - Positive values indicate a bullish engulfing pattern.
        # - Negative values indicate a bearish engulfing pattern.

        # Print results
        print(df_resample[["Date", "Open", "High", "Low", "Close", "Engulfing"]])

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

    async def find_optimal_buy_level(self, symbol, timerange):
        """
        Finds an optimal buy level based on indicators and timeranges.

        Parameters:
            data (pd.DataFrame): DataFrame containing OHLCV data with 'Date', 'Open', 'High', 'Low', 'Close'.
            timerange (str): Time range for analysis (e.g., "15min", "4h", "1d").
            indicators_config (dict): Configuration for indicators.
                Example: {"rsi": {"length": 14}, "ema": {"length": 50}}

        Returns:
            pd.DataFrame: DataFrame with buy signals and relevant columns.
        """
        actual_df = await self.data.get_data_for_pair(symbol, timerange, 300)
        data = self.data.resample_data(actual_df, timerange)

        data.rename(
            columns={
                "timestamp": "Date",
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "volume": "Volume",
            },
            inplace=True,
        )

        # Resample data to match the desired timerange
        data = (
            data.set_index("Date")
            .resample(timerange)
            .agg(
                {
                    "Open": "first",
                    "High": "max",
                    "Low": "min",
                    "Close": "last",
                    "Volume": "sum",
                }
            )
            .dropna()
            .reset_index()
        )

        # Add indicators based on configuration
        data["RSI"] = ta.rsi(data["Close"], length=14)
        data[f"EMA_9"] = ta.ema(data["Close"], length=9)
        data[f"EMA_50"] = ta.ema(data["Close"], length=50)

        # Calculate percentage difference
        data["EMA_Diff_Percent"] = (
            (data[f"EMA_9"] - data[f"EMA_50"]) / data[f"EMA_50"]
        ) * 100

        # Identify potential reversal signals
        data["Reversal_Signal"] = (data["EMA_Diff_Percent"].shift(1) < 0) & (
            data["EMA_Diff_Percent"] >= 0
        ) | (  # Bullish crossover
            data["EMA_Diff_Percent"].shift(1) > 0
        ) & (
            data["EMA_Diff_Percent"] <= 0
        )  # Bearish crossover

        # Filter by threshold (optional)
        data["Strong_Reversal"] = abs(data["EMA_Diff_Percent"]) > 0.5

        data["Pattern"] = ta.cdl_pattern(
            open_=data["Open"],
            high=data["High"],
            low=data["Low"],
            close=data["Close"],
            name="engulfing",
        )

        print(data.to_string())

        # Buy signal conditions
        data["Buy_Signal"] = (
            (data["RSI"] < 30)  # RSI is oversold
            & data["Reversal_Signal"]
            & (data["Pattern"] == 100)  # Bullish candlestick pattern (e.g., Engulfing)
        )

        # Filter rows with Buy signals
        buy_signals = data[data["Buy_Signal"] == True]

        print(buy_signals)

        return {"status": f"{buy_signals}"}
