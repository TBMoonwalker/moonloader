import asyncio

from data import Data
from logger import LoggerFactory
from indicators import Indicators


class Signals:
    def __init__(self, loglevel, currency, queue):
        self.data = Data(loglevel)
        self.indicators = Indicators(loglevel, currency)

        # Class variables
        Signals.status = True
        Signals.logging = LoggerFactory.get_logger(
            "logs/signals.log", "signals", log_level=loglevel
        )
        Signals.logging.info("Initialized")
        Signals.queue = queue

    def __format(self, num):
        if num > 1000000:
            if not num % 1000000:
                return f"{num // 1000000}M"
            return f"{round(num / 1000000, 1)}M"
        return f"{num // 1000}K"

    async def catch_new_signals(self):
        while Signals.status:
            data = []
            symbols = await self.data.get_symbols()
            for symbol in symbols:
                symbol, market = symbol.split("/")
                symbol = symbol + market
                # Get Dataframe
                df = await self.data.get_data_for_pair(symbol, "1h", 24)
                # Signal calculation values
                volume_24h = await self.indicators.calculate_24h_volume_data(
                    df, symbol, "1h", 24
                )
                rsi_slope = await self.indicators.calculate_rsi_slope(
                    df, symbol, "15min", 14
                )
                rsi = await self.indicators.calculate_rsi(df, symbol, "15min", 14)
                ema_9_slope = await self.indicators.calculate_ema_slope(
                    df, symbol, "15min", 9
                )
                ema_50_slope = await self.indicators.calculate_ema_slope(
                    df, symbol, "15min", 9
                )
                ema_cross = await self.indicators.calculate_ema_cross(
                    df, symbol, "15min"
                )
                if (
                    ema_9_slope["status"] == "upward"
                    and ema_50_slope["status"] == "upward"
                    and rsi_slope["status"] == "upward"
                ) and ema_cross["status"] == "up":
                    data = {
                        "symbol": symbol,
                        "day_volume": self.__format(int(volume_24h["status"])),
                        "rsi": rsi["status"],
                        "rsi_slope": rsi_slope["status"],
                        "ema_9_slope": ema_9_slope["status"],
                        "ema_50_slope": ema_50_slope["status"],
                        "ema_cross": ema_cross["status"],
                    }

                    await Signals.queue.put(data)
            await asyncio.sleep(10)

    async def shutdown(self):
        Signals.status = False
