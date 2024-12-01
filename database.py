import asyncio
import datetime

from tortoise import Tortoise, run_async
from logger import LoggerFactory
from models import Tickers


class Database:
    def __init__(self, db_file, loglevel, housekeeping_interval):
        self.db_housekeeping_interval = housekeeping_interval
        # Logging
        self.logging = LoggerFactory.get_logger(
            "logs/moonloader.log", "database", log_level=loglevel
        )
        self.logging.info("Initialized")
        self.db_file = db_file

        # Class variables
        Database.status = True

    async def init(self):
        await Tortoise.init(
            db_url=f"sqlite://db/{self.db_file}", modules={"models": ["models"]}
        )
        # Generate the schema
        await Tortoise.generate_schemas()

    async def cleanup(self):
        while Database.status:
            actual_timestamp = datetime.datetime.now()
            cleanup_timestamp = actual_timestamp - datetime.timedelta(
                minutes=self.db_housekeeping_interval
            )
            try:
                query = await Tickers.filter(
                    timestamp__lt=cleanup_timestamp.timestamp()
                ).delete()
                self.logging.info(
                    f"Start housekeeping. Delete {query} entries older then {cleanup_timestamp}"
                )
            except Exception as e:
                self.logging.error(f"Error db housekeeping: {e}")

            await asyncio.sleep(self.db_housekeeping_interval)

    async def shutdown(self):
        Database.status = False
        await Tortoise.close_connections()
