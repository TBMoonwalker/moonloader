import requests
import asyncio

from datetime import datetime
from logger import LoggerFactory
from models import Global
from tenacity import wait_fixed, retry, stop_after_attempt, TryAgain


class Cmc:
    def __init__(self, loglevel, cmc_api_key):
        self.cmc_api_key = cmc_api_key

        # Class variables
        Global.status = True
        Global.logging = LoggerFactory.get_logger(
            "logs/global.log", "market", log_level=loglevel
        )
        Global.logging.info("Initialized")

    @retry(wait=wait_fixed(600), stop=stop_after_attempt(10))
    async def __get_stablecoin_dominance(self):
        result = False
        total_market_cap = None
        stablecoin_market_cap = None
        actual_date = datetime.now().date()
        headers = {"X-CMC_PRO_API_KEY": self.cmc_api_key}
        ws_endpoint = "pro-api.coinmarketcap.com"
        ws_context = "v1/global-metrics/quotes/latest"
        url = f"https://{ws_endpoint}/{ws_context}"

        # Check if data already fetched for that day
        try:
            query = await Global.filter(date=actual_date).values()
        except Exception as e:
            Global.logging.error(
                f"Error getting existing values from database. Cause {e}"
            )

        if not query:
            response = requests.get(
                url,
                headers=headers,
            )

            try:
                json_data = response.json()
            except Exception as e:
                Global.logging.error(
                    f"Error fetching CMC global market data, cause: {e}"
                )
                raise TryAgain

            if json_data["status"]["error_code"] == 0:
                total_market_cap = json_data["data"]["quote"]["USD"]["total_market_cap"]
                stablecoin_market_cap = json_data["data"]["quote"]["USD"][
                    "stablecoin_market_cap"
                ]
                stablecoin_dominance = (stablecoin_market_cap / total_market_cap) * 100

                try:
                    await Global.create(
                        date=actual_date,
                        indicator="stablecoin_dominance",
                        value=stablecoin_dominance,
                    )
                    Global.logging.info(
                        f"Successfully added stablecoin dominance data for {date}"
                    )
                except Exception as e:
                    Global.logging.error(
                        f"Error importing stablecoin dominance data into database. Cause {e}. Trying again."
                    )
                    raise TryAgain
            else:
                Global.logging.error(
                    f"CMC global market data is garbage. Error: {json_data["status"]["error_code"]}. Trying again."
                )
                raise TryAgain
        else:
            Global.logging.info("Data already fetched for today.")
            result = True

        return result

    async def get_global_data(self):
        while Global.status:
            sleeptime = 60
            # Fetch stablecoin dominance
            stablecoin_dominance = await self.__get_stablecoin_dominance()
            if stablecoin_dominance:
                sleeptime = 86400
            await asyncio.sleep(sleeptime)

    async def shutdown(self):
        Global.status = False
