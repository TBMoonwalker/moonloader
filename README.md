# Moonloader
## Summary
Moonloader is a service which scrapes exchange ticker data through websockets. It uses this data to create various indicators and exposes them through websocket or REST apis.

## Disclaimer
**Moonloader is meant to be used for educational purposes only. Use with real funds at your own risk**

## Prerequisites
- A Linux server with a static ip address
- Configured API access on your exchange
- Python 3.10.x or higher

## Installation
```pip install -r requirements.txt```

## Configuration (config.ini)
Name | Type | Mandatory | Values(default) | Description
------------ | ------------ | ------------ | ------------ | ------------
timezone | string | YES | (Europe/London) | Timezone used by the logging framework
debug | boolean | NO | (false) true  | Logging debugging information into various logs
port | integer | NO | (8120) | Port to use for the internal webserver (Must be port 80 for http and Tradingview use)
exchange | string | YES | (binance) | Used exchange for trading
key | string | YES | () | API Key taken from the exchange you are using
secret | string | YES | () | API Secret taken from the exchange you are using
timeframe | string | YES | (15m) | Timerange to get ticker data from websockets - 15m means it gets 15m candles back from the exchange websocket.
currency | string | YES | (USDT) | Trading currency to use
market | string | YES | (spot) | Only spot is possible at this time
history_data | string | YES | (2024-09-01T00:00:00Z) | Timestamp until which date the historical data should be scraped for indicators
housekeeping_interval  | int | YES | (86400) | Interval when the database data gets pruned in minutes. Default is 86400 which means every 60 days

When you are ready with the configuration, copy the ``config.ini.example`` to ``config.ini`` and start the bot.

## Run
```python app.py```

## Logging
Logs are available in the ```logs/``` directory.