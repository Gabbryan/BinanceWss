import logging
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from log.logging_config import log_safe

session = requests.Session()
retry = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)

adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
session.mount("https://", adapter)

REQUEST_LIMIT = 5
REQUEST_INTERVAL = 1  # seconds


def download_sync(ex, symbol, year, month=None, day=None, is_monthly=False):
    symbol_url_part = symbol.replace("/", "")
    if is_monthly:
        url = f"https://data.binance.vision/data/futures/um/monthly/aggTrades/{symbol_url_part}/{symbol_url_part}-aggTrades-{year}-{month:02d}.zip"
    elif year and month and day:
        url = f"https://data.binance.vision/data/futures/um/daily/aggTrades/{symbol_url_part}/{symbol_url_part}-aggTrades-{year}-{month:02d}-{day:02d}.zip"
    else:
        log_safe(
            f"Invalid parameters for {symbol} in {year}-{month}-{day}", logging.ERROR
        )
        return None

    log_safe(f"Downloading {symbol} {'monthly' if is_monthly else 'daily'} data: {url}")

    try:
        response = session.get(url, stream=True, timeout=10)
        if response.status_code == 200:
            return response.content
        else:
            log_safe(
                f"Failed to download {symbol} for {year}-{month:02d}{'-' + str(day) if day else ''}: {response.status_code}\nURL: {url}",
                logging.ERROR,
            )
            return None
    except Exception as e:
        log_safe(
            f"Exception occurred while downloading {symbol} for {year}-{month:02d}{'-' + str(day) if day else ''}: {e}",
            logging.ERROR,
        )
        return None
    finally:
        time.sleep(REQUEST_INTERVAL / REQUEST_LIMIT)
