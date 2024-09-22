import logging
import time
from io import BytesIO
from zipfile import ZipFile

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from log.logging_config import log_safe

# Configure request session with retries and increased connection pool size
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


def download_and_extract(ex, symbol, year, month=None, day=None):
    symbol_url_part = symbol.replace("/", "")
    if year and month and day:
        url = f"https://data.binance.vision/data/futures/um/daily/aggTrades/{symbol_url_part}/{symbol_url_part}-aggTrades-{year}-{month:02d}-{day:02d}.zip"
    else:
        log_safe(
            f"Invalid parameters for {symbol} in {year}-{month}-{day}", logging.ERROR
        )
        return None

    log_safe(f"Downloading {symbol} {year}-{month:02d}-{day:02d}: {url}")

    try:
        response = session.get(url, stream=True, timeout=10)
        if response.status_code == 200:
            file_content = response.content
            file_like_object = BytesIO(file_content)
            zipfile = ZipFile(file_like_object)
            files_content = [
                zipfile.read(file_name) for file_name in zipfile.namelist()
            ]
            log_safe(
                f"Successfully downloaded and extracted {symbol} for {year}-{month:02d}-{day:02d}"
            )
            return files_content
        else:
            log_safe(
                f"Failed to download {symbol} for {year}-{month:02d}-{day:02d}: {response.status_code}\nURL: {url}",
                logging.ERROR,
            )
            return None
    except Exception as e:
        log_safe(
            f"Exception occurred while downloading {symbol} for {year}-{month:02d}-{day:02d}: {e}",
            logging.ERROR,
        )
        return None
    finally:
        time.sleep(REQUEST_INTERVAL / REQUEST_LIMIT)


def download_and_extract_monthly(ex, symbol, year, month):
    symbol_url_part = symbol.replace("/", "")
    url = f"https://data.binance.vision/data/futures/um/monthly/aggTrades/{symbol_url_part}/{symbol_url_part}-aggTrades-{year}-{month:02d}.zip"

    log_safe(f"Downloading {symbol} {year}-{month:02d}: {url}")

    try:
        response = session.get(url, stream=True, timeout=10)
        if response.status_code == 200:
            file_content = response.content
            file_like_object = BytesIO(file_content)
            zipfile = ZipFile(file_like_object)
            files_content = [
                zipfile.read(file_name) for file_name in zipfile.namelist()
            ]
            log_safe(
                f"Successfully downloaded and extracted {symbol} for {year}-{month:02d}"
            )
            return files_content
        else:
            log_safe(
                f"Failed to download {symbol} for {year}-{month:02d}: {response.status_code}\nURL: {url}",
                logging.ERROR,
            )
            return None
    except Exception as e:
        log_safe(
            f"Exception occurred while downloading {symbol} for {year}-{month:02d}: {e}",
            logging.ERROR,
        )
        return None
    finally:
        time.sleep(REQUEST_INTERVAL / REQUEST_LIMIT)
