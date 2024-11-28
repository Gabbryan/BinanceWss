import asyncio
import csv
import io
import logging
import os
import tempfile
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from datetime import datetime
from functools import wraps
import inspect
import aiohttp
import pandas as pd
import psutil
import pyarrow.csv as pc
import requests
from dotenv import load_dotenv
from google.cloud import bigtable
from google.cloud.bigtable import row
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    FloatType,
    BooleanType,
)
from yaspin import yaspin
from tabulate import tabulate

# Setup and Configuration
load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS"
)

BIGTABLE_PROJECT_ID = os.getenv("BIGTABLE_PROJECT_ID")
BIGTABLE_INSTANCE_ID = os.getenv("BIGTABLE_INSTANCE_ID")
BIGTABLE_TABLE_ID = os.getenv("BIGTABLE_TABLE_ID")

client = bigtable.Client(project=BIGTABLE_PROJECT_ID, admin=True)
instance = client.instance(BIGTABLE_INSTANCE_ID)
table = instance.table(BIGTABLE_TABLE_ID)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Utility Functions
def measure_time_and_resources(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"Starting {func.__name__}")
        start_time = time.time()
        process = psutil.Process()
        start_cpu_time = process.cpu_percent()
        start_memory = process.memory_info().rss

        result = func(*args, **kwargs)

        end_time = time.time()
        end_cpu_time = process.cpu_percent()
        end_memory = process.memory_info().rss

        execution_time = end_time - start_time
        cpu_usage = (end_cpu_time - start_cpu_time) / psutil.cpu_count()
        memory_usage = end_memory - start_memory

        logger.info(f"Finished {func.__name__} in {execution_time:.2f} seconds")
        logger.info(
            f"{func.__name__} - CPU Usage: {cpu_usage:.2f}%, Memory Usage: {memory_usage / (1024 * 1024):.2f} MB"
        )

        return result, execution_time, cpu_usage, memory_usage

    return wrapper


# BenchmarkRunner Class
class BenchmarkRunner:
    def __init__(self, symbol, year, month):
        self.symbol = symbol
        self.year = year
        self.month = month
        self.results = []

        jar_paths = [
            "jars/bigtable-hbase-beam-2.24.0.jar",
            "jars/hadoop-common-3.3.4.jar",
            "jars/hbase-client-2.4.12.jar",
            "jars/hbase-common-2.4.12.jar",
            "jars/hbase-protocol-2.4.12.jar",
            "jars/hbase-spark-2.0.0-alpha-1.jar",
            "jars/scala-library-2.12.18.jar",
        ]

        jars = ",".join(jar_paths)
        self.spark = (
            SparkSession.builder.appName("BinanceDataProcessing")
            .config("spark.jars", jars)
            .config("spark.driver.memory", "20g")
            .config("spark.executor.memory", "20g")
            .config("spark.driver.extraClassPath", jars)
            .config("spark.executor.extraClassPath", jars)
            .getOrCreate()
        )

    def save_csv_locally(self, content, filename="extracted_data.csv"):
        with open(filename, mode="w", encoding="utf-8") as file:
            file.write(content)
        logger.info(f"CSV content saved locally to {filename}")

    def load_csv_locally(self, filename="extracted_data.csv"):
        if os.path.exists(filename):
            with open(filename, mode="r", encoding="utf-8") as file:
                content = file.read()
            logger.info(f"CSV content loaded from {filename}")
            return content
        else:
            logger.error(f"File {filename} does not exist")
            return None

    def run_benchmarks(self):
        logger.info("Starting benchmark tests")

        # Test download methods
        download_methods = [
            self.download_sync,
            self.download_threaded,
            self.download_async,
            self.download_multiprocess,
            self.download_chunked,
        ]

        for method in download_methods:
            try:
                logger.info(f"Benchmarking {method.__name__}")
                result, execution_time, cpu_usage, memory_usage = method(
                    is_monthly=True
                )
                logger.info(
                    f"{method.__name__} completed in {execution_time:.2f} seconds"
                )
                logger.info(f"Downloaded data size: {len(result)} bytes")
                self.results.append(
                    {
                        "category": "download",
                        "method": method.__name__,
                        "execution_time": execution_time,
                        "cpu_usage": cpu_usage,
                        "memory_usage": memory_usage,
                    }
                )

                # Save the first successful download
                if not hasattr(self, "sample_content"):
                    self.sample_content = result
                    with zipfile.ZipFile(io.BytesIO(result)) as zf:
                        csv_content = zf.read(zf.namelist()[0]).decode("utf-8")
                    self.save_csv_locally(csv_content)
                    logger.info("Sample content saved locally")
            except Exception as e:
                logger.error(f"Error in {method.__name__}: {str(e)}")

        # Load CSV content for further processing
        csv_content = self.load_csv_locally()
        if csv_content is None and hasattr(self, "sample_content"):
            with zipfile.ZipFile(io.BytesIO(self.sample_content)) as zf:
                csv_content = zf.read(zf.namelist()[0]).decode("utf-8")
            self.save_csv_locally(csv_content)

        # Create a bytes-like object for methods expecting zip content
        zip_content = io.BytesIO()
        with zipfile.ZipFile(zip_content, "w") as zf:
            zf.writestr("data.csv", csv_content)
        zip_content.seek(0)

        # Test extraction and processing methods
        extraction_methods = [
            self.extract_and_process_in_memory,
            self.extract_and_process_streaming,
            self.extract_and_process_pandas,
            self.extract_and_process_pyarrow,
            self.extract_and_process_custom_yield,
            self.extract_and_process_pyspark,
        ]

        for method in extraction_methods:
            try:
                logger.info(f"Benchmarking {method.__name__}")
                if method.__name__ == "extract_and_process_in_memory":
                    result, execution_time, cpu_usage, memory_usage = method(
                        csv_content
                    )
                else:
                    result, execution_time, cpu_usage, memory_usage = method(
                        zip_content.getvalue()
                    )
                logger.info(
                    f"{method.__name__} completed in {execution_time:.2f} seconds"
                )
                logger.info(f"Processed {len(result)} rows")
                self.results.append(
                    {
                        "category": "processing",
                        "method": method.__name__,
                        "execution_time": execution_time,
                        "cpu_usage": cpu_usage,
                        "memory_usage": memory_usage,
                    }
                )
            except Exception as e:
                logger.error(f"Error in {method.__name__}: {str(e)}")

        # Use the result from extract_and_process_in_memory for upload methods
        sample_processed_data, _, _, _ = self.extract_and_process_in_memory(csv_content)
        sample_processed_data = sample_processed_data[
            :1000
        ]  # Limit to 1000 rows for testing

        # Test upload methods
        upload_methods = [
            self.upload_sync,
            self.upload_threaded,
            self.upload_async,
            self.upload_batch,
            self.upload_multiprocess,
        ]

        for method in upload_methods:
            try:
                logger.info(f"Benchmarking {method.__name__}")
                result, execution_time, cpu_usage, memory_usage = method(
                    sample_processed_data
                )
                logger.info(
                    f"{method.__name__} completed in {execution_time:.2f} seconds"
                )
                self.results.append(
                    {
                        "category": "upload",
                        "method": method.__name__,
                        "execution_time": execution_time,
                        "cpu_usage": cpu_usage,
                        "memory_usage": memory_usage,
                    }
                )
            except Exception as e:
                logger.error(f"Error in {method.__name__}: {str(e)}")

        logger.info("Benchmark tests completed")
        self.save_results_to_csv()

    def save_results_to_csv(self, filename="benchmark_results.csv"):
        with open(filename, mode="w", newline="") as file:
            writer = csv.DictWriter(
                file,
                fieldnames=[
                    "category",
                    "method",
                    "execution_time",
                    "cpu_usage",
                    "memory_usage",
                ],
            )
            writer.writeheader()
            for result in self.results:
                writer.writerow(result)
        logger.info(f"Benchmark results saved to {filename}")

    # Download Methods
    @measure_time_and_resources
    def download_sync(self, is_monthly):
        url = self._get_url(is_monthly)
        logger.info(f"Downloading synchronously from {url}")
        response = requests.get(url, timeout=60)
        logger.info(f"Download completed, status code: {response.status_code}")
        return response.content

    @measure_time_and_resources
    def download_threaded(self, is_monthly):
        urls = self._get_urls(is_monthly)
        logger.info(f"Downloading {len(urls)} files using threads")
        with ThreadPoolExecutor() as executor:
            responses = list(executor.map(requests.get, urls))
        logger.info("Threaded download completed")
        return b"".join(resp.content for resp in responses)

    @measure_time_and_resources
    def download_async(self, is_monthly):
        async def fetch(session, url):
            async with session.get(url) as response:
                logger.info(f"Async download completed for {url}")
                return await response.read()

        async def main():
            urls = self._get_urls(is_monthly)
            logger.info(f"Starting async download of {len(urls)} files")
            async with aiohttp.ClientSession() as session:
                tasks = [fetch(session, url) for url in urls]
                responses = await asyncio.gather(*tasks)
            logger.info("Async download completed for all files")
            return b"".join(responses)

        return asyncio.run(main())

    @measure_time_and_resources
    def download_multiprocess(self, is_monthly):
        urls = self._get_urls(is_monthly)
        logger.info(f"Starting multiprocess download of {len(urls)} files")
        with ProcessPoolExecutor() as executor:
            responses = list(executor.map(requests.get, urls))
        logger.info("Multiprocess download completed")
        return b"".join(resp.content for resp in responses)

    @measure_time_and_resources
    def download_chunked(self, is_monthly):
        url = self._get_url(is_monthly)
        logger.info(f"Starting chunked download from {url}")
        response = requests.get(url, stream=True, timeout=60)
        chunks = []
        for i, chunk in enumerate(response.iter_content(chunk_size=1024 * 1024)):
            chunks.append(chunk)
            if i % 10 == 0:
                logger.info(f"Downloaded {i + 1}MB")
        logger.info("Chunked download completed")
        return b"".join(chunks)

    # Processing Methods
    @measure_time_and_resources
    def extract_and_process_in_memory(self, content):
        logger.info("Extracting and processing in memory")

        # Directly use the content string for CSV processing
        reader = csv.DictReader(io.StringIO(content))
        processed_data = [
            {
                "agg_trade_id": int(row["agg_trade_id"]),
                "price": float(row["price"]),
                "quantity": float(row["quantity"]),
                "first_trade_id": int(row["first_trade_id"]),
                "last_trade_id": int(row["last_trade_id"]),
                "transact_time": int(row["transact_time"]),
                "is_buyer_maker": row["is_buyer_maker"].lower() == "true",
            }
            for row in reader
        ]
        logger.info(f"Processed {len(processed_data)} rows in memory")
        return processed_data

    @measure_time_and_resources
    def extract_and_process_streaming(self, content):
        logger.info("Extracting and processing with streaming")
        processed_data = []
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            with zf.open(zf.namelist()[0]) as csv_file:
                reader = csv.DictReader(io.TextIOWrapper(csv_file, "utf-8"))
                for row in reader:
                    processed_row = {
                        "agg_trade_id": int(row["agg_trade_id"]),
                        "price": float(row["price"]),
                        "quantity": float(row["quantity"]),
                        "first_trade_id": int(row["first_trade_id"]),
                        "last_trade_id": int(row["last_trade_id"]),
                        "transact_time": int(row["transact_time"]),
                        "is_buyer_maker": row["is_buyer_maker"].lower() == "true",
                    }
                    processed_data.append(processed_row)
        logger.info(f"Processed {len(processed_data)} rows with streaming")
        return processed_data

    @measure_time_and_resources
    def extract_and_process_pandas(self, content):
        logger.info("Extracting and processing with pandas")
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            with zf.open(zf.namelist()[0]) as csv_file:
                df = pd.read_csv(csv_file)

        df["is_buyer_maker"] = df["is_buyer_maker"].astype(bool)
        processed_data = df.to_dict("records")
        logger.info(f"Processed {len(processed_data)} rows with pandas")
        return processed_data

    @measure_time_and_resources
    def extract_and_process_pyarrow(self, content):
        logger.info("Extracting and processing with pyarrow")
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            with zf.open(zf.namelist()[0]) as csv_file:
                table = pc.read_csv(csv_file)

        processed_data = table.to_pylist()
        logger.info(f"Processed {len(processed_data)} rows with pyarrow")
        return processed_data

    @measure_time_and_resources
    def extract_and_process_custom_yield(self, content):
        logger.info("Extracting and processing with custom yield")

        def process_csv(csv_content):
            for line in csv_content.split("\n")[1:]:
                if line:
                    row = line.split(",")
                    yield {
                        "agg_trade_id": int(row[0]),
                        "price": float(row[1]),
                        "quantity": float(row[2]),
                        "first_trade_id": int(row[3]),
                        "last_trade_id": int(row[4]),
                        "transact_time": int(row[5]),
                        "is_buyer_maker": row[6].lower() == "true",
                    }

        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            csv_content = zf.read(zf.namelist()[0]).decode("utf-8")

        processed_data = list(process_csv(csv_content))
        logger.info(f"Processed {len(processed_data)} rows with custom yield")
        return processed_data

    @measure_time_and_resources
    def extract_and_process_pyspark(self, content):
        logger.info("Extracting and processing with PySpark")

        schema = StructType(
            [
                StructField("agg_trade_id", IntegerType(), True),
                StructField("price", FloatType(), True),
                StructField("quantity", FloatType(), True),
                StructField("first_trade_id", IntegerType(), True),
                StructField("last_trade_id", IntegerType(), True),
                StructField("transact_time", IntegerType(), True),
                StructField("is_buyer_maker", BooleanType(), True),
            ]
        )

        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            with zf.open(zf.namelist()[0]) as csv_file:
                with tempfile.NamedTemporaryFile(mode="wb", delete=False) as temp_file:
                    temp_file.write(csv_file.read())
                    temp_file_path = temp_file.name

        df = self.spark.read.csv(temp_file_path, header=True, schema=schema)
        processed_data = df.collect()

        os.unlink(temp_file_path)

        logger.info(f"Processed {len(processed_data)} rows with PySpark")
        return processed_data

    # Upload Methods
    @measure_time_and_resources
    def upload_sync(self, data):
        logger.info("Starting synchronous upload to BigTable")
        rows = []
        for i, row_data in enumerate(data):
            if i % 100000 == 0:
                logger.info(f"Prepared {i} rows for upload")
            row_key = f"{self.symbol.replace('/', '')}#{self.year}-{self.month:02d}#{i}".encode()
            direct_row = row.DirectRow(row_key)
            for key, value in row_data.items():
                direct_row.set_cell("cf1", key, str(value).encode())
            rows.append(direct_row)

        if not rows:
            logger.warning("No rows to upload")
            return []

        logger.info(f"Uploading {len(rows)} rows to BigTable")
        errors = table.mutate_rows(rows)

        if errors:
            logger.error(f"Errors occurred while uploading to BigTable: {errors}")
        else:
            logger.info("Upload successful")

        return rows

    @measure_time_and_resources
    def upload_threaded(self, data):
        logger.info("Starting threaded upload to BigTable")

        def upload_chunk(chunk):
            return self.upload_sync(chunk)[0]

        chunks = [data[i : i + 10000] for i in range(0, len(data), 10000)][:10]
        logger.info(f"Prepared {len(chunks)} chunks for threaded upload")
        with ThreadPoolExecutor() as executor:
            results = list(executor.map(upload_chunk, chunks))
        total_rows = sum(len(chunk) for chunk in results)
        logger.info(f"Uploaded {total_rows} rows to BigTable using threads")
        return results

    @measure_time_and_resources
    def upload_async(self, data):
        logger.info("Starting asynchronous upload to BigTable")

        async def upload_chunk(chunk):
            return self.upload_sync(chunk)[0]

        async def main():
            chunks = [data[i : i + 10000] for i in range(0, len(data), 10000)][:10]
            logger.info(f"Prepared {len(chunks)} chunks for async upload")
            tasks = [upload_chunk(chunk) for chunk in chunks]
            results = await asyncio.gather(*tasks)
            total_rows = sum(len(chunk) for chunk in results)
            logger.info(f"Uploaded {total_rows} rows to BigTable asynchronously")
            return results

        return asyncio.run(main())

    @measure_time_and_resources
    def upload_batch(self, data):
        logger.info("Starting batch upload to BigTable")
        rows = []
        for i, row_data in enumerate(data[:100000]):
            if i % 100000 == 0:
                logger.info(f"Prepared {i} rows for batch upload")
            row_key = f"{self.symbol.replace('/', '')}#{self.year}-{self.month:02d}#{i}".encode()
            direct_row = row.DirectRow(row_key)
            for key, value in row_data.items():
                direct_row.set_cell("cf1", key, str(value).encode())
            rows.append(direct_row)

        logger.info(f"Uploading {len(rows)} rows to BigTable in batch")
        errors = table.mutate_rows(rows)
        if errors:
            logger.error(f"Errors occurred while uploading to BigTable: {errors}")
        else:
            logger.info("Batch upload successful")
        return rows

    def upload_chunk(self, chunk):
        return self.upload_sync(chunk)[0]

    @measure_time_and_resources
    def upload_multiprocess(self, data):
        logger.info("Starting multiprocess upload to BigTable")

        chunks = [data[i : i + 10000] for i in range(0, len(data), 10000)][:10]
        logger.info(f"Prepared {len(chunks)} chunks for multiprocess upload")
        with ProcessPoolExecutor() as executor:
            results = list(executor.map(self.upload_chunk, chunks))
        total_rows = sum(len(chunk) for chunk in results)
        logger.info(f"Uploaded {total_rows} rows to BigTable using multiprocessing")
        return results

    # Helper Methods
    def _get_url(self, is_monthly):
        symbol_url_part = self.symbol.replace("/", "")
        if is_monthly:
            return f"https://data.binance.vision/data/futures/um/monthly/aggTrades/{symbol_url_part}/{symbol_url_part}-aggTrades-{self.year}-{self.month:02d}.zip"
        else:
            return f"https://data.binance.vision/data/futures/um/daily/aggTrades/{symbol_url_part}/{symbol_url_part}-aggTrades-{self.year}-{self.month:02d}-01.zip"

    def _get_urls(self, is_monthly):
        if is_monthly:
            return [self._get_url(True)]
        else:
            days_in_month = 31 if self.month in [1, 3, 5, 7, 8, 10, 12] else 30
            if self.month == 2:
                days_in_month = (
                    29
                    if self.year % 4 == 0
                    and (self.year % 100 != 0 or self.year % 400 == 0)
                    else 28
                )
            return [
                self._get_url(False).replace("-01.zip", f"-{day:02d}.zip")
                for day in range(1, days_in_month + 1)
            ]


# Main Execution Logic
if __name__ == "__main__":
    symbol = "BTC/USDT"
    year = datetime.now().year
    month = datetime.now().month - 1

    print(f"Starting benchmark for {symbol} {year}-{month:02d}")
    runner = BenchmarkRunner(symbol, year, month)

    methods_summary = []
    for name, method in inspect.getmembers(
        BenchmarkRunner, predicate=inspect.isfunction
    ):
        if (
            name.startswith("download_")
            or name.startswith("extract_and_process_")
            or name.startswith("upload_")
        ):
            methods_summary.append([name])

    print("\nMethods to be benchmarked:")
    print(tabulate(methods_summary, headers=["Method"], tablefmt="grid"))

    proceed = input("\nDo you want to proceed with the benchmark? (y/n): ")
    if proceed.lower() != "y":
        print("Benchmark cancelled.")
        exit()

    logger.info("Starting benchmark tests")
    runner.run_benchmarks()

    logger.info("Benchmark completed")
