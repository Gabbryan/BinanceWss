from src.commons.env_manager.env_controller import EnvController
from src.commons.logs.logging_controller import LoggingController
from src.libs.utils.sys.threading.controller_threading import ThreadController
from src.libs.third_services.google.google_cloud_bucket.controller_gcs import GCSController

# TODO : INTERFACER ?
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import zipfile
from io import BytesIO
from datetime import datetime, timedelta



class BinanceDataVision:
    def __init__(self, bucket_name, timeframes, request_limit=5, request_interval=1):
        self.timeframes = timeframes
        self.request_limit = request_limit
        self.request_interval = request_interval
        self.session = self._configure_session()
        self.EnvController = EnvController()
        self.logger = LoggingController()
        self.GCSController = GCSController(bucket_name=self.EnvController.get_yaml_config('DBV_bucket_name'))
        self.symbols = self.EnvController.get_yaml_config('symbols')
        self.stream = self.EnvController.get_yaml_config('stream')
    
    def _configure_session(self):
        # Configure session with retries and increased connection pool size
        session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
        session.mount("https://", adapter)
        return session
    
    # def download_and_extract(self, url, date):
    #     """Download and extract a ZIP file from the given URL."""
    #     try:
    #         response = self.session.get(url, stream=True, timeout=60)
    #         response.raise_for_status()
    #         with zipfile.ZipFile(BytesIO(response.content)) as z:
    #             with z.open(z.namelist()[0]) as file:
    #                 df = pd.read_csv(file)
    #         return df
    #     except Exception as e:
    #         self.logger.error(f"Error downloading or extracting {url}: {e}")
    #         return None

    # def process_tasks(self, tasks):
    #     existing_files_count = 0
    #     total_size = 0
    #     file_info_list = []

    #     for task in tqdm(tasks):
    #         base_url, symbol, timeframe, date = task
    #         gcs_key = self._generate_gcs_key(symbol, timeframe, date)
            
    #         if self.gcs_module.check_gcs_object_exists(self.gcs_module.bucket, gcs_key):
    #             self.logger.info(f"File {gcs_key} exists in GCS. Skipping.")
    #             existing_files_count += 1
    #             continue

    #         df = self.download_and_extract(base_url, date)
    #         if df is not None:
    #             local_file_path = self._save_to_parquet(df, date)
    #             if os.path.exists(local_file_path):
    #                 file_size = os.path.getsize(local_file_path)
    #                 total_size += file_size
    #                 self.gcs_module.upload_to_gcs(pd.read_parquet(local_file_path), symbol, date.year, date.month, self.gcs_module.bucket, gcs_key)
    #                 file_info_list.append({"url": gcs_key, "size": file_size})
    #         time.sleep(self.request_interval / self.request_limit)
    #     return file_info_list, existing_files_count, total_size

    # def _generate_gcs_key(self, symbol, timeframe, date):
    #     formatted_date = date.strftime("%Y-%m-%d")
    #     base_type = "futures" if "futures" in symbol else "spot"
    #     gcs_key = f"Raw/Binance-data-vision/historical/{symbol.replace('/', '')}/{base_type}/klines/{timeframe}/{date.year}/{date.month:02d}/{date.day:02d}/data.parquet"
    #     return gcs_key

    # def _save_to_parquet(self, df, date):
    #     local_dir = f"{date.year}/{date.month:02d}/{date.day:02d}"
    #     if not os.path.exists(local_dir):
    #         os.makedirs(local_dir)
    #     local_file_path = os.path.join(local_dir, "data.parquet")
    #     df.to_parquet(local_file_path)
    #     return local_file_path

    # def download_and_process_data(self, start_date=None, end_date=None):
    #     if start_date is None:
    #         start_date = datetime.today() - timedelta(days=1)
    #     if end_date is None:
    #         end_date = datetime.today() - timedelta(days=1)

    #     dates = pd.date_range(start_date, end_date, freq="D")
    #     tasks = list(itertools.product(self.symbols, self.timeframes, dates))
        
    #     with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
    #         futures = {executor.submit(self.process_tasks, tasks): "task_batch"}
    #         for future in as_completed(futures):
    #             try:
    #                 file_info_list, existing_files_count, total_size = future.result()
    #                 self.logger.info(f"Processed batch with {len(file_info_list)} new files.")
    #             except Exception as e:
    #                 self.logger.error(f"Error in processing tasks: {e}")

    # def schedule_job(self, job_time="10:00"):
    #     schedule.every().day.at(job_time).do(self.download_and_process_data)
    #     while True:
    #         schedule.run_pending()
    #         time.sleep(60)