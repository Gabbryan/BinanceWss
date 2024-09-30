import os

import pandas as pd
from src.libs.third_services.google.google_cloud_bucket.server_gcs import GCSClient
from src.commons.logs.logging_controller import LoggingController

# Initialize the logging controller
logger = LoggingController()

class GCSController:
    def __init__(self, bucket_name):
        """
        Initialize the GCS controller to manage high-level operations.

        :param bucket_name: The name of the GCS bucket.
        """
        self.gcs_client = GCSClient(bucket_name)
        logger.log_info(f"GCS Controller initialized with bucket: {bucket_name}", context={'mod': 'GCSController', 'action': 'Init'})

    def generate_gcs_paths(self, symbol, start_year, end_year, start_month, end_month):
        """
        Generate the GCS paths for uploading files.

        :param symbol: The symbol representing the data.
        :param start_year: The starting year of the data.
        :param end_year: The ending year of the data.
        :param start_month: The starting month of the data.
        :param end_month: The ending month of the data.
        :return: A list of tuples containing (year, month, GCS path).
        """
        logger.log_info(f"Generating GCS paths for symbol: {symbol} from {start_year}-{start_month} to {end_year}-{end_month}",
                        context={'mod': 'GCSController', 'action': 'GeneratePaths'})

        gcs_paths = []
        for year in range(int(start_year), int(end_year) + 1):
            month_start = int(start_month) if year == int(start_year) else 1
            month_end = int(end_month) if year == int(end_year) else 12
            for month in range(month_start, month_end + 1):
                gcs_path = f"Raw/farside/{symbol}/{year}/{month:02d}/data.parquet"
                gcs_paths.append((year, month, gcs_path))
        logger.log_info(f"Generated {len(gcs_paths)} GCS paths.", context={'mod': 'GCSController', 'action': 'GeneratePathsComplete'})
        return gcs_paths

    def aggregate_and_upload(self, df, symbol):
        """
        Aggregate the DataFrame data and upload it to GCS in monthly chunks.

        :param df: The pandas DataFrame containing the data.
        :param symbol: The symbol representing the data.
        """
        if df is not None and not df.empty:
            logger.log_info(f"Starting aggregation and upload for symbol: {symbol}", context={'mod': 'GCSController', 'action': 'StartAggregateUpload'})

            # Ensure 'Date' column is in datetime format
            df["Date"] = pd.to_datetime(df["Date"], format="%d %b %Y", errors="coerce")
            df = df.dropna(subset=["Date"])

            # Get min and max dates
            min_date = df["Date"].min()
            max_date = df["Date"].max()

            # Determine start and end year/month
            start_year = min_date.year
            start_month = min_date.month
            end_year = max_date.year
            end_month = max_date.month

            # Generate paths
            paths = self.generate_gcs_paths(symbol, start_year, end_year, start_month, end_month)

            # Group by year and month
            df["Year"] = df["Date"].dt.year
            df["Month"] = df["Date"].dt.month

            for year, month, gcs_path in paths:
                month_df = df[(df["Year"] == year) & (df["Month"] == month)].drop(columns=["Year", "Month"])
                if not month_df.empty:
                    try:
                        # Generate temp local file
                        temp_local_file = f"/tmp/{symbol.replace('/', '')}_{year}_{month:02d}.parquet"

                        # Save to parquet
                        month_df.to_parquet(temp_local_file, index=False)
                        logger.log_info(f"Saved DataFrame for {symbol} {year}-{month:02d} to {temp_local_file}",
                                        context={'mod': 'GCSController', 'action': 'SaveToParquet'})

                        # Upload to GCS
                        self.gcs_client.upload_file(temp_local_file, gcs_path)
                        logger.log_info(f"Uploaded {temp_local_file} to GCS path {gcs_path}", context={'mod': 'GCSController', 'action': 'UploadToGCS'})

                        # Remove temp file
                        os.remove(temp_local_file)
                        logger.log_info(f"Removed temporary file {temp_local_file}", context={'mod': 'GCSController', 'action': 'RemoveTempFile'})

                    except Exception as e:
                        logger.log_error(f"Error processing {symbol} {year}-{month:02d}: {e}", context={'mod': 'GCSController', 'action': 'ErrorInUpload'})
        else:
            logger.log_warning(f"No data to process for symbol: {symbol}", context={'mod': 'GCSController', 'action': 'NoDataToProcess'})
            return  # No data to process
