import os

import pandas as pd

from server_gcs import GCSClient


class GCSController:
    def __init__(self, bucket_name):
        """
        Initialize the GCS controller to manage high-level operations.

        :param bucket_name: The name of the GCS bucket.
        """
        self.gcs_client = GCSClient(bucket_name)

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
        gcs_paths = []
        for year in range(int(start_year), int(end_year) + 1):
            month_start = int(start_month) if year == int(start_year) else 1
            month_end = int(end_month) if year == int(end_year) else 12
            for month in range(month_start, month_end + 1):
                gcs_path = f"Raw/farside/{symbol}/{year}/{month:02d}/data.parquet"
                gcs_paths.append((year, month, gcs_path))
        return gcs_paths

    def aggregate_and_upload(self, df, symbol):
        """
        Aggregate the DataFrame data and upload it to GCS in monthly chunks.

        :param df: The pandas DataFrame containing the data.
        :param symbol: The symbol representing the data.
        """
        if df is not None and not df.empty:
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
                month_df = df[(df["Year"] == year) & (df["Month"] == month)].drop(
                    columns=["Year", "Month"]
                )
                if not month_df.empty:
                    # Generate temp local file
                    temp_local_file = f"/tmp/{symbol.replace('/', '')}_{year}_{month:02d}.parquet"

                    # Save to parquet
                    month_df.to_parquet(temp_local_file, index=False)

                    # Upload to GCS
                    self.gcs_client.upload_file(temp_local_file, gcs_path)

                    # Remove temp file
                    os.remove(temp_local_file)
        else:
            return  # No data to process
