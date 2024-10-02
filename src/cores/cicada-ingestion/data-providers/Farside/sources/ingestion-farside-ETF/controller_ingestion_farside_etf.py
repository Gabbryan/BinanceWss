import re

import pandas as pd

from src.commons.env_manager.env_controller import EnvController
from src.commons.logs.logging_controller import LoggingController
from src.libs.third_services.farside_data_manager.ingestors.BTC_ETFIngestor import BitcoinETFIngestor
from src.libs.third_services.farside_data_manager.ingestors.ETH_ETFIngestor import EthereumETFIngestor
from src.libs.third_services.farside_data_manager.manager import FarsideDataManager
from src.libs.third_services.google.google_cloud_bucket.controller_gcs import GCSController
from src.libs.third_services.slack.controller_slack import SlackMessageController

logger = LoggingController("FarsideETFIngestor")


class ControllerIngestionFarsideETF:
    def __init__(self):
        self.env_manager = EnvController()
        self.slack_controller = SlackMessageController(self.env_manager.get_env("WEBHOOK_URL"))
        self.gcs_module = GCSController(self.env_manager.get_env("BUCKET_NAME"))
        pass

    def clean_parentheses_values(self, df):
        """
        This function cleans columns that might have parentheses indicating negative values
        and converts them into proper float values. It ensures that all columns except the 'Date' column are converted to floats.

        :param df: DataFrame to clean.
        :return: Cleaned DataFrame with numeric values in float format and the 'Date' column preserved.
        """
        # Define a regex pattern to match values with parentheses
        parentheses_pattern = re.compile(r'^\((.*)\)$')

        # List of columns to process (excluding 'Date' column)
        numeric_cols = df.columns[df.columns != 'Date']

        # Apply to numeric columns to handle values with parentheses and convert to float
        for col in numeric_cols:
            # Apply cleaning logic to handle parentheses and ensure float conversion
            df[col] = df[col].apply(lambda x: -float(parentheses_pattern.match(x).group(1))
            if isinstance(x, str) and parentheses_pattern.match(x)
            else float(x) if isinstance(x, (int, float, str)) and x.replace('.', '', 1).isdigit()
            else float(0))  # Convert invalid values to NaN

        # Ensure all numeric columns are of float type (excluding 'Date')
        df[numeric_cols] = df[numeric_cols].astype(float)

        # Convert the 'Date' column to timestamp format if it exists and ensure it is not affected by other processing
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')  # Convert 'Date' to datetime, coerce invalid values to NaT

        return df

    def send_start_message(self):
        message = (
            ":hourglass_flowing_sand: *Daily Data Ingestion Initiated*\n\n"
            "🚀 *Task Overview:*\n"
            "- *Ingestors*: `BitcoinETFIngestor`, `EthereumETFIngestor`\n"
            "- *Start Time*: `00:00`\n\n"
            "The daily data ingestion process has started. "
            "The system is now processing the specified data sources. "
            "You will receive a notification once the process is complete."
        )
        self.slack_controller.send_slack_message(
            ":hourglass_flowing_sand: Daily Data Ingestion Initiated", message, "#36a64f"
        )

    def send_end_message(self, total_ingestors, total_uploaded):
        message = (
            ":white_check_mark: *Daily Data Ingestion Completed*\n\n"
            "🎯 *Task Summary:*\n"
            f"- *Total Ingestors Processed*: `{total_ingestors}`\n"
            f"- *Total Datasets Uploaded*: `{total_uploaded}`\n\n"
            "All data ingestors have been successfully processed and the data has been uploaded to the cloud storage. "
            "Great job team! :clap: The system is ready for the next cycle."
        )
        self.slack_controller.send_slack_message(
            ":white_check_mark: Daily Data Ingestion Completed", message, "#36a64f"
        )

    def run_daily_tasks(self):
        # Initialize data manager
        manager = FarsideDataManager()

        # Send start message
        self.send_start_message()

        # Register ingestors (add all ingestors you want to use)
        manager.register_ingestor(BitcoinETFIngestor())
        manager.register_ingestor(EthereumETFIngestor())
        # Add other ingestors here as needed

        # Run the data manager
        manager.run()

        total_uploaded = 0

        # Process and upload data for each ingestor
        for ingestor in manager.ingestors:
            symbol = ingestor.__class__.__name__.replace("Ingestor", "")
            df = ingestor.data
            if df is not None and not df.empty:
                df["Year"] = df["Date"].dt.year
                df["Month"] = df["Date"].dt.month
                params = {
                    'symbol': symbol,
                    # Add +1 to include the last year and month in the range
                    'year_range': range(min(df["Year"]), max(df["Year"]) + 1),
                    'month_range': range(min(df["Month"]), max(df["Month"]) + 1)
                }
                template = "Raw/farside/{symbol}/{year}/{month:02d}/data.parquet"
                paths = self.gcs_module.generate_gcs_paths(params, template)

                # For each GCS path
                for gcs_path in paths:
                    # Filter DataFrame by year and month
                    year = int(gcs_path.split('/')[-3])  # Extract the year from the path
                    month = int(gcs_path.split('/')[-2])  # Extract the month from the path

                    # Filter DataFrame by the year and month
                    month_df = df[(df["Year"] == year) & (df["Month"] == month)].drop(columns=["Year", "Month"])
                    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')  # Coerce invalid dates to NaT
                    cleaned_month_df = self.clean_parentheses_values(month_df)
                    if not cleaned_month_df.empty:
                        try:
                            self.gcs_module.upload_dataframe_to_gcs(cleaned_month_df, gcs_path)
                            total_uploaded += 1
                        except Exception as e:
                            logger.log_error(f"Error processing {symbol} {year}-{month:02d}: {e}", context={'mod': 'GCSController', 'action': 'ErrorInUpload'})
            else:
                logger.log_warning(f"No data to process for symbol: {symbol}", context={'mod': 'GCSController', 'action': 'NoDataToProcess'})

        # Send end message
        self.send_end_message(len(manager.ingestors), total_uploaded)
