import re

import pandas as pd

from src.commons.env_manager.env_controller import EnvController
from src.commons.logs.logging_controller import LoggingController
from src.libs.third_services.farside_data_manager.ingestors.BTC_ETFIngestor import BitcoinETFIngestor
from src.libs.third_services.farside_data_manager.ingestors.ETH_ETFIngestor import EthereumETFIngestor
from src.libs.third_services.farside_data_manager.manager import FarsideDataManager
from src.libs.third_services.google.google_cloud_bucket.controller_gcs import GCSController
from src.libs.utils.dataframe.validation.df_verification_controller import DataFrameVerificationController


class ControllerIngestionFarsideETF:
    def __init__(self):
        self.env_manager = EnvController()
        self.gcs_module = GCSController(self.env_manager.get_env("BUCKET_NAME"))
        self.logger = LoggingController("ControllerIngestionFarsideETF")
        self.logger.log_info("ControllerIngestionFarsideETF initialized.", context={'mod': 'ControllerIngestionFarsideETF', 'action': 'Initialize'})

    def clean_parentheses_values(self, df):
        parentheses_pattern = re.compile(r'^\((.*)\)$')
        numeric_cols = df.columns[df.columns != 'Date']

        for col in numeric_cols:
            df[col] = df[col].apply(lambda x: -float(parentheses_pattern.match(x).group(1))
            if isinstance(x, str) and parentheses_pattern.match(x)
            else float(x) if isinstance(x, (int, float, str)) and x.replace('.', '', 1).isdigit()
            else float(0))

        df[numeric_cols] = df[numeric_cols].astype(float)
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        self.logger.log_info("Data cleaned of parentheses values.", context={'mod': 'ControllerIngestionFarsideETF', 'action': 'CleanParenthesesValues'})
        return df

    def run_daily_tasks(self):
        manager = FarsideDataManager()

        # Register and run ingestors
        manager.register_ingestor(BitcoinETFIngestor())
        manager.register_ingestor(EthereumETFIngestor())
        manager.run()

        # Define configurations for Bitcoin and Ethereum DataFrames
        config_bitcoin = {
            'required_columns': ["Date", "IBIT", "FBTC", "BITB", "ARKB", "BTCO", "EZBC", "BRRR", "HODL", "BTCW", "GBTC", "BTC", "Total"],
            'data_type_checks': {"IBIT": "float64", "FBTC": "float64", "BITB": "float64"}
        }
        config_ethereum = {
            'required_columns': ["Date", "ETHA", "FETH", "ETHW", "CETH", "ETHV", "QETH", "EZET", "ETHE", "ETH"],
            'data_type_checks': {
                "ETHA": "float64", "FETH": "float64", "ETHW": "float64", "CETH": "float64",
                "ETHV": "float64", "QETH": "float64", "EZET": "float64", "ETHE": "float64", "ETH": "float64"
            }
        }


        for ingestor in manager.ingestors:
            symbol = ingestor.__class__.__name__.replace("Ingestor", "")
            df = ingestor.data

            # Further processing
            df["Year"] = df["Date"].dt.year
            df["Month"] = df["Date"].dt.month
            params = {
                'symbol': symbol,
                'year_range': range(min(df["Year"]), max(df["Year"]) + 1),
                'month_range': range(min(df["Month"]), max(df["Month"]) + 1)
            }
            template = "Raw/farside/{symbol}/{year}/{month:02d}/data.parquet"
            paths = self.gcs_module.generate_gcs_paths(params, template)

            for gcs_path in paths:
                year = int(gcs_path.split('/')[-3])
                month = int(gcs_path.split('/')[-2])
                month_df = df[(df["Year"] == year) & (df["Month"] == month)].drop(columns=["Year", "Month"])
                cleaned_month_df = self.clean_parentheses_values(month_df)
                # Determine configuration based on the symbol
                if symbol == "BitcoinETF":
                    verification_controller = DataFrameVerificationController(**config_bitcoin)
                elif symbol == "EthereumETF":
                    verification_controller = DataFrameVerificationController(**config_ethereum)
                else:
                    self.logger.log_warning(f"No configuration found for symbol: {symbol}", context={'mod': 'ControllerIngestionFarsideETF', 'action': 'NoConfigFound'})
                    continue

                if df is not None and not df.empty:
                    # Apply verification and transformation
                    cleaned_month_df = verification_controller.validate_and_transform_dataframe(cleaned_month_df, clean_data=True, context={'symbol': symbol})

                if not cleaned_month_df.empty:
                            try:
                                self.gcs_module.upload_dataframe_to_gcs(cleaned_month_df, gcs_path)
                                self.logger.log_info(f"Uploaded data for {symbol} {year}-{month:02d} to GCS.", context={'mod': 'GCSController', 'action': 'UploadToGCS'})
                            except Exception as e:
                                self.logger.log_error(f"Error uploading {symbol} data for {year}-{month:02d}: {e}", context={'mod': 'GCSController', 'action': 'UploadError'})
                else:
                    self.logger.log_warning(f"No data available to process for symbol: {symbol}", context={'mod': 'ControllerIngestionFarsideETF', 'action': 'NoData'})
