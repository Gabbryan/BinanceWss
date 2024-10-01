import os
import tempfile

from src.commons.logs.logging_controller import LoggingController
from src.libs.third_services.google.google_cloud_bucket.server_gcs import GCSClient

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

    def _generate_temp_file(self, file_format='parquet'):
        """
        Generate a secure temporary file with the specified file format.

        :param file_format: The file extension (e.g., 'parquet', 'csv')
        :return: The path to the temporary file.
        """
        suffix = f'.{file_format}'
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
            return temp_file.name

    def _save_data_to_temp(self, data, file_format='parquet'):
        """
        Save data to a temporary file in the specified format.

        :param data: The data to be saved (pandas DataFrame, etc.).
        :param file_format: The file format to save (default is 'parquet').
        :return: The path to the saved temporary file.
        """
        temp_file = self._generate_temp_file(file_format)

        # Handle saving based on file format
        if file_format == 'parquet':
            data.to_parquet(temp_file, index=False)
        elif file_format == 'csv':
            data.to_csv(temp_file, index=False)
        elif file_format == 'xlsx':
            data.to_excel(temp_file, index=False)
        elif file_format == 'json':
            data.to_json(temp_file, orient='records', lines=True)
        elif file_format == 'pickle':
            data.to_pickle(temp_file)
        elif file_format == 'feather':
            data.to_feather(temp_file)
        elif file_format == 'hdf':
            data.to_hdf(temp_file, key='data', mode='w')
        elif file_format == 'stata':
            data.to_stata(temp_file, write_index=False)
        elif file_format == 'html':
            data.to_html(temp_file, index=False)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        logger.log_info(f"Data saved to temporary file: {temp_file}", context={'mod': 'GCSController', 'action': 'SaveData'})
        return temp_file

    def _upload_to_gcs(self, local_file, gcs_path):
        """
        Upload a file to GCS and handle cleanup.

        :param local_file: The local file path.
        :param gcs_path: The GCS destination path.
        """
        try:
            # Upload the file to GCS
            self.gcs_client.upload_file(local_file, gcs_path)
            logger.log_info(f"Uploaded {local_file} to GCS path {gcs_path}", context={'mod': 'GCSController', 'action': 'UploadToGCS'})
        except Exception as e:
            logger.log_error(f"Error uploading file to GCS: {e}", context={'mod': 'GCSController', 'action': 'UploadError'})
            raise

    def _remove_temp_file(self, temp_file):
        """
        Remove the temporary file after it has been uploaded to GCS.

        :param temp_file: The path to the temporary file to remove.
        """
        try:
            os.remove(temp_file)
            logger.log_info(f"Removed temporary file: {temp_file}", context={'mod': 'GCSController', 'action': 'RemoveTempFile'})
        except Exception as e:
            logger.log_error(f"Error removing temporary file: {e}", context={'mod': 'GCSController', 'action': 'RemoveTempFileError'})

    def generate_gcs_paths(self, parameters, template, file_format="parquet"):
        """
        Generate GCS paths based on a flexible template.

        :param parameters: A dictionary containing the values to populate the template (e.g., {'symbol': 'BTC', 'year_range': range(2023, 2024)}).
        :param template: A string template for generating the GCS path. Use placeholders like {symbol}, {year}, {month}, etc.
        :param file_format: The file format to be used in the path (default is 'parquet').
        :return: A list of GCS paths with placeholders replaced by the actual parameter values.
        """
        logger.log_info(f"Generating GCS paths with template: {template} and parameters: {parameters}",
                        context={'mod': 'GCSController', 'action': 'GeneratePaths'})

        # Initialize paths list
        gcs_paths = []

        # Expand the parameters to generate paths
        year_range = parameters.get('year_range', [])
        month_range = parameters.get('month_range', [])

        # If both year_range and month_range are provided, create paths with year and month placeholders
        if year_range and month_range:
            for year in year_range:
                for month in month_range:
                    # Ensure year and month are integers
                    path = template.format(symbol=parameters['symbol'], year=int(year), month=int(month), file_format=file_format)
                    gcs_paths.append(path)
        else:
            # Generate path without year/month if they are not provided
            path = template.format(symbol=parameters['symbol'], year='', month='', file_format=file_format)
            gcs_paths.append(path)

        logger.log_info(f"Generated {len(gcs_paths)} GCS paths.", context={'mod': 'GCSController', 'action': 'GeneratePathsComplete'})
        return gcs_paths

    def upload_dataframe_to_gcs(self, df, gcs_path, file_format='parquet'):
        """
        Save a pandas DataFrame as a Parquet file and upload it to GCS.

        :param df: The pandas DataFrame to be saved and uploaded.
        :param gcs_path: The destination path in the GCS bucket.
        :param file_format: The file format to save the DataFrame (default is 'parquet').
        """
        logger.log_info(f"Starting DataFrame upload to GCS as {file_format} format.",
                        context={'mod': 'GCSController', 'action': 'StartDFUpload'})
        temp_file = None
        try:
            # Save DataFrame to a temporary file
            temp_file = self._save_data_to_temp(df, file_format)

            # Upload the Parquet file to GCS
            self._upload_to_gcs(temp_file, gcs_path)
            logger.log_info(f"Uploaded DataFrame to {gcs_path} in GCS.",
                            context={'mod': 'GCSController', 'action': 'DFUploadSuccess'})

        except Exception as e:
            logger.log_error(f"Error uploading DataFrame to GCS: {e}",
                             context={'mod': 'GCSController', 'action': 'DFUploadError'})
            raise

        finally:
            # Remove the temporary file after upload
            if temp_file is not None:
                self._remove_temp_file(temp_file)
