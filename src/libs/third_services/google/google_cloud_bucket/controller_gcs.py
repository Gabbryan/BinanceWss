import os
import tempfile

from src.commons.logs.logging_controller import LoggingController
from src.libs.third_services.google.google_cloud_bucket.server_gcs import GCSClient

# Initialize the logging controller
logger = LoggingController("GCSController")


class GCSController:
    def __init__(self, bucket_name):
        """
        Initialize the GCS controller to manage high-level operations.

        :param bucket_name: The name of the GCS bucket.
        """
        self.gcs_client = GCSClient(bucket_name)
        self.bucket_name = bucket_name
        logger.log_info(f"GCS Controller initialized with bucket: {bucket_name}", context={'mod': 'GCSController', 'action': 'Init'})

    def list_files(self, prefix, folder_name=None):
        """
        List all files in the GCS bucket recursively under a given prefix (folder).

        :param prefix: The root path in the GCS bucket.
        :param folder_name: (Optional) The folder name to include in the file paths (e.g., 'klines').
        :return: A list of file paths that include the specified folder.
        """
        try:
            logger.log_info(f"Listing files in GCS bucket {self.bucket_name} under prefix: {prefix}",
                            context={'mod': 'GCSClient', 'action': 'ListFiles'})

            blobs_iterator = self.gcs_client.bucket.list_blobs(prefix=prefix)

            blobs = list(blobs_iterator)

            # Filter files that contain the folder_name (if provided)
            if folder_name:
                file_list = [blob.name for blob in blobs if f"/{folder_name}/" in blob.name and not blob.name.endswith('/')]
            else:
                file_list = [blob.name for blob in blobs if not blob.name.endswith('/')]

            logger.log_info(f"Found {len(file_list)} files under {prefix} that contain folder: {folder_name}",
                            context={'mod': 'GCSClient', 'action': 'FilesListed'})
            return file_list
        except Exception as e:
            logger.log_error(f"Error listing files in GCS: {e}", context={'mod': 'GCSClient', 'action': 'ListFilesError'})
            return []

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
            if self.check_file_exists(gcs_path):
                logger.log_info(f"File {gcs_path} already exists. Skipping upload.", context={'mod': 'GCSController', 'action': 'FileExists'})
                return

            temp_file = self._save_data_to_temp(df, file_format)
            self._upload_to_gcs(temp_file, gcs_path)
            logger.log_info(f"Uploaded DataFrame to {gcs_path} in GCS.",
                            context={'mod': 'GCSController', 'action': 'DFUploadSuccess'})

        except Exception as e:
            logger.log_error(f"Error uploading DataFrame to GCS: {e}",
                             context={'mod': 'GCSController', 'action': 'DFUploadError'})
            raise

        finally:
            if temp_file is not None:
                self._remove_temp_file(temp_file)
