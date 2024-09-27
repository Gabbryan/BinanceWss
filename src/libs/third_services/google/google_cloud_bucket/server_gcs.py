from google.cloud import storage

from src.commons.logs.logging_controller import LoggingController

# Initialize the logging controller
logger = LoggingController()

class GCSClient:
    def __init__(self, bucket_name):
        """
        Initialize the Google Cloud Storage client.

        :param bucket_name: The name of the GCS bucket.
        """
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(bucket_name)
        logger.log_info(f"Initialized GCS client for bucket: {bucket_name}", context={'mod': 'GCSClient', 'action': 'Init'})

    def upload_file(self, source_file_path, destination_blob_name):
        """
        Upload a file to the specified GCS path.

        :param source_file_path: The local path of the file to upload.
        :param destination_blob_name: The destination GCS path (blob) in the bucket.
        """
        try:
            blob = self.bucket.blob(destination_blob_name)
            blob.upload_from_filename(source_file_path)
            logger.log_info(f"Uploaded file {source_file_path} to GCS path {destination_blob_name}", context={'mod': 'GCSClient', 'action': 'UploadFile'})
        except Exception as e:
            logger.log_error(f"Failed to upload file {source_file_path} to {destination_blob_name}: {e}",
                             context={'mod': 'GCSClient', 'action': 'UploadFileError'})

    def delete_file(self, file_path):
        """
        Delete a file from GCS.

        :param file_path: The GCS path of the file to delete.
        """
        try:
            blob = self.bucket.blob(file_path)
            blob.delete()
            logger.log_info(f"Deleted file from GCS path: {file_path}", context={'mod': 'GCSClient', 'action': 'DeleteFile'})
        except Exception as e:
            logger.log_error(f"Failed to delete file {file_path} from GCS: {e}", context={'mod': 'GCSClient', 'action': 'DeleteFileError'})
