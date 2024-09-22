from google.cloud import storage


class GCSClient:
    def __init__(self, bucket_name):
        """
        Initialize the Google Cloud Storage client.

        :param bucket_name: The name of the GCS bucket.
        """
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(bucket_name)

    def upload_file(self, source_file_path, destination_blob_name):
        """
        Upload a file to the specified GCS path.

        :param source_file_path: The local path of the file to upload.
        :param destination_blob_name: The destination GCS path (blob) in the bucket.
        """
        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_path)

    def delete_file(self, file_path):
        """
        Delete a file from GCS.

        :param file_path: The GCS path of the file to delete.
        """
        blob = self.bucket.blob(file_path)
        blob.delete()
