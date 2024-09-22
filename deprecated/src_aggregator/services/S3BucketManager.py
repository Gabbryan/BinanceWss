import boto3


class S3BucketManager:
    def __init__(self, access_key_id, secret_access_key, region_name, bucket_name):
        self.s3 = boto3.client(
            "s3",
            region_name=region_name,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            config=boto3.session.Config(max_pool_connections=50),
        )
        self.bucket_name = bucket_name

    def list_files(self, prefix=""):
        paginator = self.s3.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

        files = []
        for page in page_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    files.append(obj["Key"])
        return files

    def upload_file(self, file_path, object_name):
        """Upload a file to an S3 bucket"""
        try:
            self.s3.upload_file(file_path, self.bucket_name, object_name)
            print(f"File {file_path} uploaded to {object_name}")
        except boto3.exceptions.S3UploadFailedError as e:
            print(f"Failed to upload {file_path}. Error: {e}")
            raise

    def download_file(self, object_name, file_path):
        """Download a file from an S3 bucket"""
        try:
            self.s3.download_file(self.bucket_name, object_name, file_path)
            print(f"File {object_name} downloaded to {file_path}")
        except Exception as e:
            print(f"Failed to download {object_name}. Error: {e}")
            raise
