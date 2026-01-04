from abc import ABC, abstractmethod
import uuid
import os
import io
from typing import Optional
import boto3
from botocore.exceptions import ClientError

class FileStorageService(ABC):
    @abstractmethod
    async def save_file(self, file_bytes: bytes, extension: str = ".bin", file_key: Optional[str] = None) -> str:
        """Save a file and return its storage path or URL.

        Args:
            file_bytes: The content of the file in bytes.
            extension: The file extension (e.g., ".bin", ".npy").
            file_key: Optional specific key/path for the file. If not provided, generates a UUID.

        Returns:
            The storage path or URL of the saved file.
        """
        pass

    @abstractmethod
    async def get_file(self, file_path: str) -> bytes:
        """Retrieve a file from storage.

        Args:
            file_path: The storage path or key of the file.

        Returns:
            The file content in bytes.
        """
        pass

class S3FileStorageService(FileStorageService):
    """S3-compatible storage service (works with AWS S3, MinIO, etc.)"""

    def __init__(
        self,
        bucket_name: str,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: str = "us-east-1"
    ):
        """
        Initialize S3 storage service.

        Args:
            bucket_name: Name of the S3 bucket
            endpoint_url: Optional endpoint URL for S3-compatible services (e.g., MinIO)
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            region_name: AWS region name
        """
        self.bucket_name = bucket_name

        self.s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

        # Ensure bucket exists
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        """Create bucket if it doesn't exist."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                # Bucket doesn't exist, create it
                try:
                    self.s3_client.create_bucket(Bucket=self.bucket_name)
                    print(f"Created S3 bucket: {self.bucket_name}")
                except ClientError as create_error:
                    print(f"Error creating bucket: {create_error}")
            else:
                print(f"Error checking bucket: {e}")

    async def save_file(self, file_bytes: bytes, extension: str = ".bin", file_key: Optional[str] = None) -> str:
        """
        Save file to S3 bucket.

        Args:
            file_bytes: The content of the file in bytes.
            extension: The file extension.
            file_key: Optional specific key for the file. If not provided, generates a UUID.

        Returns:
            The S3 key (path) of the saved file.
        """
        if not extension.startswith("."):
            extension = f".{extension}"

        if file_key is None:
            file_key = f"{str(uuid.uuid4())}{extension}"
        elif not file_key.endswith(extension):
            file_key = f"{file_key}{extension}"

        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=file_key,
                Body=file_bytes
            )
            return f"s3://{self.bucket_name}/{file_key}"
        except ClientError as e:
            raise Exception(f"Error uploading file to S3: {e}")

    async def get_file(self, file_path: str) -> bytes:
        """
        Retrieve file from S3 bucket.

        Args:
            file_path: The S3 path (s3://bucket/key or just key).

        Returns:
            The file content in bytes.
        """
        # Extract key from s3:// URL if provided
        if file_path.startswith("s3://"):
            parts = file_path.replace("s3://", "").split("/", 1)
            if len(parts) == 2:
                bucket, key = parts
                if bucket != self.bucket_name:
                    raise ValueError(f"Bucket mismatch: {bucket} != {self.bucket_name}")
                file_key = key
            else:
                raise ValueError(f"Invalid S3 path: {file_path}")
        else:
            file_key = file_path

        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
            return response['Body'].read()
        except ClientError as e:
            raise Exception(f"Error retrieving file from S3: {e}")

class MockFileStorageService(FileStorageService):
    """Mock file storage service for local development."""

    def __init__(self, base_path: str = "mock_storage"):
        self.base_path = base_path
        os.makedirs(self.base_path, exist_ok=True)

    async def save_file(self, file_bytes: bytes, extension: str = ".bin", file_key: Optional[str] = None) -> str:
        """Save file to local filesystem."""
        if not extension.startswith("."):
            extension = f".{extension}"

        if file_key is None:
            file_key = str(uuid.uuid4())

        # Remove extension from file_key if it's already there
        if file_key.endswith(extension):
            filename = file_key
        else:
            filename = f"{file_key}{extension}"

        file_path = os.path.join(self.base_path, filename)

        # Create subdirectories if needed
        os.makedirs(os.path.dirname(file_path) if os.path.dirname(file_path) else self.base_path, exist_ok=True)

        with open(file_path, "wb") as f:
            f.write(file_bytes)

        return file_path

    async def get_file(self, file_path: str) -> bytes:
        """Retrieve file from local filesystem."""
        with open(file_path, "rb") as f:
            return f.read()