# -*- coding: utf-8 -*-
import hashlib
from os.path import basename
from datetime import datetime

from rich.prompt import Prompt

from .S3Auth import S3Auth
from rich import print

class S3Exception(Exception): ...

class S3Wrapper:
    """
    Wrapper class for interacting with AWS S3.
    """

    def __init__(
            self,
            bucket_name: str = 'conversion-testing-files',
            region: str = 'us-east-1',
            access_key: str = None,
            secret_access_key: str = None
    ):
        """
        Initialize the S3Wrapper.
        :param bucket_name: Name of the S3 bucket.
        :param region: AWS region.
        :param access_key: AWS access key.
        :param secret_access_key: AWS secret access key.
        """
        self.s3 = S3Auth(region, access_key, secret_access_key).client
        self.bucket = self._check_bucket_name(bucket_name)

    def get_files(self, s3_dir: str = None) -> list:
        """
        Get a list of files in the S3 bucket.
        :param s3_dir: Optional directory path in the S3 bucket.
        :return: List of file names.
        """
        s3_objects = self.get_objects()
        if isinstance(s3_dir, str):
            return [file for file in s3_objects if file.startswith(s3_dir) and not file.endswith('/')]
        return [file for file in s3_objects if not file.endswith('/')]

    def get_objects(self) -> list:
        """
        Get a list of all objects in the S3 bucket.
        :return: List of object keys.
        """
        file_names = []
        for page in self.s3.get_paginator('list_objects_v2').paginate(Bucket=self.bucket):
            if 'Contents' in page:
                file_names.extend([obj['Key'] for obj in page['Contents']])
        if not file_names:
            print("[red]|INFO| Bucket is empty.")
        return file_names

    def download(self, object_key: str, download_path: str, stdout: bool = True) -> bool:
        """
        Download an object from the S3 bucket to a local path.
        :param object_key: Key of the object in the S3 bucket.
        :param download_path: Local path to download the object.
        :param stdout: Whether to print download information.
        :return: True if download is successful, False otherwise.
        """
        if stdout:
            print(f"[green]|INFO| Downloading [cyan]{self.bucket}/{object_key}[/] to [cyan]{download_path}[/]")

        try:
            self.s3.download_file(self.bucket, object_key, download_path)
            return True
        except self.s3.exceptions.ClientError:
            print(f"[red]|ERROR| Object {object_key} not found.")
            return False

    def upload(self, file_path: str, object_key: str, stdout: bool = True, metadata: dict = None) -> None:
        """
        Upload a file to the S3 bucket.
        :param file_path: Local path of the file to upload.
        :param object_key: Key of the object in the S3 bucket.
        :param stdout: Whether to print upload information.
        :param metadata: Dictionary of metadata to attach to the object.
        """
        if stdout:
            print(f"[green]|INFO| Uploading [cyan]{basename(file_path)}[/] to [cyan]{self.bucket}/{object_key}[/]")

        extra_args = {}
        if metadata:
            extra_args['Metadata'] = metadata

        self.s3.upload_file(file_path, self.bucket, object_key, ExtraArgs=extra_args if extra_args else None)

    def get_headers(self, object_key: str, stderr: bool = True) -> bool | dict:
        """
        Get the headers of an object in the S3 bucket.
        :param object_key: Key of the object in the S3 bucket.
        :param stderr: Whether to print error messages.
        :return: Headers of the object if it exists, False otherwise.
        """
        try:
            return self.s3.head_object(Bucket=self.bucket, Key=object_key)
        except self.s3.exceptions.ClientError:
            print(f"[red]|ERROR| Object [cyan]{object_key}[/] not found.") if stderr else None
            return False
        except Exception as e:
            print(f"[red]|ERROR| An Error when receiving headers: {str(e)}") if stderr else None
            return False

    def get_size(self, object_key: str) -> str | int:
        """
        Get the size of an object in the S3 bucket.
        :param object_key: Key of the object in the S3 bucket.
        :return: Size of the object.
        """
        headers = self.get_headers(object_key)
        return headers['ContentLength'] if headers else 0

    def get_metadata(self, object_key: str) -> dict | None:
        """
        Get the metadata of an object in the S3 bucket.
        :param object_key: Key of the object in the S3 bucket.
        :return: Metadata of the object.
        """
        headers = self.get_headers(object_key)
        return headers['Metadata'] if headers else None

    def update_metadata(self, object_key: str, metadata: dict, stdout: bool = False) -> bool:
        """
        Update the metadata of an existing object in the S3 bucket.
        This operation copies the object to itself with new metadata.
        :param object_key: Key of the object in the S3 bucket.
        :param metadata: Dictionary of new metadata to attach to the object.
        :param stdout: Whether to print update information.
        :return: True if update is successful, False otherwise.
        """
        if not self.get_headers(object_key, stderr=False):
            print(f"[red]|ERROR| Object [cyan]{object_key}[/] not found.")
            return False

        if stdout:
            print(f"[green]|INFO| Updating metadata for [cyan]{self.bucket}/{object_key}[/]")

        try:
            self.s3.copy_object(
                Bucket=self.bucket,
                Key=object_key,
                CopySource={'Bucket': self.bucket, 'Key': object_key},
                Metadata=metadata,
                MetadataDirective='REPLACE'
            )
            return True
        except Exception as e:
            print(f"[red]|ERROR| Failed to update metadata: {str(e)}")
            return False

    def get_last_modified(self, object_key: str) -> datetime | None:
        """
        Get the last modified date of an object in the S3 bucket.
        :param object_key: Key of the object in the S3 bucket.
        :return: Last modified date of the object.
        """
        headers = self.get_headers(object_key)
        return headers['LastModified'] if headers else None

    def get_response_headers(self, object_key: str) -> dict | None:
        """
        Get the response headers of an object in the S3 bucket.
        :param object_key: Key of the object in the S3 bucket.
        :return: Response headers of the object.
        """
        headers = self.get_headers(object_key)
        return headers['ResponseMetadata']['HTTPHeaders'] if headers else None

    def get_sha256(self, object_key: str) -> str | None:
        """
        Get the SHA256 hash of an object in the S3 bucket.
        :param object_key: Key of the object in the S3 bucket.
        :return: SHA256 hash of the object.
        """
        try:
            file_contents = self.s3.get_object(Bucket=self.bucket, Key=object_key)['Body'].read()
            return hashlib.sha256(file_contents).hexdigest()
        except Exception as e:
            print(f"[red]|ERROR| Error while retrieving a file from S3: {e}")
            return None

    def delete(self, object_key: str, warning_msg: bool = True) -> None:
        """
        Delete an object from the S3 bucket.
        :param object_key: Key of the object in the S3 bucket.
        :param warning_msg: Whether to display a warning message before deletion.
        """
        print(f"[red]|INFO| Deleting object: [cyan]{object_key}[/] from [cyan]{self.bucket}[/]")
        Prompt.ask(f"[bold red]|WARNING|Are you sure you want to delete the object:") if warning_msg else None
        if self.get_headers(object_key):
            self.s3.delete_object(Bucket=self.bucket, Key=object_key)
        else:
            print(f"[bold red]|ERROR| Can't delete object: [cyan]{object_key}[/]")

    def delete_from_list(self, object_keys: list) -> None:
        """
        Delete multiple objects from the S3 bucket.
        :param object_keys: List of object keys to delete.
        """
        print(
            f"[red]|INFO| List of objects to be removed from the bucket [cyan]{self.bucket}[/]: "
            f"[cyan]{object_keys}[/]"
        )

        if Prompt.ask("[bold red]|WARNING| Continue?", choices=['yes', 'no'], default='no') == 'yes':
            for obj in object_keys:
                self.delete(obj, warning_msg=False)

    def buckets_list(self) -> list:
        """
        Get a list of all buckets in the S3 service.
        :return: List of bucket names.
        """
        try:
            return [bucket['Name'] for bucket in self.s3.list_buckets()['Buckets']]
        except KeyError:
            raise S3Exception(f"[red]|ERROR| Error while getting bucket list from AWS.")

    def _check_bucket_name(self, bucket_name: str) -> str | None:
        """
        Check if the specified bucket exists in the S3 service.
        :param bucket_name: Name of the S3 bucket.
        :return: Name of the bucket if it exists, None otherwise.
        """
        if bucket_name in self.buckets_list():
            return bucket_name
        raise S3Exception(f"[red]|ERROR| Bucket [cyan]{bucket_name}[/] not found.")
