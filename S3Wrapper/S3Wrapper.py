# -*- coding: utf-8 -*-
import hashlib
from os.path import basename

from rich.prompt import Prompt

from .S3Auth import S3Auth
from rich import print

class S3Exception(Exception): ...

class S3Wrapper:

    def __init__(
            self,
            bucket_name: str = 'conversion-testing-files',
            region: str ='us-east-1',
            access_key: str = None,
            secret_access_key: str = None
    ):
        self.s3 = S3Auth(region, access_key, secret_access_key).client
        self.bucket = self._get_bucket(bucket_name)

    def get_files(self, s3_dir: str = None) -> list:
        s3_objects = self.get_objects()
        if isinstance(s3_dir, str):
            return [file for file in s3_objects if file.startswith(s3_dir) and not file.endswith('/')]
        return [file for file in s3_objects if not file.endswith('/')]

    def get_objects(self) -> list:
        file_names = []
        for page in self.s3.get_paginator('list_objects_v2').paginate(Bucket=self.bucket):
            if 'Contents' in page:
                for obj in page['Contents']:
                    file_names.append(obj['Key'])
        if not file_names:
            print("[red]|INFO| Bucket is empty.")
        return file_names

    def download(self, object_key: str, download_path: str, stdout: bool = True) -> bool:
        print(f"[green]|INFO| Downloading {self.bucket}/{object_key} to {download_path}") if stdout else None
        try:
            self.s3.download_file(self.bucket, object_key, download_path)
            return True
        except self.s3.exceptions.ClientError:
            print(f"[red]|ERROR| Object {object_key} not found.")
            return False

    def upload(self, file_path: str, object_key: str,  stdout: bool = True) -> None:
        print(f"[green]|INFO| Uploading {basename(file_path)} to {self.bucket}/{object_key}") if stdout else None
        self.s3.upload_file(file_path, self.bucket, object_key)

    def get_headers(self, object_key: str, stderr: bool = True) -> bool | dict:
        try:
            return self.s3.head_object(Bucket=self.bucket, Key=object_key)
        except self.s3.exceptions.ClientError:
            print(f"[red]|ERROR| Object {object_key} not found.") if stderr else ...
            return False
        except Exception as e:
            print(f"[red]|ERROR| An Error when receiving headers: {str(e)}") if stderr else ...
            return False

    def get_size(self, object_key: str) -> str | int:
        headers = self.get_headers(object_key)
        return headers['ContentLength'] if headers else 0

    def get_sha256(self, object_key: str) -> str | None:
        try:
            file_contents = self.s3.get_object(Bucket=self.bucket, Key=object_key)['Body'].read()
            return hashlib.sha256(file_contents).hexdigest()
        except Exception as e:
            print(f"Error while retrieving a file from S3: {e}")
            return None

    def delete(self, object_key: str, warning_msg: bool = True) -> None:
        print(f"[red]|INFO| Deleting object: {object_key} from {self.bucket}")
        Prompt.ask(f"[bold red]|WARNING|Are you sure you want to delete the object:") if warning_msg else None
        if self.get_headers(object_key):
            self.s3.delete_object(Bucket=self.bucket, Key=object_key)
        else:
            print(f"[bold red]|ERROR| Can't delete object: {object_key}")

    def delete_from_list(self, object_keys: list) -> None:
        print(f"[red]|INFO| List of objects to be removed from the bucket {self.bucket}: {object_keys}")
        if Prompt.ask(f"[bold red]|WARNING| Continue?", choices=['yes', 'no'], default='no') == 'yes':
            for obj in object_keys:
                self.delete(obj, warning_msg=False)

    def buckets_list(self) -> list:
        try:
            return [bucket['Name'] for bucket in self.s3.list_buckets()['Buckets']]
        except KeyError:
            raise S3Exception(f"[red]|ERROR| Error while getting bucket list from aws.")

    def _get_bucket(self, bucket_name: str) -> str | None:
        if bucket_name in self.buckets_list():
            return bucket_name
        raise S3Exception(f"[red]|ERROR| Bucket {bucket_name} not found.")
