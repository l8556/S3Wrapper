# -*- coding: utf-8 -*-
from os.path import expanduser, join, isfile
from io import open as io_open

import boto3


class S3Auth:

    def __init__(self, region_name: str, access_key: str = None, secret_access_key: str = None):
        self.region_name = region_name
        self.client = self._client(access_key, secret_access_key)

    def _client(self, access_key: str = None, secret_access_key: str = None) -> boto3.client:
        if not access_key or not secret_access_key:
            access_key, secret_access_key = self._read_keys()
        return boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key,
            region_name=self.region_name
        )

    @staticmethod
    def _read_keys(key_location = f'{expanduser("~")}/.s3') -> tuple[str, str]:
        access_key_id, secret_access_key = join(key_location, 'key'), join(key_location, 'private_key')
        if not isfile(access_key_id) or not isfile(secret_access_key):
            raise print(
                f"[red]|ERROR| No key or private key found in {key_location} "
                f"Please create files {access_key_id} "
                f"and {secret_access_key}"
            )
        return S3Auth._file_read(access_key_id), S3Auth._file_read(secret_access_key)

    @staticmethod
    def _file_read(path: str) -> str:
        with io_open(path, 'r', encoding='utf-8') as file:
            return file.read().strip()
