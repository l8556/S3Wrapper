# -*- coding: utf-8 -*-
import os
import boto3


class S3Auth:
    """
    Class for managing authentication credentials for AWS S3.
    """

    def __init__(self, region_name: str, access_key: str = None, secret_access_key: str = None):
        self.region_name = region_name
        self.client = self._create_client(access_key, secret_access_key)

    def _create_client(self, access_key: str = None, secret_access_key: str = None) -> boto3.client:
        """
        Initialize an S3 client with provided or read access keys.
        :param access_key: AWS access key.
        :param secret_access_key: AWS secret access key.
        :return: Initialized boto3 S3 client.
        """
        if not access_key or not secret_access_key:
            access_key, secret_access_key = self._read_keys()
        return boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key,
            region_name=self.region_name
        )

    def _read_keys(self, key_location: str = os.path.join(os.path.expanduser("~"), '.s3')) -> tuple[str, str]:
        """
        Read AWS access and secret access keys from the specified location.
        :param key_location: Location to read the keys from.
        :return: Tuple containing access key and secret access key.
        """
        access_key_path = os.path.join(key_location, 'key')
        secret_access_key_path = os.path.join(key_location, 'private_key')

        if not all(map(os.path.isfile, [access_key_path, secret_access_key_path])):
            raise FileNotFoundError(
                f"[red]|ERROR| No key or private key found in {key_location}. "
                f"Please create files {access_key_path} and {secret_access_key_path}."
            )

        return self._file_read(access_key_path), self._file_read(secret_access_key_path)

    def _file_read(self, path: str) -> str:
        """
        Read contents of a file.
        :param path: Path of the file to read.
        :return: Contents of the file.
        """
        with open(path, 'r', encoding='utf-8') as file:
            return file.read().strip()
