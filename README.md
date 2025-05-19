# S3Wrapper

S3Wrapper is a Python library that simplifies working with AWS S3. It provides
an intuitive interface for uploading, downloading, deleting files, and
retrieving metadata or hashes from S3 buckets.

## Requirements

* Python 3.9+

## Features

* List files and objects in an S3 bucket
* Upload and download files
* Retrieve object metadata, size, and SHA256 hash
* Delete single or multiple objects
* List all available buckets
* Convenient authentication via boto3, with support for storing
  credentials in `~/.s3`

## Installation

It is recommended to use Poetry for dependency management:

```bash
poetry install
```

## Authentication

By default, S3Wrapper will look for your AWS credentials in the `~/.s3` directory:

* `~/.s3/key` - contains your AWS Access Key ID
* `~/.s3/private_key` - contains your AWS Secret Access Key

Alternatively, you can pass credentials directly when initializing S3Wrapper.

## Usage

```python
from S3Wrapper.S3Wrapper import S3Wrapper

# Initialize the wrapper (will read credentials from ~/.s3 by default)
s3 = S3Wrapper(
    bucket_name='your-bucket-name',
    region='your-region',  # e.g., 'us-east-1'
    # access_key='YOUR_ACCESS_KEY',  # Optional, will use ~/.s3/key if not provided
    # secret_access_key='YOUR_SECRET_KEY'  # Optional, will use ~/.s3/private_key if not provided
)

# List all files in the bucket
files = s3.get_files()  # Optionally: s3.get_files('folder/subfolder/')

# Upload a file
s3.upload('local-file.txt', 'remote/path/in/s3.txt')

# Download a file
s3.download('remote/path/in/s3.txt', 'local-file.txt')

# Get object metadata
metadata = s3.get_headers('remote/path/in/s3.txt')

# Get object size
size = s3.get_size('remote/path/in/s3.txt')

# Get SHA256 hash of an object
sha256 = s3.get_sha256('remote/path/in/s3.txt')

# Delete a file
s3.delete('remote/path/in/s3.txt')

# Delete multiple files
s3.delete_from_list(['file1.txt', 'file2.txt'])

# List all buckets
buckets = s3.buckets_list()
```
